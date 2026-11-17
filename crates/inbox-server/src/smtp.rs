use crate::{service::MailService, validation::MAX_RAW};
use mailin::{Action, Handler, Response, SessionBuilder, response};
use std::{
    io::{self, BufRead, BufReader, Write},
    net::{IpAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::runtime::Handle;
use uuid::Uuid;

struct Receiver {
    service: MailService,
    runtime: Handle,
    raw: Vec<u8>,
    recipients: Vec<(Uuid, String)>,
    oversize: bool,
    rejected_recipient: bool,
    drain: Arc<Mutex<bool>>,
    in_data: Arc<AtomicBool>,
    force: tokio_util::sync::CancellationToken,
}
fn reject_data(code: u16, message: &str) -> Response {
    let mut response = Response::custom(code, message.into());
    // mailin retains DATA state on errors; do not accept more commands as body.
    response.action = Action::Close;
    response
}
fn completion(result: Result<(), tonic::Status>) -> Response {
    match result {
        Ok(()) => response::OK,
        Err(e) => {
            if matches!(
                e.code(),
                tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
            ) {
                reject_data(554, "Invalid message")
            } else {
                reject_data(451, "Storage unavailable; retry later")
            }
        }
    }
}
impl Handler for Receiver {
    fn mail(&mut self, _: IpAddr, _: &str, _: &str) -> Response {
        self.raw.clear();
        self.recipients.clear();
        self.oversize = false;
        self.rejected_recipient = false;
        response::OK
    }
    fn rcpt(&mut self, to: &str) -> Response {
        if self.recipients.len() >= 1000 {
            self.rejected_recipient = true;
            return Response::custom(452, "Too many recipients".into());
        }
        match self.runtime.block_on(self.service.recipient(to)) {
            Ok(r) => {
                self.recipients.push(r);
                response::OK
            }
            Err(s) => {
                self.rejected_recipient = true;
                if s.code() == tonic::Code::Unavailable {
                    Response::custom(451, "Storage unavailable".into())
                } else {
                    response::NO_MAILBOX
                }
            }
        }
    }
    fn data_start(&mut self, _: &str, _: &str, _: bool, _: &[String]) -> Response {
        let drain = self.drain.lock().expect("SMTP drain lock");
        if *drain {
            return reject_data(421, "Server draining; retry later");
        }
        self.in_data.store(true, Ordering::Relaxed);
        self.raw.clear();
        self.oversize = false;
        response::OK
    }
    fn data(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.raw.len().saturating_add(buf.len()) > MAX_RAW {
            self.oversize = true
        } else if !self.oversize {
            self.raw.extend_from_slice(buf)
        }
        Ok(())
    }
    fn data_end(&mut self) -> Response {
        let result = self.finish_data();
        self.in_data.store(false, Ordering::Relaxed);
        result
    }
}
impl Receiver {
    fn finish_data(&mut self) -> Response {
        if self.rejected_recipient {
            return reject_data(554, "Recipient set rejected");
        }
        if self.oversize {
            return reject_data(552, "Message exceeds 25 MiB");
        }
        let raw = std::mem::take(&mut self.raw);
        let recipients = std::mem::take(&mut self.recipients);
        completion(
            self.runtime
                .block_on(async {
                    tokio::select! {
                        _ = self.force.cancelled() => Err(tonic::Status::unavailable("server draining")),
                        result = self.service.accept_smtp(raw, recipients) => result,
                    }
                })
                .map(|_| ()),
        )
    }
}
pub struct SmtpGuard {
    drain: Arc<Mutex<bool>>,
    force: tokio_util::sync::CancellationToken,
    active: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl SmtpGuard {
    pub fn begin_drain(&self) {
        *self.drain.lock().expect("SMTP drain lock") = true;
    }
    pub fn force_close(&self) {
        self.force.cancel();
    }
    pub async fn wait_until(&self, deadline: tokio::time::Instant) {
        while self.active.load(Ordering::Relaxed) > 0 {
            if tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(20)))
                .await
                .is_err()
            {
                self.force_close();
                break;
            }
        }
    }
}
impl Drop for SmtpGuard {
    fn drop(&mut self) {
        self.begin_drain();
        self.force_close();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
pub fn start(addr: &str, service: MailService) -> io::Result<SmtpGuard> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let drain = Arc::new(Mutex::new(false));
    let stopping = drain.clone();
    let force = tokio_util::sync::CancellationToken::new();
    let forcing = force.clone();
    let active = Arc::new(AtomicUsize::new(0));
    let connections = active.clone();
    let runtime = Handle::current();
    let thread = std::thread::spawn(move || {
        let active = connections;
        while !*stopping.lock().expect("SMTP drain lock") {
            match listener.accept() {
                Ok((mut stream, peer)) => {
                    if active.load(Ordering::Relaxed) >= 16 {
                        let _ = stream.write_all(b"421 Server busy\r\n");
                        continue;
                    }
                    active.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    let service = service.clone();
                    let runtime = runtime.clone();
                    let stopping = stopping.clone();
                    let forcing = forcing.clone();
                    std::thread::spawn(move || {
                        let _ = connection(stream, peer.ip(), service, runtime, stopping, forcing);
                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Err(_) => break,
            }
        }
    });
    Ok(SmtpGuard {
        drain,
        force,
        active,
        thread: Some(thread),
    })
}
fn connection(
    mut stream: TcpStream,
    peer: IpAddr,
    service: MailService,
    runtime: Handle,
    drain: Arc<Mutex<bool>>,
    force: tokio_util::sync::CancellationToken,
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let in_data = Arc::new(AtomicBool::new(false));
    let mut session = SessionBuilder::new(service.domain.clone()).build(
        peer,
        Receiver {
            service,
            runtime,
            raw: Vec::new(),
            recipients: Vec::new(),
            oversize: false,
            rejected_recipient: false,
            drain: drain.clone(),
            in_data: in_data.clone(),
            force: force.clone(),
        },
    );
    session.greeting().write_to(&mut stream)?;
    let mut line = Vec::new();
    let mut idle = Instant::now();
    let started = Instant::now();
    while !force.is_cancelled() && started.elapsed() < Duration::from_secs(300) {
        if *drain.lock().expect("SMTP drain lock") && !in_data.load(Ordering::Relaxed) {
            stream.write_all(b"421 Server draining; retry later\r\n")?;
            break;
        }
        let available = match reader.fill_buf() {
            Ok(b) => b,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if idle.elapsed() > Duration::from_secs(30) {
                    break;
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        if available.is_empty() {
            break;
        }
        idle = Instant::now();
        let n = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(available.len());
        if line.len() + n > 64 * 1024 {
            stream.write_all(b"500 Line too long\r\n")?;
            break;
        }
        line.extend_from_slice(&available[..n]);
        reader.consume(n);
        if line.last() != Some(&b'\n') {
            continue;
        }
        let reply = session.process(&line);
        line.clear();
        match reply.action {
            Action::Reply => {
                reply.write_to(&mut stream)?;
                stream.flush()?
            }
            Action::Close => {
                reply.write_to(&mut stream)?;
                break;
            }
            Action::NoReply => {}
            Action::UpgradeTls => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScriptedReceiver {
        receiver: Receiver,
        policy_rejected: bool,
    }
    impl Handler for ScriptedReceiver {
        fn mail(&mut self, ip: IpAddr, domain: &str, from: &str) -> Response {
            self.receiver.mail(ip, domain, from)
        }
        fn rcpt(&mut self, to: &str) -> Response {
            if to == "rejected@inbox.test" {
                self.receiver.rejected_recipient = true;
                response::NO_MAILBOX
            } else {
                self.receiver.recipients.push((Uuid::new_v4(), to.into()));
                response::OK
            }
        }
        fn data_start(
            &mut self,
            domain: &str,
            from: &str,
            is8bit: bool,
            to: &[String],
        ) -> Response {
            self.receiver.data_start(domain, from, is8bit, to)
        }
        fn data(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.receiver.data(bytes)
        }
        fn data_end(&mut self) -> Response {
            if self.policy_rejected {
                completion(Err(tonic::Status::failed_precondition(
                    "domain receiving disabled",
                )))
            } else {
                self.receiver.data_end()
            }
        }
    }

    #[tokio::test]
    async fn session_closes_after_every_negative_data_completion() {
        let credentials =
            s3::creds::Credentials::new(Some("synthetic"), Some("synthetic"), None, None, None)
                .unwrap();
        let bucket = s3::Bucket::new(
            "synthetic",
            s3::Region::Custom {
                region: "garage".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            credentials,
        )
        .unwrap();
        let service = MailService {
            auth: Default::default(),
            principal: None,
            db: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://synthetic:synthetic@127.0.0.1:1/synthetic")
                .unwrap(),
            bucket: Arc::new(*bucket),
            domain: "inbox.test".into(),
            pipeline: None,
        };
        let runtime = Handle::current();
        tokio::task::spawn_blocking(move || {
            for (mixed, policy_rejected, oversized, expected) in [
                (true, false, false, 554),
                (false, true, false, 554),
                (false, false, true, 552),
                (false, false, false, 451),
            ] {
                let handler = ScriptedReceiver {
                    receiver: Receiver {
                        service: service.clone(),
                        runtime: runtime.clone(),
                        raw: Vec::new(),
                        recipients: Vec::new(),
                        oversize: false,
                        rejected_recipient: false,
                        drain: Arc::new(Mutex::new(false)),
                        in_data: Arc::new(AtomicBool::new(false)),
                        force: tokio_util::sync::CancellationToken::new(),
                    },
                    policy_rejected,
                };
                let mut session =
                    SessionBuilder::new("inbox.test").build("127.0.0.1".parse().unwrap(), handler);
                for line in [
                    b"EHLO local\r\n".as_slice(),
                    b"MAIL FROM:<external@example.org>\r\n",
                    b"RCPT TO:<accepted@inbox.test>\r\n",
                ] {
                    assert_eq!(session.process(line).code, 250);
                }
                if mixed {
                    assert_eq!(
                        session.process(b"RCPT TO:<rejected@inbox.test>\r\n").code,
                        550
                    );
                }
                assert_eq!(session.process(b"DATA\r\n").code, 354);
                assert_eq!(
                    session.process(b"From: external@example.org\r\n").action,
                    Action::NoReply
                );
                assert_eq!(session.process(b"\r\n").action, Action::NoReply);
                if oversized {
                    let line = vec![b'x'; 32 * 1024];
                    for _ in 0..=MAX_RAW / line.len() {
                        assert_eq!(session.process(&line).action, Action::NoReply);
                    }
                }
                let mut processed = Vec::new();
                for line in [
                    b".\r\n".as_slice(),
                    b"RSET\r\n",
                    b"MAIL FROM:<next@example.org>\r\n",
                ] {
                    processed.push(line);
                    let reply = session.process(line);
                    assert_eq!(reply.code, expected);
                    assert_eq!(reply.action, Action::Close);
                    if reply.action == Action::Close {
                        break;
                    }
                }
                assert_eq!(processed, vec![b".\r\n".as_slice()]);
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn absent_broker_cannot_acknowledge_smtp_acceptance() {
        let credentials =
            s3::creds::Credentials::new(Some("synthetic"), Some("synthetic"), None, None, None)
                .unwrap();
        let bucket = s3::Bucket::new(
            "synthetic",
            s3::Region::Custom {
                region: "garage".into(),
                endpoint: "http://127.0.0.1:1".into(),
            },
            credentials,
        )
        .unwrap();
        let service = MailService {
            auth: Default::default(),
            principal: None,
            db: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://synthetic:synthetic@127.0.0.1:1/synthetic")
                .unwrap(),
            bucket: Arc::new(*bucket),
            domain: "inbox.test".into(),
            pipeline: None,
        };
        let runtime = Handle::current();
        let code = tokio::task::spawn_blocking(move || {
            let mut receiver = Receiver {
                service,
                runtime,
                raw: b"From: a@example.org\r\n\r\nsynthetic".to_vec(),
                recipients: vec![(Uuid::new_v4(), "a@inbox.test".into())],
                oversize: false,
                rejected_recipient: true,
                drain: Arc::new(Mutex::new(false)),
                in_data: Arc::new(AtomicBool::new(false)),
                force: tokio_util::sync::CancellationToken::new(),
            };
            assert_eq!(receiver.data_end().code, 554);
            receiver.rejected_recipient = false;
            *receiver.drain.lock().unwrap() = true;
            let reply = receiver.data_start("", "", false, &[]);
            assert_eq!(reply.code, 421);
            assert_eq!(reply.action, Action::Close);
            assert!(!receiver.in_data.load(Ordering::Relaxed));
            receiver.data_end().code
        })
        .await
        .unwrap();
        assert_eq!(code, 451);
    }
    #[derive(Clone)]
    struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
    impl Handler for Capture {
        fn data(&mut self, buf: &[u8]) -> io::Result<()> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(())
        }
    }
    #[test]
    fn smtp_library_preserves_unstuffed_raw_bytes() {
        let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut session = SessionBuilder::new("inbox.test")
            .build("127.0.0.1".parse().unwrap(), Capture(bytes.clone()));
        for line in [
            b"EHLO local\r\n".as_slice(),
            b"MAIL FROM:<source@example.org>\r\n",
            b"RCPT TO:<target@inbox.test>\r\n",
            b"DATA\r\n",
            b"From: source@example.org\r\n",
            b"\r\n",
            b"..original-dot\r\n",
            b"binary-\x80\r\n",
            b".\r\n",
        ] {
            session.process(line);
        }
        assert_eq!(
            *bytes.lock().unwrap(),
            b"From: source@example.org\r\n\r\n.original-dot\r\nbinary-\x80\r\n"
        );
        let next = session.process(b"MAIL FROM:<next@example.org>\r\n");
        assert_eq!(next.code, 250);
        assert_eq!(next.action, Action::Reply);
    }
}
