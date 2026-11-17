use axum::{
    body::{Body, Bytes, HttpBody},
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::sync::WaitForCancellationFutureOwned;

pub struct DrainListener {
    inner: tokio::net::TcpListener,
    lifecycle: Arc<Lifecycle>,
}
impl DrainListener {
    pub fn new(inner: tokio::net::TcpListener, lifecycle: Arc<Lifecycle>) -> Self {
        Self { inner, lifecycle }
    }
}
impl axum::serve::Listener for DrainListener {
    type Io = DrainIo;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = axum::serve::Listener::accept(&mut self.inner).await;
        (
            DrainIo {
                inner: Some(stream),
                cancelled: Box::pin(self.lifecycle.force.clone().cancelled_owned()),
                _activity: self.lifecycle.track(),
            },
            address,
        )
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

pub struct DrainIo {
    inner: Option<tokio::net::TcpStream>,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    _activity: Activity,
}
impl Drop for DrainIo {
    fn drop(&mut self) {
        self.inner.take();
    }
}
impl DrainIo {
    fn stream(&mut self, cx: &mut Context<'_>) -> io::Result<&mut tokio::net::TcpStream> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.inner.take();
        }
        self.inner
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionAborted, "HTTP drain deadline"))
    }
}
impl tokio::io::AsyncRead for DrainIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream(cx)?).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for DrainIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream(cx)?).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream(cx)?).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream(cx)?).poll_shutdown(cx)
    }
}

struct DrainBody {
    inner: Option<Body>,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    _activity: Activity,
}
impl Drop for DrainBody {
    fn drop(&mut self) {
        self.inner.take();
    }
}
impl HttpBody for DrainBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner
            .as_ref()
            .map(HttpBody::size_hint)
            .unwrap_or_default()
    }
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.inner.take();
            return Poll::Ready(None);
        }
        match self.inner.as_mut() {
            Some(inner) => Pin::new(inner).poll_frame(cx),
            None => Poll::Ready(None),
        }
    }
}

pub const WITHDRAW: Duration = Duration::from_secs(10);
pub const ADMISSIONS: Duration = Duration::from_secs(100);
pub const WORKERS: Duration = Duration::from_secs(120);
pub const CLOSURE: Duration = Duration::from_secs(10);

#[derive(Default)]
pub struct Lifecycle {
    closed: AtomicBool,
    force: CancellationToken,
    active: AtomicUsize,
    idle: tokio::sync::Notify,
}
struct Activity(Arc<Lifecycle>);
impl Drop for Activity {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.idle.notify_waiters();
    }
}
impl Lifecycle {
    fn track(self: &Arc<Self>) -> Activity {
        self.active.fetch_add(1, Ordering::SeqCst);
        Activity(self.clone())
    }
    pub async fn wait_closed(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }
    pub fn close_admission(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    pub fn force_close(&self) {
        self.force.cancel();
    }
}

pub struct Deadlines {
    pub withdraw: Instant,
    pub admissions: Instant,
    pub workers: Instant,
    pub closure: Instant,
}
impl Deadlines {
    pub fn new(now: Instant) -> Self {
        let withdraw = now + WITHDRAW;
        let admissions = withdraw + ADMISSIONS;
        let workers = admissions + WORKERS;
        let closure = workers + CLOSURE;
        Self {
            withdraw,
            admissions,
            workers,
            closure,
        }
    }
}

pub async fn admit(State(state): State<Arc<Lifecycle>>, req: Request, next: Next) -> Response {
    if matches!(req.uri().path(), "/health" | "/ready") {
        return next.run(req).await;
    }
    if state.closed.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let activity = state.track();
    let response = tokio::select! {
        biased;
        _ = state.force.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        response = next.run(req) => response,
    };
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(DrainBody {
            inner: Some(body),
            cancelled: Box::pin(state.force.clone().cancelled_owned()),
            _activity: activity,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, middleware, routing::get};

    struct ToolBody {
        first: bool,
        finished: Pin<Box<WaitForCancellationFutureOwned>>,
        cancelled: CancellationToken,
        dropped: Arc<AtomicBool>,
    }
    impl HttpBody for ToolBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            if std::mem::take(&mut self.first) {
                Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from_static(
                    b": tool started\n\n",
                )))))
            } else if self.finished.as_mut().poll(cx).is_ready() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for ToolBody {
        fn drop(&mut self) {
            self.cancelled.cancel();
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn force_closes_headers_first_tool_body_and_connection_before_worker_phase() {
        for force in [false, true] {
            let lifecycle = Arc::new(Lifecycle::default());
            let finished = CancellationToken::new();
            let body_finished = finished.clone();
            let dropped = Arc::new(AtomicBool::new(false));
            let cancelled = CancellationToken::new();
            let task_cancelled = cancelled.clone();
            let operation = tokio::spawn(async move { task_cancelled.cancelled().await });
            let body_dropped = dropped.clone();
            let app = Router::new()
                .route(
                    "/mcp",
                    get(move || {
                        let body = ToolBody {
                            first: true,
                            finished: Box::pin(body_finished.clone().cancelled_owned()),
                            cancelled: cancelled.clone(),
                            dropped: body_dropped.clone(),
                        };
                        async move {
                            Response::builder()
                                .header("content-type", "text/event-stream")
                                .body(Body::new(body))
                                .unwrap()
                        }
                    }),
                )
                .layer(middleware::from_fn_with_state(lifecycle.clone(), admit));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/mcp", listener.local_addr().unwrap());
            let stop = CancellationToken::new();
            let stopped = stop.clone();
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(stopped.cancelled_owned())
                    .await
                    .unwrap()
            });
            let mut response = reqwest::get(url).await.unwrap();
            assert_eq!(response.status(), 200);
            assert!(response.chunk().await.unwrap().is_some());
            lifecycle.close_admission();
            stop.cancel();
            assert!(!dropped.load(Ordering::SeqCst));
            assert!(!operation.is_finished());
            assert!(!server.is_finished());
            if force {
                lifecycle.force_close();
            } else {
                finished.cancel();
            }
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), lifecycle.wait_closed())
                .await
                .unwrap();
            assert!(
                dropped.load(Ordering::SeqCst),
                "worker phase requires body destruction"
            );
            tokio::time::timeout(Duration::from_secs(2), operation)
                .await
                .unwrap()
                .unwrap();
            // The client never needs to drain its response to release the server.
            drop(response);
        }
    }

    #[tokio::test]
    async fn force_interrupts_idle_connection_io_without_body_polling() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let lifecycle = Arc::new(Lifecycle::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let mut listener = DrainListener::new(listener, lifecycle.clone());
        let (mut connection, _) = axum::serve::Listener::accept(&mut listener).await;
        client.write_all(b"partial HTTP request").await.unwrap();
        let mut buffer = [0; 64];
        assert!(connection.read(&mut buffer).await.unwrap() > 0);
        let read = tokio::spawn(async move {
            let error = connection.read(&mut buffer).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
            assert!(connection.inner.is_none());
        });
        lifecycle.force_close();
        tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client.read(&mut buffer).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn body_proxy_preserves_native_grpc_trailers_before_deadline() {
        struct Trailers(Option<axum::http::HeaderMap>);
        impl HttpBody for Trailers {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
                Poll::Ready(
                    self.0
                        .take()
                        .map(|headers| Ok(http_body::Frame::trailers(headers))),
                )
            }
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());
        let mut body = DrainBody {
            inner: Some(Body::new(Trailers(Some(headers)))),
            cancelled: Box::pin(CancellationToken::new().cancelled_owned()),
            _activity: Arc::new(Lifecycle::default()).track(),
        };
        let frame = futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.trailers_ref().unwrap()["grpc-status"], "0");
    }

    #[test]
    fn phase_deadlines_are_absolute_and_fit_the_pod_grace() {
        let now = Instant::now();
        let deadlines = Deadlines::new(now);
        assert_eq!(deadlines.withdraw - now, WITHDRAW);
        assert_eq!(deadlines.admissions - now, Duration::from_secs(110));
        assert_eq!(deadlines.workers - now, Duration::from_secs(230));
        assert_eq!(deadlines.closure - now, Duration::from_secs(240));
        assert!(deadlines.closure - now < Duration::from_secs(270));
    }

    #[tokio::test]
    async fn admission_closes_without_cancelling_work_until_forced() {
        let lifecycle = Arc::new(Lifecycle::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let handler_entered = entered.clone();
        let handler_finish = finish.clone();
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route(
                "/work",
                get(move || {
                    let entered = handler_entered.clone();
                    let finish = handler_finish.clone();
                    async move {
                        entered.notify_one();
                        finish.notified().await;
                        "finished"
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(lifecycle.clone(), admit));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let pending = {
            let client = client.clone();
            let url = url.clone();
            tokio::spawn(async move { client.get(format!("{url}/work")).send().await.unwrap() })
        };
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        lifecycle.close_admission();
        assert_eq!(
            client
                .get(format!("{url}/work"))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert!(!pending.is_finished());
        finish.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .text()
                .await
                .unwrap(),
            "finished"
        );
        lifecycle.force_close();
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        server.abort();
    }

    #[tokio::test]
    async fn forced_deadline_cancels_an_admitted_handler() {
        let lifecycle = Arc::new(Lifecycle::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let handler_entered = entered.clone();
        let app = Router::new()
            .route(
                "/",
                get(move || {
                    let entered = handler_entered.clone();
                    async move {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(lifecycle.clone(), admit));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let pending = tokio::spawn(async move { reqwest::get(url).await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        lifecycle.close_admission();
        lifecycle.force_close();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .status(),
            503
        );
        server.abort();
    }
}
