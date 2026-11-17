use crate::{
    mutation::{Command, Outcome, hash, outbox, storage},
    pb::ChangeEvent,
    service::MailService,
};
use async_nats::{
    HeaderMap,
    jetstream::{
        self,
        consumer::{AckPolicy, DeliverPolicy, pull},
        stream::{Config, DiscardPolicy, RetentionPolicy, StorageType},
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    sync::{Semaphore, watch},
};
use tonic::Status;
use tracing::Instrument;
use uuid::Uuid;

const MAX_PAYLOAD: usize = 32 * 1024 * 1024;
const GIB: i64 = 1024 * 1024 * 1024;
pub type ChangeStream = std::pin::Pin<Box<dyn Stream<Item = Result<ChangeEvent, Status>> + Send>>;

#[derive(Serialize, Deserialize)]
struct Confirmation {
    digest: String,
    outcome: Outcome,
}
#[derive(Serialize, Deserialize)]
enum ConfirmationRecord {
    Inline(Vec<u8>),
    Staged {
        object: String,
        size: usize,
        digest: String,
    },
}

#[derive(Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub digest: String,
    pub size: usize,
    pub object: Option<String>,
    pub inline: Option<Vec<u8>>,
    pub traceparent: String,
}
impl Envelope {
    pub fn inline(id: Uuid, command: Command, parent: &str) -> Result<Self, Status> {
        let bytes = bincode::serialize(&command).map_err(storage)?;
        Ok(Self {
            id,
            digest: hash(&bytes),
            size: bytes.len(),
            object: None,
            inline: Some(bytes),
            traceparent: parent.into(),
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self).map_err(storage)
    }
}

pub struct Pipeline {
    config: crate::nats_config::Config,
    client: async_nats::Client,
    js: jetstream::Context,
    objects: jetstream::object_store::ObjectStore,
    stop: watch::Sender<bool>,
    watch_stop: tokio_util::sync::CancellationToken,
    publisher_stop: watch::Sender<bool>,
    draining: AtomicBool,
    running: AtomicBool,
    worker_healthy: AtomicBool,
    ingress: Semaphore,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    publisher_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Pipeline {
    pub async fn connect(url: &str) -> Result<Arc<Self>, Status> {
        Self::connect_configured(url, crate::nats_config::Config::local()).await
    }
    pub async fn connect_configured(
        url: &str,
        config: crate::nats_config::Config,
    ) -> Result<Arc<Self>, Status> {
        let client = config.connect(url).await?;
        let mut js = jetstream::new(client.clone());
        js.set_timeout(Duration::from_secs(3));
        for (name, subject, max_bytes, retention, discard, age) in [
            (
                config.resource("COMMANDS"),
                config.subject("commands.*"),
                64 * 1024 * 1024,
                RetentionPolicy::WorkQueue,
                DiscardPolicy::New,
                0,
            ),
            (
                config.resource("RESULTS"),
                config.subject("results.*"),
                GIB,
                RetentionPolicy::Limits,
                DiscardPolicy::New,
                0,
            ),
            (
                config.resource("CHANGES"),
                config.subject("changes"),
                64 * 1024 * 1024,
                RetentionPolicy::Limits,
                DiscardPolicy::Old,
                7 * 86400,
            ),
        ] {
            let expected = Config {
                name,
                subjects: vec![subject],
                max_bytes,
                storage: StorageType::File,
                retention,
                discard,
                max_age: Duration::from_secs(age),
                max_message_size: 1024 * 1024,
                num_replicas: config.replicas(),
                ..Default::default()
            };
            let stream = js
                .get_or_create_stream(expected.clone())
                .await
                .map_err(storage)?;
            let actual = &stream.cached_info().config;
            if actual.storage != expected.storage
                || actual.discard != expected.discard
                || actual.retention != expected.retention
                || actual.max_bytes != expected.max_bytes
                || actual.max_age != expected.max_age
                || actual.subjects != expected.subjects
                || actual.num_replicas != expected.num_replicas
                || actual.max_message_size != expected.max_message_size
                || actual.max_messages > 0
                || actual.max_messages_per_subject > 0
                || actual.max_consumers > 0
                || actual.no_ack
                || actual.sealed
                || actual.mirror.is_some()
                || actual.sources.is_some()
                || !isolated_stream(actual, false)
            {
                return Err(Status::failed_precondition(
                    "incompatible durable stream configuration",
                ));
            }
        }
        let staging = config.resource("STAGING");
        // async-nats 0.46 object-store creation uses DiscardNew and supports replicas.
        // Provision the exact backing stream without updating any reused resource.
        let expected_staging = Config {
            name: format!("OBJ_{staging}"),
            subjects: vec![format!("$O.{staging}.C.>"), format!("$O.{staging}.M.>")],
            max_bytes: GIB,
            storage: StorageType::File,
            num_replicas: config.replicas(),
            discard: DiscardPolicy::New,
            allow_rollup: true,
            allow_direct: true,
            ..Default::default()
        };
        let stream = js
            .get_or_create_stream(expected_staging.clone())
            .await
            .map_err(storage)?;
        let actual = &stream.cached_info().config;
        if actual.storage != StorageType::File
            || actual.max_age != Duration::ZERO
            || actual.max_bytes != GIB
            || actual.num_replicas != config.replicas()
            || actual.subjects != expected_staging.subjects
            || actual.discard != DiscardPolicy::New
            || actual.retention != RetentionPolicy::Limits
            || !actual.allow_rollup
            || !actual.allow_direct
            || actual.max_messages > 0
            || actual.max_messages_per_subject > 0
            || actual.max_message_size > 0
            || actual.max_consumers > 0
            || actual.sealed
            || actual.no_ack
            || actual.mirror.is_some()
            || actual.sources.is_some()
            || !isolated_stream(actual, true)
        {
            return Err(Status::failed_precondition(
                "incompatible staging configuration",
            ));
        }
        let objects = js.get_object_store(staging).await.map_err(storage)?;
        let (stop, _) = watch::channel(false);
        let (publisher_stop, _) = watch::channel(false);
        Ok(Arc::new(Self {
            config,
            client,
            js,
            objects,
            stop,
            watch_stop: tokio_util::sync::CancellationToken::new(),
            publisher_stop,
            draining: AtomicBool::new(false),
            running: AtomicBool::new(false),
            worker_healthy: AtomicBool::new(false),
            ingress: Semaphore::new(16),
            tasks: Mutex::new(Vec::new()),
            publisher_task: Mutex::new(None),
        }))
    }
    pub fn namespace(&self) -> &str {
        self.config.namespace()
    }
    pub fn ready(&self) -> bool {
        self.running.load(Ordering::Relaxed)
            && self.worker_healthy.load(Ordering::Relaxed)
            && self.client.connection_state() == async_nats::connection::State::Connected
            && !*self.stop.borrow()
    }
    pub async fn resources_ready(&self) -> bool {
        if self.draining.load(Ordering::Relaxed) || !self.ready() {
            return false;
        }
        for (suffix, subjects, bytes, retention, discard, age, message_size) in [
            (
                "COMMANDS",
                vec![self.config.subject("commands.*")],
                64 * 1024 * 1024,
                RetentionPolicy::WorkQueue,
                DiscardPolicy::New,
                0,
                1024 * 1024,
            ),
            (
                "RESULTS",
                vec![self.config.subject("results.*")],
                GIB,
                RetentionPolicy::Limits,
                DiscardPolicy::New,
                0,
                1024 * 1024,
            ),
            (
                "CHANGES",
                vec![self.config.subject("changes")],
                64 * 1024 * 1024,
                RetentionPolicy::Limits,
                DiscardPolicy::Old,
                7 * 86400,
                1024 * 1024,
            ),
            (
                "STAGING",
                vec![
                    format!("$O.{}.C.>", self.config.resource("STAGING")),
                    format!("$O.{}.M.>", self.config.resource("STAGING")),
                ],
                GIB,
                RetentionPolicy::Limits,
                DiscardPolicy::New,
                0,
                0,
            ),
        ] {
            let name = if suffix == "STAGING" {
                format!("OBJ_{}", self.config.resource(suffix))
            } else {
                self.config.resource(suffix)
            };
            let Ok(stream) = self.js.get_stream(name).await else {
                return false;
            };
            let c = &stream.cached_info().config;
            if c.subjects != subjects
                || c.max_bytes != bytes
                || c.num_replicas != self.config.replicas()
                || c.retention != retention
                || c.discard != discard
                || c.max_age != Duration::from_secs(age)
                || c.storage != StorageType::File
                || c.sealed
                || c.no_ack
                || c.max_messages > 0
                || c.max_messages_per_subject > 0
                || c.max_consumers > 0
                || c.mirror.is_some()
                || c.sources.is_some()
                || !isolated_stream(c, suffix == "STAGING")
                || (message_size > 0 && c.max_message_size != message_size)
                || (suffix == "STAGING"
                    && (c.max_message_size > 0 || !c.allow_rollup || !c.allow_direct))
            {
                return false;
            }
        }
        let Ok(stream) = self.js.get_stream(self.config.resource("COMMANDS")).await else {
            return false;
        };
        let durable = self.config.resource("MUTATIONS");
        let Ok(consumer) = stream.get_consumer::<pull::Config>(&durable).await else {
            return false;
        };
        isolated_consumer(&consumer.cached_info().config, &durable)
    }
    pub async fn execute(&self, command: Command, parent: String) -> Result<Vec<u8>, Status> {
        if !self.ready() {
            return Err(Status::unavailable("command broker unavailable"));
        }
        let _permit = self
            .ingress
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("command ingress busy"))?;
        let id = Uuid::new_v4();
        let bytes = bincode::serialize(&command).map_err(storage)?;
        if bytes.len() > MAX_PAYLOAD {
            return Err(Status::resource_exhausted(
                "command exceeds transport limit",
            ));
        }
        let name = id.to_string();
        let metadata = jetstream::object_store::ObjectMetadata {
            name: name.clone(),
            chunk_size: Some(128 * 1024),
            ..Default::default()
        };
        tokio::time::timeout(
            Duration::from_secs(30),
            self.objects.put(metadata, &mut bytes.as_slice()),
        )
        .await
        .map_err(|_| Status::unavailable("command staging unavailable"))?
        .map_err(|_| Status::unavailable("command staging unavailable"))?;
        let envelope = Envelope {
            id,
            digest: hash(&bytes),
            size: bytes.len(),
            object: Some(name),
            inline: None,
            traceparent: parent,
        };
        self.publish(
            &self.config.subject("commands.api"),
            envelope.encode()?,
            &id.to_string(),
            &envelope.traceparent,
        )
        .await
        .map_err(|_| Status::unknown("command may be queued; retry with the same request key"))?;
        // Only a persisted result is completion. A PubAck above is not acceptance.
        let stream = self
            .js
            .get_stream(self.config.resource("RESULTS"))
            .await
            .map_err(|_| {
                Status::unknown("operation outcome unknown; retry with the same request key")
            })?;
        let subject = self.config.subject(&format!("results.{id}"));
        let wait = async {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let mut stop = self.stop.subscribe();
            loop {
                tokio::select! {_=stop.changed()=>return Err(Status::unknown("operation outcome unknown; retry with the same request key")),_=tick.tick()=>{}}
                if let Ok(message) = stream.get_last_raw_message_by_subject(&subject).await {
                    let result = self.confirmation(&message.payload).await.map_err(|_| {
                        Status::unknown(
                            "operation confirmation unavailable; retry with the same request key",
                        )
                    })?;
                    if result.digest != envelope.digest {
                        return Err(Status::already_exists("command ID conflicts"));
                    }
                    return result.outcome.result();
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(60), wait)
            .await
            .unwrap_or_else(|_| {
                Err(Status::unknown(
                    "operation outcome unknown; retry with the same request key",
                ))
            })
    }
    #[tracing::instrument(name="queue.publish",skip_all,fields(trace_id=tracing::field::Empty,span_id=tracing::field::Empty))]
    async fn publish(
        &self,
        subject: &str,
        mut payload: Vec<u8>,
        key: &str,
        parent: &str,
    ) -> Result<(), Status> {
        crate::telemetry::set_parent(&tracing::Span::current(), parent);
        let (trace_id, span_id) = crate::telemetry::ids();
        tracing::Span::current()
            .record("trace_id", trace_id)
            .record("span_id", span_id);
        if subject.starts_with(&self.config.subject("results.")) {
            let record = if payload.len() > 512 * 1024 {
                if payload.len() > MAX_PAYLOAD {
                    return Err(Status::resource_exhausted(
                        "confirmation exceeds transport limit",
                    ));
                }
                let object = format!("result-{}", Uuid::new_v4());
                tokio::time::timeout(
                    Duration::from_secs(30),
                    self.objects.put(
                        jetstream::object_store::ObjectMetadata {
                            name: object.clone(),
                            chunk_size: Some(128 * 1024),
                            ..Default::default()
                        },
                        &mut payload.as_slice(),
                    ),
                )
                .await
                .map_err(|_| Status::unavailable("confirmation staging unavailable"))?
                .map_err(storage)?;
                ConfirmationRecord::Staged {
                    object,
                    size: payload.len(),
                    digest: hash(&payload),
                }
            } else {
                ConfirmationRecord::Inline(payload)
            };
            payload = bincode::serialize(&record).map_err(storage)?;
        }
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", key);
        if !parent.is_empty() {
            headers.insert("traceparent", parent)
        }
        self.js
            .publish_with_headers(subject.to_owned(), headers, payload.into())
            .await
            .map_err(storage)?
            .await
            .map_err(storage)?;
        Ok(())
    }
    async fn confirmation(&self, payload: &[u8]) -> Result<Confirmation, Status> {
        tokio::time::timeout(Duration::from_secs(30), self.confirmation_inner(payload))
            .await
            .map_err(|_| Status::unavailable("confirmation staging unavailable"))?
    }
    async fn confirmation_inner(&self, payload: &[u8]) -> Result<Confirmation, Status> {
        let bytes = match bincode::deserialize(payload).map_err(storage)? {
            ConfirmationRecord::Inline(bytes) => bytes,
            ConfirmationRecord::Staged {
                object,
                size,
                digest,
            } => {
                if size > MAX_PAYLOAD
                    || object
                        .strip_prefix("result-")
                        .and_then(|s| Uuid::parse_str(s).ok())
                        .is_none()
                {
                    return Err(Status::invalid_argument("invalid confirmation reference"));
                }
                let mut bytes = Vec::with_capacity(size);
                self.objects
                    .get(object)
                    .await
                    .map_err(storage)?
                    .take((MAX_PAYLOAD + 1) as u64)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(storage)?;
                if bytes.len() != size || hash(&bytes) != digest {
                    return Err(Status::invalid_argument(
                        "confirmation integrity check failed",
                    ));
                }
                bytes
            }
        };
        bincode::deserialize(&bytes).map_err(storage)
    }
    async fn load(&self, envelope: &Envelope) -> Result<Command, Status> {
        tokio::time::timeout(Duration::from_secs(30), self.load_inner(envelope))
            .await
            .map_err(|_| Status::unavailable("command staging unavailable"))?
    }
    async fn load_inner(&self, envelope: &Envelope) -> Result<Command, Status> {
        if envelope.size > MAX_PAYLOAD || envelope.traceparent.len() > 512 {
            return Err(Status::invalid_argument("invalid command envelope"));
        }
        let bytes = match (&envelope.object, &envelope.inline) {
            (Some(name), None) if name == &envelope.id.to_string() => {
                let object = self.objects.get(name).await.map_err(storage)?;
                let mut bytes = Vec::with_capacity(envelope.size);
                object
                    .take((MAX_PAYLOAD + 1) as u64)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(storage)?;
                bytes
            }
            (None, Some(bytes)) if bytes.len() < 4096 => bytes.clone(),
            _ => return Err(Status::invalid_argument("invalid command reference")),
        };
        if bytes.len() != envelope.size || hash(&bytes) != envelope.digest {
            return Err(Status::invalid_argument("command integrity check failed"));
        }
        bincode::deserialize(&bytes)
            .map_err(|_| Status::invalid_argument("invalid command payload"))
    }
    pub async fn start(self: &Arc<Self>, service: MailService) -> Result<(), Status> {
        let stream = self
            .js
            .get_stream(self.config.resource("COMMANDS"))
            .await
            .map_err(storage)?;
        let durable = self.config.resource("MUTATIONS");
        let expected_consumer = pull::Config {
            durable_name: Some(durable.clone()),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(120),
            max_ack_pending: 1,
            max_deliver: -1,
            ..Default::default()
        };
        let consumer = stream
            .get_or_create_consumer(&durable, expected_consumer.clone())
            .await
            .map_err(storage)?;
        if !isolated_consumer(&consumer.cached_info().config, &durable) {
            return Err(Status::failed_precondition(
                "incompatible durable consumer configuration",
            ));
        }
        if self.running.swap(true, Ordering::Relaxed) {
            return Err(Status::failed_precondition("worker already started"));
        }
        self.worker_healthy.store(true, Ordering::Relaxed);
        let p = self.clone();
        let publisher_service = service.clone();
        let publisher = tokio::spawn(async move { p.publisher(publisher_service).await });
        *self.publisher_task.lock().expect("publisher task lock") = Some(publisher);
        let p = self.clone();
        let worker = tokio::spawn(async move {
            let mut stop = p.stop.subscribe();
            p.running.store(true, Ordering::Relaxed);
            loop {
                if *stop.borrow() {
                    break;
                }
                let batch = consumer
                    .batch()
                    .max_messages(1)
                    .expires(Duration::from_secs(2))
                    .messages()
                    .await;
                let Ok(mut batch) = batch else {
                    p.worker_healthy.store(false, Ordering::Relaxed);
                    tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                    continue;
                };
                let message = tokio::select! {biased;_=stop.changed()=>break,m=batch.next()=>m};
                let message = match message {
                    Some(Ok(message)) => {
                        p.worker_healthy.store(true, Ordering::Relaxed);
                        message
                    }
                    Some(Err(_)) => {
                        p.worker_healthy.store(false, Ordering::Relaxed);
                        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                        continue;
                    }
                    None => {
                        p.worker_healthy.store(true, Ordering::Relaxed);
                        continue;
                    }
                };
                let envelope: Envelope = match serde_json::from_slice(&message.payload) {
                    Ok(e) => e,
                    Err(_) => {
                        let _ = message.ack_with(jetstream::AckKind::Term).await;
                        continue;
                    }
                };
                let span = tracing::info_span!(
                    "command",
                    trace_id = tracing::field::Empty,
                    span_id = tracing::field::Empty
                );
                crate::telemetry::set_parent(&span, &envelope.traceparent);
                let processing = async {
                    let (trace_id, span_id) = crate::telemetry::ids();
                    tracing::Span::current()
                        .record("trace_id", trace_id)
                        .record("span_id", span_id);
                    p.consume(&service, &envelope).await
                }
                .instrument(span);
                tokio::pin!(processing);
                let mut progress = tokio::time::interval(Duration::from_secs(30));
                progress.tick().await;
                let result = loop {
                    tokio::select! {result=&mut processing=>break result,_=progress.tick()=>{let _=message.ack_with(jetstream::AckKind::Progress).await;}}
                };
                match result {
                    Ok(outcome) => {
                        let bytes = match bincode::serialize(&Confirmation {
                            digest: envelope.digest.clone(),
                            outcome,
                        }) {
                            Ok(b) => b,
                            Err(_) => continue,
                        };
                        if p.publish(
                            &p.config.subject(&format!("results.{}", envelope.id)),
                            bytes,
                            &format!("result:{}", envelope.id),
                            &envelope.traceparent,
                        )
                        .await
                        .is_ok()
                        {
                            let _ = message.double_ack().await;
                        } else {
                            let _ = message
                                .ack_with(jetstream::AckKind::Nak(Some(Duration::from_secs(2))))
                                .await;
                        }
                    }
                    Err(_) => {
                        let _ = message
                            .ack_with(jetstream::AckKind::Nak(Some(Duration::from_secs(5))))
                            .await;
                    }
                }
            }
            p.running.store(false, Ordering::Relaxed);
        });
        self.tasks.lock().expect("worker task lock").push(worker);
        // Wait until a worker actually exists before ingress becomes ready.
        for _ in 0..100 {
            if self.ready() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await
        }
        Err(Status::unavailable("command worker unavailable"))
    }
    async fn consume(&self, s: &MailService, e: &Envelope) -> Result<Outcome, Status> {
        let results = self
            .js
            .get_stream(self.config.resource("RESULTS"))
            .await
            .map_err(storage)?;
        match results
            .get_last_raw_message_by_subject(&self.config.subject(&format!("results.{}", e.id)))
            .await
        {
            Ok(message) => {
                let result = self.confirmation(&message.payload).await?;
                if result.digest != e.digest {
                    return Ok(Outcome::from_result(Err(Status::already_exists(
                        "command ID conflicts",
                    ))));
                }
                return Ok(result.outcome);
            }
            Err(error)
                if error.kind() == jetstream::stream::LastRawMessageErrorKind::NoMessageFound => {}
            Err(error) => return Err(storage(error)),
        }
        let command = match self.load(e).await {
            Ok(c) => c,
            Err(error) => return Ok(Outcome::from_result(Err(error))),
        };
        let mut tx = match s.db.begin().await {
            Ok(tx) => tx,
            Err(_) if matches!(&command, Command::Deliver(_)) => {
                return Err(Status::unavailable("delivery retry pending"));
            }
            Err(_) => {
                return Ok(Outcome::from_result(Err(Status::unavailable(
                    "storage temporarily unavailable",
                ))));
            }
        };
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(e.id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        if let Some(row) =
            sqlx::query("SELECT payload_hash,result FROM command_journal WHERE id=$1")
                .bind(e.id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
        {
            if row.get::<String, _>("payload_hash") != e.digest {
                return Ok(Outcome::from_result(Err(Status::already_exists(
                    "command ID conflicts",
                ))));
            }
            return bincode::deserialize(row.get::<&[u8], _>("result")).map_err(storage);
        }
        let scope = command.request_scope();
        let request_digest = command.request_digest()?;
        if let Ok(Some(scope)) = &scope {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
                .bind(scope)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            if let Some(row) =
                sqlx::query("SELECT payload_hash,result FROM mutation_requests WHERE scope=$1")
                    .bind(scope)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?
            {
                let outcome = if row.get::<String, _>("payload_hash") == request_digest {
                    bincode::deserialize(row.get::<&[u8], _>("result")).map_err(storage)?
                } else {
                    Outcome::from_result(Err(Status::already_exists(
                        "request ID used with different payload",
                    )))
                };
                self.finish(&mut tx, e, &outcome, None).await?;
                tx.commit().await.map_err(storage)?;
                return Ok(outcome);
            }
        }
        let delivery = if let Command::Deliver(id) = &command {
            Some(*id)
        } else {
            None
        };
        if let Some(job) = delivery {
            let claimed=sqlx::query("UPDATE deliveries SET attempts=attempts+1,lease_until=now()+interval '120 seconds' WHERE id=$1 AND status=1 AND attempts<8 AND (lease_until IS NULL OR lease_until<now()) RETURNING id").bind(job).fetch_optional(&s.db).await.map_err(storage)?;
            if claimed.is_none() {
                let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM deliveries WHERE id=$1 AND status=1 AND lease_until>now())").bind(job).fetch_one(&s.db).await.map_err(storage)?;
                if active {
                    return Err(Status::unavailable("delivery lease active"));
                }
            }
        }
        sqlx::query("SAVEPOINT application")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let current_parent = crate::telemetry::parent_header();
        let processing_parent = if current_parent.is_empty() {
            e.traceparent.as_str()
        } else {
            current_parent.as_str()
        };
        let result = match scope.as_ref() {
            Err(s) => Err(s.clone()),
            Ok(_) => s.apply(command, &mut tx, processing_parent).await,
        };
        if result.is_err() {
            sqlx::query("ROLLBACK TO SAVEPOINT application")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        let outcome = Outcome::from_result(result);
        if let Some(job) = delivery
            && outcome.code == tonic::Code::Unavailable as i32
        {
            sqlx::query("UPDATE deliveries SET lease_until=now()+interval '5 seconds' WHERE id=$1 AND status=1").bind(job).execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            return Err(Status::unavailable("delivery retry pending"));
        }
        let cache = scope.ok().flatten().filter(|_| {
            outcome.code != tonic::Code::Unavailable as i32
                && outcome.code != tonic::Code::Unknown as i32
        });
        self.finish(
            &mut tx,
            e,
            &outcome,
            cache
                .as_deref()
                .map(|scope| (scope, request_digest.as_str())),
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(outcome)
    }
    async fn finish(
        &self,
        tx: &mut sqlx::PgConnection,
        e: &Envelope,
        outcome: &Outcome,
        scope: Option<(&str, &str)>,
    ) -> Result<(), Status> {
        let bytes = bincode::serialize(outcome).map_err(storage)?;
        sqlx::query("INSERT INTO command_journal(id,payload_hash,result) VALUES($1,$2,$3)")
            .bind(e.id)
            .bind(&e.digest)
            .bind(&bytes)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        if let Some((scope, request_digest)) = scope {
            sqlx::query(
                "INSERT INTO mutation_requests(scope,payload_hash,result) VALUES($1,$2,$3)",
            )
            .bind(scope)
            .bind(request_digest)
            .bind(&bytes)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let confirmation = bincode::serialize(&Confirmation {
            digest: e.digest.clone(),
            outcome: outcome.clone(),
        })
        .map_err(storage)?;
        outbox(
            tx,
            &self.config.subject(&format!("results.{}", e.id)),
            confirmation,
            &e.traceparent,
        )
        .await
    }
    async fn publisher(&self, s: MailService) {
        let mut stop = self.publisher_stop.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {_=stop.changed()=>break,_=tick.tick()=>{}}
            if self.reconcile(&s).await.is_err() {
                continue;
            }
            for _ in 0..100 {
                if *stop.borrow() {
                    return;
                }
                let Ok(mut tx) = s.db.begin().await else {
                    break;
                };
                // One publisher globally serializes published change watermarks.
                if sqlx::query("SELECT pg_advisory_xact_lock(763194815)")
                    .execute(&mut *tx)
                    .await
                    .is_err()
                {
                    break;
                }
                let Ok(Some(row))=sqlx::query("SELECT * FROM command_outbox WHERE NOT published ORDER BY created_at,id LIMIT 1 FOR UPDATE").fetch_optional(&mut *tx).await else{break};
                let id: Uuid = row.get("id");
                let Ok(subject) = self.config.outbox_subject(&row.get::<String, _>("subject"))
                else {
                    break;
                };
                let key = if let Some(id) = subject.strip_prefix(&self.config.subject("results.")) {
                    format!("result:{id}")
                } else {
                    id.to_string()
                };
                if self
                    .publish(
                        &subject,
                        row.get("payload"),
                        &key,
                        &row.get::<String, _>("traceparent"),
                    )
                    .await
                    .is_err()
                {
                    break;
                }
                if sqlx::query("UPDATE command_outbox SET published=true,subject=$2 WHERE id=$1")
                    .bind(id)
                    .bind(&subject)
                    .execute(&mut *tx)
                    .await
                    .is_err()
                {
                    break;
                }
                if tx.commit().await.is_err() {
                    break;
                }
            }
        }
    }
    async fn reconcile(&self, s: &MailService) -> Result<(), Status> {
        let mut tx = s.db.begin().await.map_err(storage)?;
        let rows=sqlx::query("SELECT d.id,d.traceparent FROM deliveries d LEFT JOIN delivery_commands c ON c.delivery_id=d.id WHERE d.status=1 AND c.delivery_id IS NULL LIMIT 100 FOR UPDATE OF d SKIP LOCKED").fetch_all(&mut *tx).await.map_err(storage)?;
        for row in rows {
            let job: Uuid = row.get("id");
            let command = Uuid::new_v4();
            let parent: String = row.get("traceparent");
            sqlx::query("INSERT INTO delivery_commands(delivery_id,command_id) VALUES($1,$2)")
                .bind(job)
                .bind(command)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            outbox(
                &mut tx,
                &self.config.subject("commands.delivery"),
                Envelope::inline(command, Command::Deliver(job), &parent)?.encode()?,
                &parent,
            )
            .await?;
        }
        tx.commit().await.map_err(storage)
    }
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }
    pub fn stop_watches(&self) {
        self.watch_stop.cancel();
    }
    pub async fn shutdown(&self) {
        self.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(120))
            .await;
    }
    pub async fn shutdown_until(&self, deadline: tokio::time::Instant) {
        self.begin_drain();
        self.stop_watches();
        self.stop.send_replace(true);
        self.running.store(false, Ordering::Relaxed);
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("worker task lock"));
        for mut task in tasks {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
        self.publisher_stop.send_replace(true);
        let publisher = self
            .publisher_task
            .lock()
            .expect("publisher task lock")
            .take();
        if let Some(mut task) = publisher
            && tokio::time::timeout_at(deadline, &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
        let _ = tokio::time::timeout_at(deadline, self.client.flush()).await;
    }
    pub async fn watch(&self, resume: String) -> Result<ChangeStream, Status> {
        if self.watch_stop.is_cancelled() {
            return Err(Status::unavailable("server draining"));
        }
        let mut stream = self
            .js
            .get_stream(self.config.resource("CHANGES"))
            .await
            .map_err(storage)?;
        let info = stream.info().await.map_err(storage)?;
        let namespace = self.config.namespace().to_owned();
        let generation = format!("{namespace}:{}", info.created);
        let last = info.state.last_sequence;
        let parsed = watch_cursor(&resume, &generation, info.state.first_sequence, last);
        let reset = parsed.is_none();
        let mut seq = parsed.unwrap_or(last);
        let consumer = stream
            .create_consumer(pull::Config {
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: seq + 1,
                },
                ack_policy: AckPolicy::Explicit,
                max_ack_pending: 16,
                inactive_threshold: Duration::from_secs(30),
                ..Default::default()
            })
            .await
            .map_err(storage)?;
        let consumer_name = consumer.cached_info().name.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let lagged = Arc::new(AtomicBool::new(false));
        let reset_seq = Arc::new(AtomicU64::new(seq));
        let lagged_task = lagged.clone();
        let reset_task = reset_seq.clone();
        let final_generation = generation.clone();
        let stop = self.watch_stop.clone();
        tokio::spawn(async move {
            if reset {
                let _ = sender.send(Ok(event(1, &generation, seq))).await;
            } else {
                let _ = sender.send(Ok(event(5, &generation, seq))).await;
            }
            'watch: loop {
                let batch = tokio::select! {_=stop.cancelled()=>break,_=sender.closed()=>break,b=consumer.batch().max_messages(16).expires(Duration::from_secs(15)).messages()=>b};
                let Ok(mut batch) = batch else {
                    let _ = sender.try_send(Err(Status::unavailable("change stream unavailable")));
                    break;
                };
                loop {
                    let next = tokio::select! {_=stop.cancelled()=>break 'watch,_=sender.closed()=>break 'watch,m=batch.next()=>m};
                    let Some(Ok(message)) = next else { break };
                    let Ok(metadata) = message.info() else {
                        break 'watch;
                    };
                    let received = metadata.stream_sequence;
                    if received != seq + 1 {
                        if sender
                            .try_send(Ok(event(1, &generation, received)))
                            .is_err()
                        {
                            reset_task.store(received, Ordering::Relaxed);
                            lagged_task.store(true, Ordering::Relaxed);
                            break 'watch;
                        }
                        seq = received;
                        let _ = message.ack().await;
                        continue;
                    }
                    let Ok(mut change) = ChangeEvent::decode(message.payload.as_ref()) else {
                        break 'watch;
                    };
                    change.resume_token = token(&generation, received);
                    if tokio::select! {
                        _ = stop.cancelled() => break 'watch,
                        result = tokio::time::timeout(Duration::from_secs(15), sender.send(Ok(change))) => result
                    }
                        .is_err()
                    {
                        reset_task.store(received, Ordering::Relaxed);
                        lagged_task.store(true, Ordering::Relaxed);
                        break 'watch;
                    }
                    seq = received;
                    let _ = message.ack().await;
                }
                // Detect expiry even when a consumer has no next retained event.
                if let Ok(info) = stream.info().await
                    && (format!("{namespace}:{}", info.created) != generation
                        || seq + 1 < info.state.first_sequence)
                {
                    seq = info.state.last_sequence;
                    if sender.try_send(Ok(event(1, &generation, seq))).is_err() {
                        reset_task.store(seq, Ordering::Relaxed);
                        lagged_task.store(true, Ordering::Relaxed);
                        break;
                    }
                    break;
                }
                if sender.try_send(Ok(event(5, &generation, seq))).is_err() {
                    reset_task.store(seq, Ordering::Relaxed);
                    lagged_task.store(true, Ordering::Relaxed);
                    break;
                }
            }
            let _ = stream.delete_consumer(&consumer_name).await;
        });
        let terminal = futures_util::stream::once(async move {
            lagged.load(Ordering::Relaxed).then(|| {
                Ok(event(
                    1,
                    &final_generation,
                    reset_seq.load(Ordering::Relaxed),
                ))
            })
        })
        .filter_map(std::future::ready);
        Ok(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver).chain(terminal),
        ))
    }
}
fn isolated_stream(config: &Config, staging: bool) -> bool {
    config.republish.is_none()
        && config.subject_transform.is_none()
        && config.consumer_limits.is_none()
        && !matches!(
            config.persist_mode,
            Some(jetstream::stream::PersistenceMode::Async)
        )
        && config.template_owner.is_empty()
        && !config.allow_message_ttl
        && !config.allow_message_schedules
        && !config.allow_message_counter
        && !config.discard_new_per_subject
        && config.allow_rollup == staging
}
fn isolated_consumer(config: &jetstream::consumer::Config, durable: &str) -> bool {
    config.durable_name.as_deref() == Some(durable)
        && config.ack_policy == AckPolicy::Explicit
        && config.ack_wait == Duration::from_secs(120)
        && config.max_ack_pending == 1
        && config.max_deliver == -1
        && config.filter_subject.is_empty()
        && config.filter_subjects.is_empty()
        && config.deliver_policy == DeliverPolicy::All
        && config.deliver_subject.is_none()
}
fn token(generation: &str, seq: u64) -> String {
    URL_SAFE_NO_PAD.encode(format!("{generation}\n{seq}"))
}
fn watch_cursor(value: &str, generation: &str, first: u64, last: u64) -> Option<u64> {
    if value.len() > 512 {
        return None;
    }
    let bytes = URL_SAFE_NO_PAD.decode(value).ok()?;
    let text = std::str::from_utf8(&bytes).ok()?;
    let (g, s) = text.split_once('\n')?;
    let seq: u64 = s.parse().ok()?;
    (g == generation && seq <= last && seq.saturating_add(1) >= first).then_some(seq)
}
fn event(kind: i32, generation: &str, seq: u64) -> ChangeEvent {
    ChangeEvent {
        resume_token: token(generation, seq),
        kind,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "requires explicitly isolated INBOX_TEST_NATS_URL"]
    async fn shutdown_keeps_publisher_until_workers_finish_and_bounds_all_tasks() {
        use super::*;
        let url = std::env::var("INBOX_TEST_NATS_URL").unwrap();
        for forced in [false, true] {
            let pipeline = Pipeline::connect(&url).await.unwrap();
            let completed = Arc::new(AtomicBool::new(false));
            let done = completed.clone();
            let p = pipeline.clone();
            pipeline
                .tasks
                .lock()
                .unwrap()
                .push(tokio::spawn(async move {
                    if forced {
                        std::future::pending::<()>().await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        assert!(!*p.publisher_stop.borrow());
                        done.store(true, Ordering::Relaxed);
                    }
                }));
            let mut stop = pipeline.publisher_stop.subscribe();
            let publisher = tokio::spawn(async move {
                stop.changed().await.unwrap();
                if forced {
                    std::future::pending::<()>().await;
                }
            });
            *pipeline.publisher_task.lock().unwrap() = Some(publisher);
            let started = tokio::time::Instant::now();
            pipeline
                .shutdown_until(started + Duration::from_millis(200))
                .await;
            assert_eq!(completed.load(Ordering::Relaxed), !forced);
            assert!(started.elapsed() < Duration::from_millis(300));
            assert!(pipeline.tasks.lock().unwrap().is_empty());
            assert!(pipeline.publisher_task.lock().unwrap().is_none());
        }
    }
    use super::*;
    #[test]
    fn reused_stream_cannot_transform_republish_expire_or_weaken_confirmation() {
        let mut config = Config::default();
        assert!(isolated_stream(&config, false));
        config.allow_message_ttl = true;
        assert!(!isolated_stream(&config, false));
        config.allow_message_ttl = false;
        config.persist_mode = Some(jetstream::stream::PersistenceMode::Async);
        assert!(!isolated_stream(&config, false));
        config.persist_mode = Some(jetstream::stream::PersistenceMode::Default);
        assert!(isolated_stream(&config, false));
        config.allow_rollup = true;
        assert!(!isolated_stream(&config, false));
        assert!(isolated_stream(&config, true));
        config.republish = Some(jetstream::stream::Republish {
            source: ">".into(),
            destination: "FOREIGN.>".into(),
            headers_only: false,
        });
        assert!(!isolated_stream(&config, true));
    }
    #[test]
    fn cursor_resets_invalid_expired_future_and_generation() {
        let t = token("generation", 5);
        assert_eq!(watch_cursor(&t, "generation", 4, 8), Some(5));
        assert!(watch_cursor(&t, "generation", 7, 8).is_none());
        assert!(watch_cursor(&t, "different", 1, 8).is_none());
        assert!(watch_cursor(&t, "generation", 1, 4).is_none());
        assert!(watch_cursor("bad", "generation", 1, 8).is_none());
    }
    #[test]
    fn serialization_retains_exact_large_raw_without_array_expansion() {
        let raw = vec![255; 25 * 1024 * 1024];
        let bytes = bincode::serialize(&Command::Smtp {
            raw: raw.clone(),
            recipients: vec!["a@inbox.test".into()],
        })
        .unwrap();
        assert!(bytes.len() < MAX_PAYLOAD);
        let Command::Smtp { raw: decoded, .. } = bincode::deserialize(&bytes).unwrap() else {
            panic!()
        };
        assert_eq!(raw, decoded);
    }
}
