use axum::{
    Json, Router,
    extract::{Request, State},
    middleware,
    response::Response,
    routing::{get, get_service},
};
use inbox_server::{pb::mail_service_server::MailServiceServer, service::MailService, telemetry};
use std::{env, sync::Arc, time::Duration};
use tower_http::services::{ServeDir, ServeFile};
use tracing::Instrument;
fn value(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}
#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("{{\"level\":\"ERROR\",\"message\":\"server startup or shutdown failed\"}}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let telemetry = telemetry::init()?;
    let auth_config = inbox_server::auth::Config::from_env()?;
    let nats_config = inbox_server::nats_config::Config::from_env(auth_config.is_some())?;
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='10s'")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET lock_timeout='5s'")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&env::var("DATABASE_URL")?)
        .await?;
    sqlx::migrate!("../../migrations").run(&db).await?;
    let auth = inbox_server::auth::Auth::initialize(db.clone(), auth_config).await?;
    let hosted = inbox_server::oauth::Hosted::initialize(db.clone(), auth.clone()).await?;
    let credentials = s3::creds::Credentials::new(
        Some(&env::var("AWS_ACCESS_KEY_ID")?),
        Some(&env::var("AWS_SECRET_ACCESS_KEY")?),
        None,
        None,
        None,
    )?;
    let bucket = s3::Bucket::new(
        &value("INBOX_S3_BUCKET", "inbox"),
        s3::Region::Custom {
            region: value("INBOX_S3_REGION", "garage"),
            endpoint: value("INBOX_S3_ENDPOINT", "http://garage:3900"),
        },
        credentials,
    )?
    .with_path_style()
    .with_request_timeout(Duration::from_secs(10))?;
    let mut service = MailService {
        auth: auth.clone(),
        principal: None,
        db: db.clone(),
        bucket: Arc::new(*bucket),
        domain: value("INBOX_DOMAIN", "inbox.test"),
        pipeline: None,
    };
    inbox_server::validation::address(&format!("validation@{}", service.domain), &service.domain)?;
    if env::args().nth(1).as_deref() == Some("--inventory-orphans") {
        inventory(&service).await?;
        return Ok(());
    }
    let pipeline = inbox_server::pipeline::Pipeline::connect_configured(
        &value("INBOX_NATS_URL", "nats://nats:4222"),
        nats_config,
    )
    .await?;
    service.pipeline = Some(pipeline.clone());
    pipeline.start(service.clone()).await?;
    service.bootstrap_domains().await?;
    let smtp =
        inbox_server::smtp::start(&value("INBOX_SMTP_ADDR", "0.0.0.0:2525"), service.clone())?;
    let grpc = tonic::service::Routes::new(
        MailServiceServer::new(service.clone())
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024),
    )
    .into_axum_router()
    .layer(tonic_web::GrpcWebLayer::new())
    .layer(middleware::from_fn_with_state(
        auth.clone(),
        inbox_server::auth::protect,
    ));
    let config = serde_json::json!({"faro_url":value("INBOX_FARO_URL","https://faro.holdenitdown.net/collect"),"environment":value("INBOX_ENVIRONMENT","local"),"version":env!("CARGO_PKG_VERSION")});
    let web = value("INBOX_WEB_DIR", "web/dist");
    let mcp_stop = tokio_util::sync::CancellationToken::new();
    let adapter = inbox_server::mcp::InboxMcp::new(service.clone(), mcp_stop.clone());
    let mcp = if let Some(hosted) = hosted {
        adapter
            .hosted_router(hosted.authorization()?)
            .merge(hosted.router())
    } else {
        adapter.router()
    };
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route(
            "/config.json",
            get(move || {
                let c = config.clone();
                async move { Json(c) }
            }),
        )
        .with_state(service)
        .merge(grpc)
        .merge(mcp)
        .merge(auth.router())
        .route(
            "/domains",
            get_service(ServeFile::new(format!("{web}/index.html"))),
        )
        .route(
            "/mail/{*path}",
            get_service(ServeFile::new(format!("{web}/index.html"))),
        )
        .fallback_service(
            ServeDir::new(&web).not_found_service(ServeFile::new(format!("{web}/index.html"))),
        )
        .layer(middleware::from_fn(trace_request));
    let listener = tokio::net::TcpListener::bind(value("INBOX_HTTP_ADDR", "0.0.0.0:8080")).await?;
    tracing::info!("server ready");
    {
        let (http_stop, http_stopped) = tokio::sync::oneshot::channel();
        let server = axum::serve(listener, app).with_graceful_shutdown(async {
            let _ = http_stopped.await;
        });
        let serving = std::future::IntoFuture::into_future(server);
        tokio::pin!(serving);
        tokio::select! {result=&mut serving=>{mcp_stop.cancel();result?;},_=shutdown()=>{mcp_stop.cancel();let _=http_stop.send(());let _=tokio::time::timeout(Duration::from_secs(15),&mut serving).await;}}
    }
    pipeline.shutdown().await;
    drop(smtp);
    let _ = tokio::time::timeout(Duration::from_secs(5), db.close()).await;
    tokio::task::spawn_blocking(move || drop(telemetry)).await?;
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install termination signal");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
async fn inventory(service: &MailService) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut continuation = None;
    loop {
        let (page, status) = service
            .bucket
            .list_page("raw/".into(), None, continuation, None, Some(1000))
            .await?;
        if status != 200 {
            return Err("archive inventory unavailable".into());
        }
        for object in page.contents {
            let referenced: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages WHERE object_key=$1)")
                    .bind(&object.key)
                    .fetch_one(&service.db)
                    .await?;
            if !referenced {
                println!(
                    "{}",
                    serde_json::json!({"orphan_object_key":object.key,"size":object.size})
                );
            }
        }
        continuation = page.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }
    Ok(())
}
async fn ready(State(s): State<MailService>) -> axum::http::StatusCode {
    if let Some(pipeline) = &s.pipeline
        && pipeline.resources_ready().await
        && sqlx::query("SELECT 1").execute(&s.db).await.is_ok()
        && s.bucket
            .list_page("raw/".into(), None, None, None, Some(1))
            .await
            .is_ok_and(|(_, status)| status == 200)
    {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn trace_request(req: Request, next: middleware::Next) -> Response {
    let span = tracing::info_span!(
        "request",
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty
    );
    if let Some(p) = req
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
    {
        telemetry::set_parent(&span, p)
    }
    async move {
        let (trace_id, span_id) = telemetry::ids();
        tracing::Span::current()
            .record("trace_id", &trace_id)
            .record("span_id", &span_id);
        let result = next.run(req).await;
        tracing::info!(status = result.status().as_u16(), "request completed");
        result
    }
    .instrument(span)
    .await
}
