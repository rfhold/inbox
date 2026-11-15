use opentelemetry::{
    global,
    propagation::Extractor,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
const FILTER: &str = "off,inbox_server=info";
pub struct Guard {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.tracer.shutdown_with_timeout(Duration::from_secs(2));
        let _ = self.logger.shutdown_with_timeout(Duration::from_secs(2));
    }
}
pub fn init() -> Result<Guard, Box<dyn std::error::Error + Send + Sync>> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let resource = Resource::builder()
        .with_service_name("inbox-server")
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
        .build();
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:4318".into());
    let traces = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .with_timeout(Duration::from_secs(2))
        .build()?;
    let logs = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/logs", endpoint.trim_end_matches('/')))
        .with_timeout(Duration::from_secs(2))
        .build()?;
    let tracer = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(traces)
        .build();
    let logger = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(logs)
        .build();
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer.tracer("inbox-server"));
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(FILTER))
        .with(telemetry)
        .with(opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger))
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()?;
    Ok(Guard { tracer, logger })
}
struct Parent<'a>(&'a str);
impl Extractor for Parent<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        (key == "traceparent").then_some(self.0)
    }
    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}
pub fn set_parent(span: &tracing::Span, parent: &str) {
    if parent.len() <= 512 {
        let context = global::get_text_map_propagator(|p| p.extract(&Parent(parent)));
        let _ = span.set_parent(context);
    }
}
pub fn ids() -> (String, String) {
    let c = tracing::Span::current().context();
    let s = c.span();
    let x = s.span_context();
    (x.trace_id().to_string(), x.span_id().to_string())
}

pub fn parent_header() -> String {
    let context = tracing::Span::current().context();
    let span = context.span();
    let s = span.span_context();
    if s.is_valid() {
        format!(
            "00-{}-{}-{:02x}",
            s.trace_id(),
            s.span_id(),
            s.trace_flags().to_u8()
        )
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
    };
    #[test]
    fn exports_logs_and_traces_to_local_fake_collector() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let captured = Arc::new(Mutex::new(Vec::new()));
        let output = captured.clone();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while output.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
                let (mut conn, _) = match listener.accept() {
                    Ok(c) => c,
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                };
                conn.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                let mut data = Vec::new();
                let mut b = [0u8; 4096];
                let header_end = loop {
                    let n = conn.read(&mut b).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&b[..n]);
                    if let Some(p) = data.windows(4).position(|p| p == b"\r\n\r\n") {
                        break p + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&data[..header_end]).to_lowercase();
                let len: usize = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                while data.len() < header_end + len {
                    let n = conn.read(&mut b).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&b[..n]);
                }
                assert!(headers.contains("application/x-protobuf"));
                assert!(len > 0);
                output.lock().unwrap().push((
                    headers.lines().next().unwrap().to_owned(),
                    data[header_end..header_end + len].to_vec(),
                ));
                conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            }
        });
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _enter = runtime.enter();
        let trace_export = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{endpoint}/v1/traces"))
            .build()
            .unwrap();
        let log_export = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(format!("{endpoint}/v1/logs"))
            .build()
            .unwrap();
        let tracer = SdkTracerProvider::builder()
            .with_batch_exporter(trace_export)
            .build();
        let logger = SdkLoggerProvider::builder()
            .with_batch_exporter(log_export)
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(FILTER))
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test")))
            .with(opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("safe-operation");
            use opentelemetry::propagation::TextMapPropagator;
            let parent = TraceContextPropagator::new().extract(&Parent(
                "00-01010101010101010101010101010101-0202020202020202-01",
            ));
            span.set_parent(parent).unwrap();
            let _guard = span.enter();
            tracing::info!("safe-event");
            tracing::error!(target:"sqlx","private-address-subject-credentials");
        });
        tracer.force_flush().unwrap();
        logger.force_flush().unwrap();
        tracer.shutdown().unwrap();
        logger.shutdown().unwrap();
        server.join().unwrap();
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 2);
        use prost::Message;
        let traces = &requests
            .iter()
            .find(|r| r.0.contains("/v1/traces"))
            .unwrap()
            .1;
        let logs = &requests
            .iter()
            .find(|r| r.0.contains("/v1/logs"))
            .unwrap()
            .1;
        let traces =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(
                traces.as_slice(),
            )
            .unwrap();
        let logs =
            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(
                logs.as_slice(),
            )
            .unwrap();
        let spans: Vec<_> = traces
            .resource_spans
            .iter()
            .flat_map(|r| r.scope_spans.iter())
            .flat_map(|s| s.spans.iter())
            .collect();
        let records: Vec<_> = logs
            .resource_logs
            .iter()
            .flat_map(|r| r.scope_logs.iter())
            .flat_map(|s| s.log_records.iter())
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(records.len(), 1);
        assert_eq!(spans[0].name, "safe-operation");
        assert_eq!(spans[0].trace_id, vec![1; 16]);
        assert_eq!(spans[0].parent_span_id, vec![2; 8]);
        assert_eq!(records[0].trace_id, spans[0].trace_id);
        assert_eq!(records[0].span_id, spans[0].span_id);
        assert!(!format!("{logs:?}{traces:?}").contains("private-address-subject-credentials"));
    }
}
