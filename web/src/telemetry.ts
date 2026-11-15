import { initializeFaro } from '@grafana/faro-web-sdk';
import { TracingInstrumentation } from '@grafana/faro-web-tracing';
import { trace, SpanKind } from '@opentelemetry/api';
const methods = new Set(['CreateMailbox','ListMailboxes','ListMessages','GetMessage','GetRawMessage','SubmitMessage','GetSubmission','SetReadState','WatchChanges']);
export function rpcMethod(url: string, origin: string): string | undefined {
  try {
    const parsed = new URL(url, origin);
    if (parsed.origin !== origin || parsed.search) return;
    const parts = parsed.pathname.split('/');
    if (parts.length === 3 && parts[1] === 'inbox.v1.MailService' && methods.has(parts[2])) return parts[2];
  } catch { /* Invalid URLs are never traced. */ }
}
let report: ((name: string) => void) | undefined;
export function collectorHeaders(headers?: HeadersInit) {
  const result = new Headers(headers);
  for (const name of ['idempotency-key', 'traceparent', 'tracestate', 'baggage']) result.delete(name);
  return result;
}
export function safeEvent(name: 'application_error' | 'message_submitted') {
  try { report?.(name); } catch { /* Telemetry cannot interrupt mail. */ }
}
export async function startTelemetry() {
  try {
    const response = await fetch('/config.json', {cache: 'no-store', signal: AbortSignal.timeout(4000)});
    const config = await response.json();
    if (!config.faro_url) return;
    const endpoint = new URL(config.faro_url, location.origin);
    if (!['http:', 'https:'].includes(endpoint.protocol)) return;
    const faro = initializeFaro({
      url: endpoint.href,
      app: {name: 'inbox-web', environment: String(config.environment || 'compose'), version: String(config.version || 'unknown')},
      instrumentations: [new TracingInstrumentation({instrumentations: []})],
      // Faro's trace sampler requires this fixed opt-in; no per-user session exists.
      metas: [{session: {id: 'untracked', attributes: {isSampled: 'true'}}}],
      sessionTracking: {enabled: false},
      beforeSend: item => {
        // No URL, browser, user, session, DOM or exception metadata is collected.
        item.meta = {app: {name: 'inbox-web', environment: String(config.environment || 'compose'), version: String(config.version || 'unknown')}, page: {url: '/'}};
        return item;
      },
    });
    const original = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = input instanceof Request ? input.url : String(input);
      if (new URL(url, location.origin).href === endpoint.href) {
        // Faro 2.14 adds an optional collector dedupe header unsupported by this collector.
        const headers = collectorHeaders(init?.headers ?? (input instanceof Request ? input.headers : undefined));
        return original(input, {...init, headers});
      }
      const method = rpcMethod(url, location.origin);
      if (!method) return original(input, init);
      const span = trace.getTracer('inbox-web').startSpan(`MailService/${method}`, {kind: SpanKind.CLIENT});
      const context = span.spanContext();
      const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
      if (context.traceId !== '00000000000000000000000000000000') headers.set('traceparent', `00-${context.traceId}-${context.spanId}-01`);
      try {
        const result = await original(input, {...init, headers});
        span.setAttribute('http.response.status_code', result.status);
        return result;
      } catch (error) {
        span.setStatus({code: 2, message: 'rpc_transport_failure'});
        if (method !== 'WatchChanges' && !init?.signal?.aborted) safeEvent('application_error');
        throw error;
      } finally { span.end(); }
    };
    report = name => {
      faro.api.pushEvent(name, {route: '/'}, 'inbox-web');
      if (name === 'application_error') faro.api.pushError(new Error('application_error'), {type: 'application', stackFrames: [], context: {route: '/'}});
    };
    report('application_started');
  } catch { /* Runtime config and collection fail open. */ }
}
