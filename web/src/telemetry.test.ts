import { describe, expect, it, vi } from 'vitest';
vi.mock('@grafana/faro-web-sdk', () => ({initializeFaro: vi.fn()}));
vi.mock('@grafana/faro-web-tracing', () => ({TracingInstrumentation: vi.fn()}));
import { collectorHeaders, rpcMethod, startTelemetry } from './telemetry';
describe('telemetry privacy boundary', () => {
  it('strips only the collector transport dedupe and propagation headers', () => {
    const source = new Headers({'Content-Type': 'application/json', 'Idempotency-Key': 'synthetic', traceparent: 'synthetic'});
    const headers = collectorHeaders(source);
    expect(headers.has('idempotency-key')).toBe(false);
    expect(headers.has('traceparent')).toBe(false);
    expect(headers.get('content-type')).toBe('application/json');
    expect(source.get('idempotency-key')).toBe('synthetic');
  });
  it('traces only fixed same-origin RPC method names', () => {
    const origin = 'http://localhost:8080';
    expect(rpcMethod('/inbox.v1.MailService/SubmitMessage', origin)).toBe('SubmitMessage');
    expect(rpcMethod('/inbox.v1.MailService/WatchChanges', origin)).toBe('WatchChanges');
    expect(rpcMethod('/inbox.v1.MailService/WatchChanges?resume_token=private', origin)).toBeUndefined();
    for (const value of ['/config.json', '/collect', '/inbox.v1.MailService/GetMessage?entry=secret', '/inbox.v1.MailService/private-id', 'https://collector.test/inbox.v1.MailService/GetMessage']) expect(rpcMethod(value, origin)).toBeUndefined();
  });
  it('does not initialize or contact a collector when telemetry is disabled', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({json: async () => ({faro_url: '', environment: 'test', version: 'test'})})));
    await startTelemetry();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith('/config.json', expect.anything());
  });
  it('fails open when runtime config is unavailable', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => {throw new Error('offline');}));
    await expect(startTelemetry()).resolves.toBeUndefined();
  });
});
