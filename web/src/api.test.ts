import { afterEach, expect, it, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { create, toBinary } from '@bufbuild/protobuf';
import { ChangeEventSchema, ChangeKind, CreateMailboxResponseSchema, ListMailboxesResponseSchema } from './gen/inbox/v1/inbox_pb';

afterEach(() => {vi.useRealTimers(); vi.unstubAllGlobals();});
it('collects every directory page and rejects repeated tokens, failed pages, and cancellation without partial results', async () => {
  vi.stubGlobal('location', {origin: 'http://synthetic.test'});
  const {collectPages} = await import('./api');
  const read = vi.fn(async (page: string) => ({items: [page || 'first'], nextPageToken: page ? '' : 'second'}));
  expect(await collectPages(read)).toEqual([{items: ['first'], nextPageToken: 'second'}, {items: ['second'], nextPageToken: ''}]);
  expect(read.mock.calls.map(([page]) => page)).toEqual(['', 'second']);
  await expect(collectPages(async () => ({nextPageToken: 'loop'}))).rejects.toThrow('pagination');
  await expect(collectPages(async page => {if (page) throw new Error('page unavailable'); return {nextPageToken: 'next'};})).rejects.toThrow('page unavailable');
  const abort = new AbortController(); abort.abort();
  await expect(collectPages(read, abort.signal)).rejects.toThrow('Aborted');
});
it('caches every domain page for offline selectors and forwards the query abort signal to generated RPC reads', async () => {
  vi.stubGlobal('location', {origin: 'http://domain-cache.synthetic.test'});
  vi.stubGlobal('navigator', {onLine: true});
  const {allDomains, client} = await import('./api');
  const read = vi.spyOn(client, 'listDomains').mockImplementation(async request => ({
    $typeName: 'inbox.v1.ListDomainsResponse',
    domains: [{$typeName: 'inbox.v1.Domain', id: request.pageToken ? 'two' : 'one', name: request.pageToken ? 'two.test' : 'one.test', receiveEnabled: false, sendEnabled: true, createdAt: '', updatedAt: ''}],
    nextPageToken: request.pageToken ? '' : 'next',
  }));
  try {
    const signal = new AbortController().signal;
    expect((await allDomains(signal)).map(domain => domain.id)).toEqual(['one', 'two']);
    expect(read.mock.calls).toEqual([
      [{pageSize: 50, pageToken: ''}, {signal}],
      [{pageSize: 50, pageToken: 'next'}, {signal}],
    ]);
    vi.stubGlobal('navigator', {onLine: false});
    expect((await allDomains()).map(domain => domain.name)).toEqual(['one.test', 'two.test']);
    expect(read).toHaveBeenCalledTimes(2);
  } finally {read.mockRestore();}
});
it('uses actual gRPC-Web streaming without inheriting the 15-second unary deadline', async () => {
  vi.useFakeTimers();
  vi.stubGlobal('location', {origin: 'http://synthetic.test'});
  let output!: ReadableStreamDefaultController<Uint8Array>;
  let signal!: AbortSignal;
  const request = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    signal = init!.signal!;
    const body = new ReadableStream<Uint8Array>({start(controller) {output = controller;}});
    signal.addEventListener('abort', () => output.error(new DOMException('Aborted', 'AbortError')), {once: true});
    return new Response(body, {headers: {'content-type': 'application/grpc-web+proto'}});
  });
  vi.stubGlobal('fetch', request);
  const {changeStream} = await import('./api');
  const abort = new AbortController();
  const iterator = changeStream('private-cursor', abort.signal)[Symbol.asyncIterator]();
  const first = iterator.next();
  await vi.advanceTimersByTimeAsync(0);
  expect(request).toHaveBeenCalledTimes(1);
  expect(new Headers(request.mock.calls[0][1]?.headers).has('grpc-timeout')).toBe(false);
  await vi.advanceTimersByTimeAsync(16000);
  expect(signal.aborted).toBe(false);
  const payload = toBinary(ChangeEventSchema, create(ChangeEventSchema, {kind: ChangeKind.HEARTBEAT, resumeToken: 'private-cursor'}));
  const frame = new Uint8Array(5 + payload.length);
  new DataView(frame.buffer).setUint32(1, payload.length); frame.set(payload, 5);
  output.enqueue(frame);
  expect((await first).value?.kind).toBe(ChangeKind.HEARTBEAT);
  abort.abort();
  await iterator.return?.();
  expect(signal.aborted).toBe(true);
});
it('does not dispatch an RPC until identity revalidation completes and cancels an old generation before dispatch', async () => {
  vi.stubGlobal('location', {origin: 'https://account.synthetic.test'});
  vi.stubGlobal('navigator', {onLine: true});
  const request = vi.fn(async () => new Response());
  vi.stubGlobal('fetch', request);
  const {accountApi} = await import('./api');
  const {createStorage} = await import('./storage');
  const controller = new AbortController();
  const assert = () => {if (controller.signal.aborted) throw new DOMException('Session changed', 'AbortError');};
  let complete!: () => void;
  let started!: () => void;
  const verifying = new Promise<void>(resolve => {started = resolve;});
  const api = accountApi({
    identity: {mode: 'oidc', authenticated: true, user_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', administrator: false, expires_at: new Date(Date.now() + 60000).toISOString()},
    namespace: 'synthetic-account-rpc', signal: controller.signal, assert,
    reject: () => controller.abort(),
    verify: () => {started(); return new Promise<void>(resolve => {complete = resolve;});},
  }, createStorage(() => 'synthetic-account-rpc', assert));
  const pending = api.client.createMailbox({domainId: 'domain', localPart: 'me', name: 'Me', requestId: 'private-command'});
  await verifying;
  expect(request).not.toHaveBeenCalled();
  controller.abort(); complete();
  await expect(pending).rejects.toThrow('Session changed');
  expect(request).not.toHaveBeenCalled();
});
it('pins reads, mutations and streams to the expected user when the cookie changes after session preflight', async () => {
  vi.stubGlobal('location', {origin: `https://${crypto.randomUUID()}.test`});
  vi.stubGlobal('navigator', {onLine: true});
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
  const A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  const B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  let cookie = A;
  let switchAfterPreflight = false;
  let writes = 0;
  let reads = 0;
  const headers: {method: string; expected: string | null; actor: string}[] = [];
  const frame = (payload: Uint8Array) => {
    const trailer = new TextEncoder().encode('grpc-status: 0\r\n');
    const bytes = new Uint8Array(10 + payload.length + trailer.length);
    const view = new DataView(bytes.buffer);
    view.setUint32(1, payload.length); bytes.set(payload, 5);
    bytes[5 + payload.length] = 128;
    view.setUint32(6 + payload.length, trailer.length); bytes.set(trailer, 10 + payload.length);
    return new Response(bytes, {headers: {'content-type': 'application/grpc-web+proto'}});
  };
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === '/auth/session') {
      const response = Response.json({mode: 'oidc', authenticated: true, user_id: cookie, administrator: false, expires_at: new Date(Date.now() + 60000).toISOString()});
      if (switchAfterPreflight) {cookie = B; switchAfterPreflight = false;}
      return response;
    }
    const method = url.split('/').pop()!;
    const expected = new Headers(init?.headers).get('x-inbox-expected-user');
    headers.push({method, expected, actor: cookie});
    // Model the backend admission check before any handler or projection runs.
    if (expected !== cookie) return new Response('Rejected before handler', {status: 401});
    if (method === 'ListMailboxes') {
      reads++;
      return frame(toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{id: `mailbox-${cookie}`, name: 'Owned'}]})));
    }
    if (method === 'CreateMailbox') {
      writes++;
      return frame(toBinary(CreateMailboxResponseSchema, create(CreateMailboxResponseSchema, {mailbox: {id: `created-${cookie}`}})));
    }
    return frame(toBinary(ChangeEventSchema, create(ChangeEventSchema, {kind: ChangeKind.HEARTBEAT})));
  }));
  const {Session} = await import('./session');
  const {accountApi} = await import('./api');
  const {createStorage} = await import('./storage');
  const session = new Session(() => {});
  try {
    await session.resolve();
    if (session.state.status !== 'ready') throw new Error('Session not ready');
    const account = session.state.account;
    const storage = createStorage(() => account.namespace, account.assert);
    const api = accountApi(account, storage);
    expect((await api.mailboxes('matching')).mailboxes[0].id).toBe(`mailbox-${A}`);
    await api.client.createMailbox({domainId: 'domain', localPart: 'me', requestId: 'matching'});
    const stream = api.changeStream('', new AbortController().signal)[Symbol.asyncIterator]();
    expect((await stream.next()).value?.kind).toBe(ChangeKind.HEARTBEAT);
    await stream.return?.();
    switchAfterPreflight = true;
    await expect(api.mailboxes('racing')).rejects.toThrow();
    expect(headers.at(-1)).toEqual({method: 'ListMailboxes', expected: A, actor: B});
    expect(account.signal.aborted).toBe(true);
    expect(reads).toBe(1);
    expect(writes).toBe(1);
    const retained = createStorage(() => account.namespace);
    vi.stubGlobal('navigator', {onLine: false});
    await expect(retained.cached('boxes:racing', async () => 'never')).rejects.toThrow('Not cached');
    await expect(storage.drafts.save(storage.newDraft('A'))).rejects.toThrow('Session changed');
    vi.stubGlobal('navigator', {onLine: true});
    cookie = A;
    await session.resolve();
    if (session.state.status !== 'ready') throw new Error('Session not ready');
    const next = session.state.account;
    const apiNext = accountApi(next, createStorage(() => next.namespace, next.assert));
    switchAfterPreflight = true;
    await expect(apiNext.client.createMailbox({domainId: 'domain', localPart: 'me', requestId: 'racing'})).rejects.toThrow();
    expect(writes).toBe(1);
    expect(headers.at(-1)).toEqual({method: 'CreateMailbox', expected: A, actor: B});
    expect(headers.every(header => header.expected === A)).toBe(true);
    expect(headers.map(header => header.method)).toContain('WatchChanges');
  } finally {session.lock();}
});
