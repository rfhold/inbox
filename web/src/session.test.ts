import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { Session, type Account, type SessionState } from './session';
import { createStorage } from './storage';
import { legacyLogoutKey, logoutBarrierKey } from './logout-barrier';

const A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const identity = (user_id = A) => ({mode: 'oidc', authenticated: true, user_id, administrator: false, expires_at: new Date(Date.now() + 60000).toISOString()});
let current: SessionState;
let session: Session;
function account(): Account {if (current.status !== 'ready') throw new Error('Not ready'); return current.account;}
beforeEach(() => {
  vi.stubGlobal('location', {origin: `https://${crypto.randomUUID()}.test`});
  vi.stubGlobal('navigator', {onLine: true});
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
  session = new Session(state => {current = state;});
});
afterEach(() => {session.lock(); vi.useRealTimers(); vi.unstubAllGlobals();});
it('fails closed on 401, network failure, malformed identity and expired identity', async () => {
  for (const response of [new Response('{}', {status: 401}), new Response('{}'), Response.json({...identity(), expires_at: new Date(0).toISOString()})]) {
    vi.stubGlobal('fetch', vi.fn(async () => response));
    await session.resolve();
    expect(current.status).not.toBe('ready');
  }
  vi.stubGlobal('fetch', vi.fn(async () => {throw new TypeError('offline');}));
  await session.resolve();
  expect(current.status).toBe('error');
});
it('isolates two accounts and retains legacy noauth drafts without importing them', async () => {
  let value = identity();
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(value)));
  const legacy = createStorage(() => `inbox:${location.origin}:shared`);
  const draft = legacy.newDraft('local'); await legacy.drafts.save(draft);
  await session.resolve();
  const old = account(); const a = createStorage(() => old.namespace, old.assert);
  expect(await a.drafts.all()).toEqual([]);
  const owned = {...a.newDraft('A'), locked: true}; await a.drafts.save(owned);
  await a.cached('boxes:', async () => 'mail A');
  value = identity(B); await session.resolve();
  const next = account(); const b = createStorage(() => next.namespace, next.assert);
  expect(await b.drafts.all()).toEqual([]);
  await expect(a.drafts.save(owned)).rejects.toThrow('Session changed');
  value = identity(A); await session.resolve();
  const restored = account(); const restoredStorage = createStorage(() => restored.namespace, restored.assert);
  expect(await restoredStorage.drafts.all()).toEqual([owned]);
  vi.stubGlobal('navigator', {onLine: false});
  expect(await restoredStorage.cached('boxes:', async () => 'never')).toBe('mail A');
  expect(await legacy.drafts.all()).toEqual([draft]);
});
it('blocks late RPC cache completion and queued autosave after an account switch', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(identity())));
  await session.resolve();
  const old = account(); const storage = createStorage(() => old.namespace, old.assert);
  let finish!: (value: string) => void;
  let started!: () => void;
  const running = new Promise<void>(resolve => {started = resolve;});
  const read = storage.cached('late', () => {started(); return new Promise<string>(resolve => {finish = resolve;});});
  await running;
  session.lock(); finish('private A');
  await expect(read).rejects.toThrow('Session changed');
  await expect(storage.drafts.save(storage.newDraft('A'))).rejects.toThrow('Session changed');
});
it('revokes in-memory access on remote account changes, logout and session expiry', async () => {
  let value = identity();
  const request = vi.fn(async () => Response.json(value));
  vi.stubGlobal('fetch', request);
  await session.resolve(); const old = account();
  value = identity(B);
  await expect(old.verify()).rejects.toThrow();
  expect(old.signal.aborted).toBe(true);
  expect(current.status).toBe('locked');
  await session.resolve(); const next = account();
  await session.logout();
  expect(next.signal.aborted).toBe(true);
  expect(request.mock.calls.at(-1)).toBeDefined();
  vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout', 'Date']});
  value = identity(); await session.resolve(); const last = account();
  await vi.advanceTimersByTimeAsync(60001);
  expect(last.signal.aborted).toBe(true);
});
it('retains the exact explicit noauth namespace', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => Response.json({mode: 'disabled', authenticated: true, user_id: '00000000-0000-4000-8000-000000000001', administrator: true, expires_at: null})));
  await session.resolve();
  expect(account().namespace).toBe(`inbox:${location.origin}:shared`);
});
it('cannot reopen a workspace while durable logout is still pending', async () => {
  let finish!: () => void;
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    if (url === '/auth/logout') {await new Promise<void>(resolve => {finish = resolve;}); return new Response(null, {status: 204});}
    return Response.json(identity());
  }));
  await session.resolve();
  const old = account();
  const logout = session.logout();
  await session.resolve();
  expect(current.status).toBe('loading');
  expect(old.signal.aborted).toBe(true);
  await vi.waitFor(() => expect(finish).toBeTypeOf('function'));
  finish(); await logout;
  expect(current.status).toBe('locked');
});
it('keeps failed/offline sign-out locked across new sessions until server revocation succeeds', async () => {
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
  let offline = false;
  const request = vi.fn(async (url: string) => {
    if (url === '/auth/logout') {if (offline) throw new TypeError('offline'); return new Response(null, {status: 204});}
    return Response.json(identity());
  });
  vi.stubGlobal('fetch', request);
  await session.resolve(); const old = account();
  offline = true; await session.logout();
  expect(old.signal.aborted).toBe(true);
  session = new Session(state => {current = state;});
  await session.resolve();
  expect(current.status).toBe('error');
  offline = false; await session.resolve();
  expect(current.status).toBe('locked');
  expect(values.get(logoutBarrierKey)).toBe('0');
  expect(request.mock.calls.filter(([url]) => url === '/auth/session')).toHaveLength(1);
});
it('uses only a prior noauth tab binding for offline reopening and clears it on a server 401', async () => {
  const values = new Map<string, string>();
  vi.stubGlobal('sessionStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
  vi.stubGlobal('fetch', vi.fn(async () => Response.json({mode: 'disabled', authenticated: true, user_id: '00000000-0000-4000-8000-000000000001', administrator: true, expires_at: null})));
  await session.resolve();
  vi.stubGlobal('navigator', {onLine: false});
  await session.resolve();
  expect(account().identity.mode).toBe('disabled');
  vi.stubGlobal('navigator', {onLine: true});
  vi.stubGlobal('fetch', vi.fn(async () => new Response('{}', {status: 401})));
  await session.resolve();
  expect(current.status).toBe('locked');
  vi.stubGlobal('navigator', {onLine: false});
  await session.resolve();
  expect(current.status).toBe('error');
});
it('refuses authenticated unlock when a tiny probe fits but the actual barrier reservation does not', async () => {
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      if (key.length + value.length > 'inbox:privacy-check'.length + 1) throw new DOMException('Quota full', 'QuotaExceededError');
      values.set(key, value);
    },
    removeItem: (key: string) => values.delete(key),
  });
  localStorage.setItem('inbox:privacy-check', '1');
  localStorage.removeItem('inbox:privacy-check');
  vi.stubGlobal('fetch', vi.fn(async () => Response.json(identity())));
  await session.resolve();
  expect(current.status).toBe('error');
  session = new Session(state => {current = state;});
  await session.resolve();
  expect(current.status).toBe('error');
});
it('persists offline logout in the fallback if the reserved localStorage update fails, blocking reload until server logout', async () => {
  const values = new Map<string, string>();
  let failPendingWrite = false;
  const writes: string[] = [];
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      if (key === logoutBarrierKey) {
        writes.push(value);
        if (failPendingWrite && value === '1') throw new DOMException('Synthetic storage failure', 'QuotaExceededError');
      }
      values.set(key, value);
    },
    removeItem: (key: string) => values.delete(key),
  });
  // The old probe succeeds, but does not establish that the actual logout write will succeed.
  localStorage.setItem('inbox:privacy-check', '1');
  localStorage.removeItem('inbox:privacy-check');
  let offline = false;
  let revoked = false;
  let probes = 0;
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    if (url === '/auth/logout') {
      if (offline) throw new TypeError('offline');
      revoked = true; return new Response(null, {status: 204});
    }
    probes++;
    return revoked ? new Response(null, {status: 401}) : Response.json(identity());
  }));
  await session.resolve();
  const old = account();
  expect(values.get(logoutBarrierKey)).toBe('0');
  failPendingWrite = true; offline = true;
  await session.logout();
  expect(current.status).toBe('error');
  expect(old.signal.aborted).toBe(true);
  expect(values.get(logoutBarrierKey)).toBe('0');
  session = new Session(state => {current = state;});
  await session.resolve();
  expect(current.status).toBe('error');
  expect(probes).toBe(1);
  offline = false;
  await session.resolve();
  expect(current.status).toBe('locked');
  expect(revoked).toBe(true);
  expect(probes).toBe(1);
  await session.resolve();
  expect(current.status).toBe('locked');
  expect(writes.every(value => value.length === 1)).toBe(true);
});
it('uses the reserved fixed-size allocation at full quota and honors legacy pending markers', async () => {
  const values = new Map<string, string>();
  let full = false;
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      const previous = values.get(key);
      if (full && (previous === undefined || value.length > previous.length)) throw new DOMException('Quota full', 'QuotaExceededError');
      values.set(key, value);
    },
    removeItem: (key: string) => values.delete(key),
  });
  vi.stubGlobal('fetch', vi.fn(async (url: string) => url === '/auth/logout' ? new Response(null, {status: 204}) : Response.json(identity())));
  await session.resolve();
  expect(account().identity.user_id).toBe(A);
  full = true;
  await session.logout();
  expect(current.status).toBe('locked');
  expect(values.get(logoutBarrierKey)).toBe('0');
  full = false;
  values.set(legacyLogoutKey, 'true');
  session = new Session(state => {current = state;});
  await session.resolve();
  expect(current.status).toBe('locked');
  expect(values.has(legacyLogoutKey)).toBe(false);
});
it('stays locally locked and explicitly reports unconfirmed durability if both storage writes fail', async () => {
  const values = new Map<string, string>();
  let fail = false;
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {if (fail) throw new Error('Storage unavailable'); values.set(key, value);},
    removeItem: (key: string) => values.delete(key),
  });
  vi.stubGlobal('fetch', vi.fn(async (url: string) => {
    if (url === '/auth/logout') throw new TypeError('offline');
    return Response.json(identity());
  }));
  await session.resolve();
  const old = account();
  fail = true;
  vi.stubGlobal('indexedDB', {open: () => {throw new Error('Storage unavailable');}});
  await session.logout();
  expect(current).toEqual({status: 'error', unsafeLogout: true});
  expect(old.signal.aborted).toBe(true);
  await session.resolve();
  expect(current).toEqual({status: 'error', unsafeLogout: true});
});
