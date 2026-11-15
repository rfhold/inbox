import { afterEach, expect, it, vi } from 'vitest';
import 'fake-indexeddb/auto';

function session(initial = new Map<string, string>()) {
  const values = new Map(initial);
  return {values, getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => {values.set(key, value);}};
}
function webLocks() {
  const held = new Map<string, string>();
  return {
    document: (documentId: string) => ({
      request: async (name: string, _options: object, callback: (lock: object | null) => Promise<void>) => {
        if (held.has(name)) return callback(null);
        held.set(name, documentId);
        try {await callback({name});} finally {if (held.get(name) === documentId) held.delete(name);}
      },
    }),
    unload: (documentId: string) => {for (const [name, holder] of held) if (holder === documentId) held.delete(name);},
  };
}
afterEach(() => {vi.unstubAllGlobals(); vi.resetModules();});

it('isolates cloned session storage and restores each immutable management request after reload; B success cannot remove A', async () => {
  const locks = webLocks();
  const aSession = session();
  const origin = `http://cloned-tabs-${crypto.randomUUID()}.synthetic.test`;
  vi.stubGlobal('location', {origin});
  async function document(id: string, storage: ReturnType<typeof session>) {
    vi.stubGlobal('sessionStorage', storage);
    vi.stubGlobal('navigator', {onLine: true, locks: locks.document(id)});
    vi.resetModules();
    return (await import('./storage')).commands;
  }
  const a = await document('A', aSession);
  const keys = ['createDomain', 'createMailbox', 'policy:same-domain'];
  const aIntents = keys.map((operation, index) => ({operation, name: `a-${index}`, domainId: 'same-domain', requestId: crypto.randomUUID()}));
  for (const [index, key] of keys.entries()) await a.save(key, aIntents[index]);
  // A real opener/duplicate begins with the exact same session-storage namespace.
  const bSession = session(aSession.values);
  expect(bSession.getItem('inbox:command-tab')).toBe(aSession.getItem('inbox:command-tab'));
  const b = await document('B', bSession);
  const bIntents = keys.map((operation, index) => ({operation, name: `b-${index}`, domainId: 'same-domain', requestId: crypto.randomUUID()}));
  for (const [index, key] of keys.entries()) await b.save(key, bIntents[index]);
  expect(bSession.getItem('inbox:command-tab')).not.toBe(aSession.getItem('inbox:command-tab'));
  for (const [index, key] of keys.entries()) {
    expect(await a.get(key)).toEqual(aIntents[index]);
    expect(await b.get(key)).toEqual(bIntents[index]);
    await expect(b.save(key, {...bIntents[index], name: 'overwrite'})).rejects.toThrow('immutable payload');
    await expect(b.save(key, {...bIntents[index], requestId: crypto.randomUUID()})).rejects.toThrow('immutable payload');
    await expect(b.remove(key, aIntents[index].requestId)).rejects.toThrow('immutable payload');
  }
  locks.unload('A');
  const reloadedA = await document('A-reload', aSession);
  for (const [index, key] of keys.entries()) expect(await reloadedA.get(key)).toEqual(aIntents[index]);
  locks.unload('B');
  const reloadedB = await document('B-reload', bSession);
  for (const [index, key] of keys.entries()) {
    expect(await reloadedB.get(key)).toEqual(bIntents[index]);
    await reloadedB.remove(key, bIntents[index].requestId);
    expect(await reloadedB.get(key)).toBeUndefined();
    expect(await reloadedA.get(key)).toEqual(aIntents[index]);
  }
  locks.unload('A-reload');
  const againA = await document('A-second-reload', aSession);
  for (const [index, key] of keys.entries()) expect(await againA.get(key)).toEqual(aIntents[index]);
});

it('fails closed when exclusive tab ownership is unavailable, without rewriting a recovery namespace', async () => {
  const storage = session(new Map([['inbox:command-tab', 'original-owner']]));
  vi.stubGlobal('location', {origin: 'http://unsupported-locks.synthetic.test'});
  vi.stubGlobal('sessionStorage', storage);
  vi.stubGlobal('navigator', {onLine: true});
  const {commands} = await import('./storage');
  await expect(commands.save('createDomain', {requestId: 'unconfirmed-key'})).rejects.toThrow('Web Locks');
  expect(storage.getItem('inbox:command-tab')).toBe('original-owner');
});
