import { beforeAll, describe, expect, it, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { ChangeKind } from './gen/inbox/v1/inbox_pb';
import { cached, clearMail, commands, drafts, invalidateMail, jobs, newDraft, resumeToken } from './storage';

const origin = 'http://upgrade.synthetic.test';
const locks = {request: (_name: string, _options: object, callback: (lock: object) => Promise<void>) => callback({})};
beforeAll(async () => {
  vi.stubGlobal('location', {origin}); vi.stubGlobal('navigator', {onLine: true});
  const session = new Map<string, string>();
  vi.stubGlobal('sessionStorage', {getItem: (key: string) => session.get(key) ?? null, setItem: (key: string, value: string) => session.set(key, value)});
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.open(`inbox:${origin}:shared`, 1);
    request.onupgradeneeded = () => {for (const name of ['mail-v1', 'drafts', 'jobs']) request.result.createObjectStore(name);};
    request.onsuccess = () => {
      const tx = request.result.transaction(['drafts', 'jobs', 'mail-v1'], 'readwrite');
      tx.objectStore('drafts').put({...newDraft('legacy'), id: 'legacy', key: 'legacy-key'}, 'legacy');
      tx.objectStore('jobs').put({id: 'legacy-job', status: 1}, 'legacy-job');
      tx.objectStore('mail-v1').put({data: 'legacy-cache', saved: Date.now(), bytes: 12}, 'boxes:legacy');
      tx.oncomplete = () => {request.result.close(); resolve();}; tx.onerror = () => reject(tx.error);
    }; request.onerror = () => reject(request.error);
  });
});
describe('durable change invalidations', () => {
  it('upgrades v1 without clearing drafts, jobs, or existing cached mail', async () => {
    expect((await drafts.all()).find(value => value.id === 'legacy')?.key).toBe('legacy-key');
    expect(await jobs.all()).toContainEqual({id: 'legacy-job', status: 1});
    vi.stubGlobal('navigator', {onLine: false});
    expect(await cached('boxes:legacy', async () => null)).toBe('legacy-cache');
    vi.stubGlobal('navigator', {onLine: true});
  });
  it('invalidates all domain pages and mailbox state without touching mail, drafts, jobs or manual retries', async () => {
    vi.stubGlobal('navigator', {onLine: true, locks});
    const intent = {domainId: 'one', receiveEnabled: false, sendEnabled: true, requestId: 'fixed-key'};
    await commands.save('policy:one', intent);
    for (const key of ['domains:', 'domains:next', 'boxes:', 'list:kept:1:']) await cached(key, async () => key);
    await invalidateMail({kind: ChangeKind.DOMAIN, domainId: 'one', resumeToken: 'domain-cursor', mailboxId: '', entryId: '', submissionId: ''});
    vi.stubGlobal('navigator', {onLine: false});
    for (const key of ['domains:', 'domains:next', 'boxes:']) await expect(cached(key, async () => null)).rejects.toThrow('Not cached');
    expect(await cached('list:kept:1:', async () => null)).toBe('list:kept:1:');
    expect(await commands.get('policy:one')).toEqual(intent);
    await clearMail();
    expect(await commands.get('policy:one')).toEqual(intent);
    vi.resetModules();
    const reloaded = await import('./storage');
    vi.stubGlobal('navigator', {onLine: false, locks});
    expect(await reloaded.commands.get('policy:one')).toEqual(intent);
    expect((await drafts.all()).find(value => value.id === 'legacy')?.key).toBe('legacy-key');
    await commands.remove('policy:one', intent.requestId);
    expect(await commands.get('policy:one')).toBeUndefined();
    vi.stubGlobal('navigator', {onLine: true});
  });
  it('atomically saves cursor and scoped invalidation; heartbeat and cache clearing preserve drafts', async () => {
    await clearMail();
    for (const key of ['boxes:', 'list:a:1:', 'list:a:2:', 'detail:a:e', 'list:b:1:']) await cached(key, async () => key);
    await invalidateMail({kind: ChangeKind.MESSAGE, resumeToken: 'opaque-1', mailboxId: 'a', entryId: 'e', submissionId: ''});
    expect(await resumeToken()).toBe('opaque-1');
    await invalidateMail({kind: ChangeKind.HEARTBEAT, resumeToken: 'ignored', mailboxId: '', entryId: '', submissionId: ''});
    expect(await resumeToken()).toBe('opaque-1');
    vi.stubGlobal('navigator', {onLine: false});
    for (const key of ['list:a:1:', 'list:a:2:', 'detail:a:e']) await expect(cached(key, async () => null)).rejects.toThrow('Not cached');
    expect(await cached('boxes:', async () => null)).toBe('boxes:');
    expect(await cached('list:b:1:', async () => null)).toBe('list:b:1:');
    await clearMail();
    expect((await drafts.all()).find(value => value.id === 'legacy')?.key).toBe('legacy-key');
    vi.stubGlobal('navigator', {onLine: true});
  });
  it('rejects an in-flight stale response even when another tab invalidates and repopulates the same key', async () => {
    let finish!: (value: string) => void;
    let started!: () => void;
    const ready = new Promise<void>(resolve => {started = resolve;});
    const pending = cached('list:shared:1:', () => {started(); return new Promise<string>(resolve => {finish = resolve;});});
    const rejection = expect(pending).rejects.toThrow('Mail changed');
    await ready;
    vi.resetModules();
    const otherTab = await import('./storage');
    await otherTab.invalidateMail({kind: ChangeKind.MESSAGE, resumeToken: 'shared-cursor', mailboxId: 'shared', entryId: '', submissionId: ''});
    await otherTab.cached('list:shared:1:', async () => 'fresh');
    finish('stale'); await rejection;
    vi.stubGlobal('navigator', {onLine: false});
    expect(await cached('list:shared:1:', async () => null)).toBe('fresh');
    expect(await resumeToken()).toBe('shared-cursor');
    vi.stubGlobal('navigator', {onLine: true});
  });
  it('resets mail trust and cursor without changing durable drafts or jobs', async () => {
    await cached('detail:old:e', async () => 'old');
    await invalidateMail({kind: ChangeKind.RESET, resumeToken: 'new-generation', mailboxId: '', entryId: '', submissionId: ''});
    expect(await resumeToken()).toBe('new-generation');
    vi.stubGlobal('navigator', {onLine: false});
    await expect(cached('detail:old:e', async () => null)).rejects.toThrow('Not cached');
    expect((await drafts.all()).find(value => value.id === 'legacy')?.key).toBe('legacy-key');
    expect(await jobs.all()).toContainEqual({id: 'legacy-job', status: 1});
    vi.stubGlobal('navigator', {onLine: true});
  });
});
