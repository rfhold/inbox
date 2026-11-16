import { ChangeKind, type ChangeEvent } from './gen/inbox/v1/inbox_pb';
import { claimCommandOwner } from './command-owner';

export interface Draft {
  id: string; from: string; to: string; cc: string; subject: string; body: string;
  replyEntry?: string; sourceMailbox?: string; key: string; locked?: boolean; rejected?: boolean;
}
export interface Job { id: string; status: number; recipients?: {address: string; status: number; errorCode: string}[]; }
let commandTab: Promise<string> | undefined;
export function createStorage(namespace: () => string, assert = () => {}) {
let opened: Promise<IDBDatabase> | undefined;
async function db() {
  assert();
  const database = await (opened ??= new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(namespace(), 2);
    request.onupgradeneeded = () => {
      for (const name of ['mail-v1', 'drafts', 'jobs', 'metadata']) if (!request.result.objectStoreNames.contains(name)) request.result.createObjectStore(name);
    };
    request.onsuccess = () => {
      request.result.onversionchange = () => {request.result.close(); opened = undefined;};
      try {assert(); resolve(request.result);} catch (error) {request.result.close(); reject(error);}
    };
    request.onerror = () => reject(request.error);
  }));
  assert();
  return database;
}
async function transaction<T>(name: string, mode: IDBTransactionMode, action: (store: IDBObjectStore) => IDBRequest<T>): Promise<T> {
  if (name === 'mail') name = 'mail-v1';
  const database = await db();
  assert();
  return new Promise((resolve, reject) => {
    const tx = database.transaction(name, mode);
    const request = action(tx.objectStore(name));
    tx.oncomplete = () => resolve(request.result);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
const drafts = {
  all: () => transaction<Draft[]>('drafts', 'readonly', s => s.getAll()),
  save: (draft: Draft) => transaction('drafts', 'readwrite', s => s.put(structuredClone(draft), draft.id)),
  remove: (id: string) => transaction('drafts', 'readwrite', s => s.delete(id)),
};
const jobs = {
  all: () => transaction<Job[]>('jobs', 'readonly', s => s.getAll()),
  save: (job: Job) => transaction('jobs', 'readwrite', s => s.put(job, job.id)),
};
const notifications = {
  dismissed: async () => await transaction<string[] | undefined>('metadata', 'readonly', s => s.get('dismissed-notifications')) ?? [],
  dismiss: (keys: string[]) => transaction('metadata', 'readwrite', s => s.put(keys.slice(-100), 'dismissed-notifications')),
};
async function commandKey(key: string) {
  commandTab ??= (() => {
    if (typeof sessionStorage === 'undefined' || !navigator.locks)
      throw new Error('This browser cannot safely own management retries. Use a browser with Web Locks and session storage.');
    return claimCommandOwner(sessionStorage, navigator.locks);
  })();
  return `command:${await commandTab}:${key}`;
}
interface SavedCommand {requestId: string;}
async function commandRecord<T>(key: string, mode: IDBTransactionMode, intent?: SavedCommand, resolvedId?: string): Promise<T | undefined> {
  const slot = await commandKey(key);
  const database = await db();
  assert();
  return new Promise((resolve, reject) => {
    const tx = database.transaction('metadata', mode);
    const store = tx.objectStore('metadata');
    let result: T | undefined;
    let failure: Error | undefined;
    const conflict = () => {failure = new Error('An unresolved management request has a different immutable payload. Retry the original request.'); tx.abort();};
    const pointer = store.get(slot);
    pointer.onsuccess = () => {
      const active = pointer.result as string | SavedCommand | undefined;
      const inspect = (stored?: SavedCommand) => {
        if (!intent && !resolvedId) {result = stored as T | undefined; return;}
        if (resolvedId) {
          if (!stored) return;
          if (stored.requestId !== resolvedId) {conflict(); return;}
          store.delete(`${slot}:${resolvedId}`);
          store.delete(slot);
          return;
        }
        if (stored && JSON.stringify(stored) !== JSON.stringify(intent)) {conflict(); return;}
        const recordKey = `${slot}:${intent!.requestId}`;
        const immutable = store.get(recordKey);
        immutable.onsuccess = () => {
          if (immutable.result && JSON.stringify(immutable.result) !== JSON.stringify(intent)) {conflict(); return;}
          if (!immutable.result) store.add(structuredClone(intent), recordKey);
          store.put(intent!.requestId, slot);
        };
      };
      // Older unresolved slot records remain recoverable; only an explicit retry migrates them.
      if (typeof active !== 'string') inspect(active);
      else {
        const record = store.get(`${slot}:${active}`);
        record.onsuccess = () => {
          if (!record.result) {failure = new Error('Saved management request is incomplete. Keep this tab open.'); tx.abort(); return;}
          inspect(record.result);
        };
      }
    };
    tx.oncomplete = () => resolve(result);
    tx.onerror = () => reject(failure ?? tx.error);
    tx.onabort = () => reject(failure ?? tx.error);
  });
}
// Saved commands are manual retries only, never an offline write queue.
const commands = {
  get: <T>(key: string) => commandRecord<T>(key, 'readonly'),
  save: <T extends SavedCommand>(key: string, intent: T) => commandRecord(key, 'readwrite', intent),
  remove: (key: string, requestId: string) => commandRecord(key, 'readwrite', undefined, requestId),
};
async function confirmDraft(draft: Draft, job: Job) {
  const database = await db();
  return new Promise<void>((resolve, reject) => {
    const tx = database.transaction(['drafts', 'jobs'], 'readwrite');
    tx.objectStore('jobs').put(job, job.id);
    tx.objectStore('drafts').delete(draft.id);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
const emptyState = (): CacheState => ({epoch: 0, cursor: ''});
const cacheState = async () => await transaction<CacheState | undefined>('metadata', 'readonly', s => s.get('cache')) ?? emptyState();
const resumeToken = async () => (await cacheState()).cursor;
function affectedMailKey(key: string, event: MailChange): boolean {
  if (event.kind === ChangeKind.HEARTBEAT) return false;
  if (event.kind === ChangeKind.MAILBOX) return key.startsWith('boxes:');
  if (event.kind === ChangeKind.DOMAIN) return key.startsWith('domains:') || key.startsWith('boxes:');
  if (event.kind === ChangeKind.MESSAGE || event.kind === ChangeKind.SUBMISSION) {
    if (!event.mailboxId) return key.startsWith('list:') || key.startsWith('detail:');
    return key.startsWith(`list:${event.mailboxId}:`) || (event.entryId
      ? key === `detail:${event.mailboxId}:${event.entryId}` : key.startsWith(`detail:${event.mailboxId}:`));
  }
  return true;
}
async function invalidateMail(event?: MailChange) {
  if (event?.kind === ChangeKind.HEARTBEAT) return;
  const database = await db();
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction(['mail-v1', 'metadata'], 'readwrite');
    const meta = tx.objectStore('metadata');
    const state = meta.get('cache');
    state.onsuccess = () => {
      const previous: CacheState = state.result ?? emptyState();
      meta.put({epoch: previous.epoch + 1, cursor: event ? event.resumeToken : previous.cursor}, 'cache');
      const store = tx.objectStore('mail-v1');
      if (!event || event.kind === ChangeKind.RESET || event.kind === ChangeKind.UNSPECIFIED) store.clear();
      else {
        const cursor = store.openCursor();
        cursor.onsuccess = () => {
          const item = cursor.result;
          if (item) {if (affectedMailKey(String(item.key), event)) item.delete(); item.continue();}
        };
      }
    };
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
const clearMail = () => invalidateMail();
async function beginMail(key: string) {
  const database = await db();
  return new Promise<string>((resolve, reject) => {
    const tx = database.transaction('mail-v1', 'readwrite');
    const store = tx.objectStore('mail-v1');
    let generation: string;
    const record = store.get(key);
    record.onsuccess = () => {
      // Invalidations delete this generation, including placeholders for in-flight reads.
      generation = record.result?.generation ?? crypto.randomUUID();
      store.put({...record.result, generation, saved: record.result?.saved ?? Date.now(), bytes: record.result?.bytes ?? 0}, key);
    };
    tx.oncomplete = () => resolve(generation);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
async function saveMail(key: string, data: unknown, generation: string) {
  const bytes = new TextEncoder().encode(JSON.stringify(data, (_, value) => typeof value === 'bigint' ? String(value) : value)).length;
  const database = await db();
  return new Promise<boolean>((resolve, reject) => {
    const tx = database.transaction('mail-v1', 'readwrite');
    let saved = false;
    const state = tx.objectStore('mail-v1').get(key);
    state.onsuccess = () => {
      if (state.result?.generation !== generation) return;
      tx.objectStore('mail-v1').put({data, generation, saved: Date.now(), bytes}, key);
      saved = true;
    };
    tx.oncomplete = () => resolve(saved);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}
async function cached<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
  assert();
  if (navigator.onLine) {
    try {
      const generation = await beginMail(key).then(async value => {await trimMail(); return value;}).catch(() => undefined);
      const data = await fetcher();
      assert();
      let accepted = true;
      try {
        if (generation) {accepted = await saveMail(key, data, generation); if (accepted) await trimMail();}
      } catch { /* A full or unavailable cache must not break online reads. */ }
      if (!accepted) throw new CacheInvalidatedError();
      return data;
    } catch (error) {
      // Only fall back on connectivity failure, not application errors.
      if (navigator.onLine) throw error;
    }
  }
  const record = await transaction<{data: T} | undefined>('mail', 'readonly', s => s.get(key));
  assert();
  if (!record || record.data === undefined) throw new Error('Not cached on this device. Connect to load this view.');
  return record.data;
}
async function trimMail() {
  const database = await db();
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction('mail-v1', 'readwrite');
    const store = tx.objectStore('mail-v1');
    const records: {key: IDBValidKey; saved: number; bytes: number}[] = [];
    const cursor = store.openCursor();
    cursor.onsuccess = () => {
      const item = cursor.result;
      if (item) { records.push({key: item.key, saved: item.value.saved, bytes: item.value.bytes || 0}); item.continue(); }
      else {
        let bytes = 0;
        for (const [index, record] of records.sort((a,b) => b.saved-a.saved).entries()) {
          bytes += record.bytes;
          if (index >= 250 || bytes > 20 * 1024 * 1024) store.delete(record.key);
        }
      }
    };
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}
function newDraft(from: string): Draft {
  return {id: crypto.randomUUID(), key: crypto.randomUUID(), from, to: '', cc: '', subject: '', body: ''};
}
const close = () => {void opened?.then(database => database.close()).catch(() => {}); opened = undefined;};
return {drafts, jobs, notifications, commands, confirmDraft, resumeToken, invalidateMail, clearMail, cached, newDraft, affectedMailKey, close};
}
export type MailChange = Pick<ChangeEvent, 'kind' | 'resumeToken' | 'mailboxId' | 'entryId' | 'submissionId'> & Partial<Pick<ChangeEvent, 'domainId'>>;
interface CacheState { epoch: number; cursor: string; }
export class CacheInvalidatedError extends Error {
  constructor() {super('Mail changed while loading. Refreshing this view.');}
}
// Explicit no-auth callers retain the original database, including local drafts.
export const {drafts, jobs, commands, confirmDraft, resumeToken, invalidateMail, clearMail, cached, newDraft, affectedMailKey} = createStorage(() => `inbox:${location.origin}:shared`);
