import { ChangeKind } from './gen/inbox/v1/inbox_pb';
import type { MailChange } from './storage';

interface WatchOptions {
  stream: (cursor: string, signal: AbortSignal) => AsyncIterable<MailChange>;
  cursor: () => Promise<string>;
  apply: (event: MailChange) => Promise<void>;
  refresh: (batch: RefreshBatch) => Promise<unknown>;
  state?: (state: SyncState) => void;
  isOnline?: () => boolean;
  heartbeatMs?: number;
  random?: () => number;
}
export type SyncState = 'offline' | 'reconnecting' | 'syncing' | 'current';
export interface RefreshBatch {events: readonly MailChange[]; full: boolean; jobs: boolean; signal: AbortSignal;}
export interface WatchControl {stop: () => void; refreshJobs: () => void;}

export function affectedQuery(key: readonly unknown[], event: MailChange): boolean {
  if (event.kind === ChangeKind.HEARTBEAT) return false;
  if (event.kind === ChangeKind.MAILBOX) return key[0] === 'boxes';
  if (event.kind === ChangeKind.DOMAIN) return key[0] === 'domains' || key[0] === 'boxes';
  if (event.kind === ChangeKind.MESSAGE || event.kind === ChangeKind.SUBMISSION) {
    if (key[0] !== 'messages' && key[0] !== 'detail') return false;
    if (event.mailboxId && key[1] !== event.mailboxId) return false;
    return key[0] === 'messages' || !event.entryId || key[2] === event.entryId;
  }
  return true;
}

// One owner, bounded coalescing, and no timer-driven data polling.
export function watchChanges(options: WatchOptions): WatchControl {
  const online = options.isOnline ?? (() => navigator.onLine);
  let stopped = false;
  let running = false;
  let immediate = false;
  let controller: AbortController | undefined;
  let retry: ReturnType<typeof setTimeout> | undefined;
  let heartbeat: ReturnType<typeof setTimeout> | undefined;
  let batch: ReturnType<typeof setTimeout> | undefined;
  let failures = 0;
  let refreshFailures = 0;
  let refreshing = false;
  let unfinished = false;
  let refreshController: AbortController | undefined;
  let ready = false;
  let applying = false;
  let connection = 0;
  let fullStarted = 0;
  let requiresFull = true;
  let fullRefresh = false;
  let jobRefresh = false;
  const pending = new Map<string, MailChange>();
  const hasWork = () => unfinished || fullRefresh || jobRefresh || pending.size > 0;
  const backoff = (attempt: number) => Math.min(30000, 500 * 2 ** Math.min(attempt, 6) * (0.75 + (options.random ?? Math.random)() * 0.5));
  function state() {
    if (!stopped) options.state?.(!online() ? 'offline' : !ready ? 'reconnecting'
      : requiresFull || applying || refreshing || hasWork() ? 'syncing' : 'current');
  }
  function schedule(delay: number) {
    if (stopped || !online() || !ready || refreshing || batch || !hasWork()) return;
    batch = setTimeout(() => {batch = undefined; void drain();}, delay);
  }
  async function drain() {
    if (stopped || !online() || !ready || refreshing || !hasWork()) return;
    const events = [...pending.values()];
    const full = fullRefresh;
    const jobs = jobRefresh;
    const generation = connection;
    pending.clear(); fullRefresh = false; jobRefresh = false; unfinished = false;
    if (full) fullStarted = generation;
    refreshing = true;
    const active = refreshController = new AbortController();
    let succeeded = false;
    state();
    try {
      await options.refresh({events, full, jobs, signal: active.signal});
      if (active.signal.aborted) throw new Error('Reconciliation interrupted');
      succeeded = true;
      refreshFailures = 0;
    } catch {
      // The consumer retains individual failed views/jobs; an empty retry drains only those.
      unfinished = true;
    } finally {
      refreshing = false;
      refreshController = undefined;
      if (succeeded && generation === connection && ready && fullStarted === generation && !applying && !hasWork()) requiresFull = false;
      state();
      schedule(succeeded ? 100 : backoff(refreshFailures++));
    }
  }
  function enqueue(event?: MailChange, full = false, jobs = false) {
    if (stopped) return;
    fullRefresh ||= full || (event !== undefined && ![ChangeKind.DOMAIN, ChangeKind.MAILBOX, ChangeKind.MESSAGE, ChangeKind.SUBMISSION].includes(event.kind));
    jobRefresh ||= jobs;
    if (fullRefresh) requiresFull = true;
    if (event && !fullRefresh) {
      pending.set(`${event.kind}:${event.domainId ?? ''}:${event.mailboxId}:${event.entryId}:${event.submissionId}`, event);
      if (pending.size > 64) fullRefresh = true;
    }
    if (fullRefresh) pending.clear();
    state();
    schedule(100);
  }
  async function run() {
    if (stopped || running || !online()) return;
    running = true;
    const generation = ++connection;
    ready = false; requiresFull = true;
    state();
    immediate = false;
    const active = controller = new AbortController();
    const started = Date.now();
    const deadline = () => {
      clearTimeout(heartbeat);
      heartbeat = setTimeout(() => active.abort(), options.heartbeatMs ?? 45000);
    };
    try {
      let token = await options.cursor();
      if (stopped || !online() || active.signal.aborted) return;
      deadline();
      let first = true;
      for await (const event of options.stream(token, active.signal)) {
        if (stopped || !online() || active.signal.aborted) break;
        deadline();
        if (Date.now() - started >= 30000) failures = 0;
        const snapshot = first;
        first = false;
        ready = true;
        if (event.kind === ChangeKind.HEARTBEAT) {
          // A reconnect snapshot follows subscription readiness, not the other way round.
          if (snapshot) enqueue(undefined, true);
          continue;
        }
        if (event.kind !== ChangeKind.RESET && event.resumeToken && event.resumeToken === token) {
          if (snapshot) enqueue(undefined, true);
          continue;
        }
        applying = true; state();
        await options.apply(event);
        token = event.resumeToken;
        enqueue(event, snapshot);
        applying = false; state();
      }
    } catch { /* No cursors, IDs, or raw stream errors enter telemetry or the UI. */ }
    finally {
      active.abort();
      clearTimeout(heartbeat);
      if (generation === connection) {ready = false; applying = false; requiresFull = true;}
      clearTimeout(batch); batch = undefined;
      refreshController?.abort();
      state();
      running = false;
      if (!stopped && online()) {
        if (immediate) void run();
        else {
          retry = setTimeout(() => {retry = undefined; void run();}, backoff(failures++));
        }
      }
    }
  }
  function connectivity() {
    clearTimeout(retry); retry = undefined;
    clearTimeout(batch); batch = undefined;
    ready = false; requiresFull = true;
    refreshController?.abort();
    state();
    immediate = online();
    if (running) controller?.abort();
    else if (online()) void run();
  }
  window.addEventListener('online', connectivity);
  window.addEventListener('offline', connectivity);
  void run();
  const stop = () => {
    stopped = true;
    clearTimeout(retry); clearTimeout(heartbeat); clearTimeout(batch);
    pending.clear(); controller?.abort(); refreshController?.abort();
    window.removeEventListener('online', connectivity);
    window.removeEventListener('offline', connectivity);
  };
  return {stop, refreshJobs: () => enqueue(undefined, false, true)};
}
