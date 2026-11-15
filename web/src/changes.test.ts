import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ChangeKind } from './gen/inbox/v1/inbox_pb';
import { affectedQuery, watchChanges, type WatchControl } from './changes';
import type { MailChange } from './storage';

const event = (kind: ChangeKind, resumeToken: string): MailChange => ({kind, resumeToken, mailboxId: 'box', entryId: 'entry', submissionId: ''});
function channel(signal: AbortSignal) {
  const queue: MailChange[] = [];
  let waiting: ((result: IteratorResult<MailChange>) => void) | undefined;
  const abort = () => {waiting?.({done: true, value: undefined}); waiting = undefined;};
  signal.addEventListener('abort', abort, {once: true});
  return {
    push(value: MailChange) {if (waiting) {const resolve = waiting; waiting = undefined; resolve({done: false, value});} else queue.push(value);},
    [Symbol.asyncIterator]() {return this;},
    async next(): Promise<IteratorResult<MailChange>> {
      if (signal.aborted) return {done: true, value: undefined};
      if (queue.length) return {done: false, value: queue.shift()!};
      return new Promise(resolve => {waiting = resolve;});
    },
    async return(): Promise<IteratorResult<MailChange>> {signal.removeEventListener('abort', abort); return {done: true, value: undefined};},
  };
}
const tick = async () => {for (let i = 0; i < 10; i++) await Promise.resolve();};
describe('resumable watch owner', () => {
  let stop: WatchControl | undefined;
  beforeEach(() => {vi.useFakeTimers(); vi.stubGlobal('window', new EventTarget());});
  afterEach(() => {stop?.stop(); vi.useRealTimers(); vi.unstubAllGlobals();});
  it('targets both mailbox folders and the affected detail, with broad fallback for missing scope', () => {
    const change = event(ChangeKind.MESSAGE, 'cursor');
    expect(affectedQuery(['messages', 'box', 1], change)).toBe(true);
    expect(affectedQuery(['messages', 'box', 2], change)).toBe(true);
    expect(affectedQuery(['detail', 'box', 'entry'], change)).toBe(true);
    expect(affectedQuery(['detail', 'box', 'other'], change)).toBe(false);
    expect(affectedQuery(['messages', 'other', 1], change)).toBe(false);
    expect(affectedQuery(['messages', 'other', 1], {...change, mailboxId: ''})).toBe(true);
    expect(affectedQuery(['boxes'], event(ChangeKind.MAILBOX, 'cursor'))).toBe(true);
    expect(affectedQuery(['boxes'], event(ChangeKind.HEARTBEAT, 'cursor'))).toBe(false);
    expect(affectedQuery(['domains'], event(ChangeKind.DOMAIN, 'cursor'))).toBe(true);
    expect(affectedQuery(['boxes'], event(ChangeKind.DOMAIN, 'cursor'))).toBe(true);
    expect(affectedQuery(['detail', 'box', 'entry'], event(ChangeKind.DOMAIN, 'cursor'))).toBe(false);
  });
  it('applies changes before refresh, coalesces duplicates, and ignores ordinary heartbeats', async () => {
    let feed!: ReturnType<typeof channel>;
    const refresh = vi.fn(async () => {});
    const apply = vi.fn(async () => {});
    stop = watchChanges({cursor: async () => '', stream: (_, signal) => feed = channel(signal), apply, refresh, isOnline: () => true});
    await tick();
    feed.push(event(ChangeKind.RESET, '1')); await tick();
    await vi.advanceTimersByTimeAsync(100);
    expect(refresh).toHaveBeenLastCalledWith(expect.objectContaining({events: [], full: true}));
    feed.push(event(ChangeKind.MESSAGE, '2'));
    feed.push(event(ChangeKind.MESSAGE, '2'));
    feed.push(event(ChangeKind.HEARTBEAT, '2'));
    await tick();
    expect(apply).toHaveBeenCalledTimes(2);
    expect(refresh).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(100);
    expect(refresh).toHaveBeenLastCalledWith(expect.objectContaining({events: [event(ChangeKind.MESSAGE, '2')], full: false}));
    feed.push(event(ChangeKind.HEARTBEAT, '2')); await tick();
    await vi.advanceTimersByTimeAsync(10000);
    expect(refresh).toHaveBeenCalledTimes(2);
  });
  it('resumes opaque tokens and accepts invalid-cursor RESET as a full refresh', async () => {
    let feed!: ReturnType<typeof channel>;
    const stream = vi.fn((_: string, signal: AbortSignal) => feed = channel(signal));
    const apply = vi.fn(async () => {});
    const refresh = vi.fn(async () => {});
    stop = watchChanges({cursor: async () => 'opaque-old', stream, apply, refresh, isOnline: () => true});
    await tick();
    expect(stream.mock.calls[0][0]).toBe('opaque-old');
    feed.push(event(ChangeKind.RESET, 'opaque-current')); await tick();
    await vi.advanceTimersByTimeAsync(100);
    expect(apply).toHaveBeenCalledWith(event(ChangeKind.RESET, 'opaque-current'));
    expect(refresh).toHaveBeenCalledWith(expect.objectContaining({events: [], full: true}));
  });
  it('backs off exponentially and releases reconnect timers on cleanup', async () => {
    const stream = vi.fn(async function* () {throw new Error('synthetic disconnect');});
    stop = watchChanges({cursor: async () => '', stream, apply: async () => {}, refresh: async () => {}, isOnline: () => true, random: () => .5});
    await tick();
    expect(stream).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(499); expect(stream).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1); expect(stream).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(999); expect(stream).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); expect(stream).toHaveBeenCalledTimes(3);
    stop.stop(); await vi.advanceTimersByTimeAsync(60000);
    expect(stream).toHaveBeenCalledTimes(3);
    expect(vi.getTimerCount()).toBe(0);
  });
  it('pauses offline, aborts once, and immediately resumes with a subscribe-first snapshot', async () => {
    let online = true;
    const signals: AbortSignal[] = [];
    let feed!: ReturnType<typeof channel>;
    const refresh = vi.fn(async () => {});
    const stream = vi.fn((_: string, signal: AbortSignal) => {signals.push(signal); return feed = channel(signal);});
    stop = watchChanges({cursor: async () => 'last', stream, apply: async () => {}, refresh, isOnline: () => online});
    await tick();
    online = false; window.dispatchEvent(new Event('offline')); await tick();
    expect(signals[0].aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(60000); expect(stream).toHaveBeenCalledTimes(1);
    online = true; window.dispatchEvent(new Event('online')); await tick();
    expect(stream).toHaveBeenCalledTimes(2);
    expect(refresh).not.toHaveBeenCalled();
    feed.push(event(ChangeKind.HEARTBEAT, 'last')); await tick();
    await vi.advanceTimersByTimeAsync(100);
    expect(refresh).toHaveBeenCalledWith(expect.objectContaining({events: [], full: true}));
    stop.stop(); expect(signals[1].aborted).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it('survives the unary deadline but restarts a stalled stream at the heartbeat deadline', async () => {
    const signals: AbortSignal[] = [];
    const stream = vi.fn((_: string, signal: AbortSignal) => {signals.push(signal); return channel(signal);});
    stop = watchChanges({cursor: async () => '', stream, apply: async () => {}, refresh: async () => {}, isOnline: () => true, random: () => .5});
    await tick(); await vi.advanceTimersByTimeAsync(16000);
    expect(signals[0].aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(29000);
    expect(signals[0].aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(500);
    expect(stream).toHaveBeenCalledTimes(2);
  });
  it('bounds event storms to one broad snapshot and does not advance after a failed invalidation', async () => {
    let feed!: ReturnType<typeof channel>;
    let token = 'durable';
    let fail = true;
    const apply = vi.fn(async (change: MailChange) => {if (fail) {fail = false; throw new Error('synthetic storage failure');} token = change.resumeToken;});
    const refresh = vi.fn(async () => {});
    const stream = vi.fn((_: string, signal: AbortSignal) => feed = channel(signal));
    stop = watchChanges({cursor: async () => token, stream, apply, refresh, isOnline: () => true, random: () => .5});
    await tick(); feed.push(event(ChangeKind.MESSAGE, 'uncommitted')); await tick();
    expect(token).toBe('durable'); expect(refresh).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(500);
    expect(stream.mock.calls[1][0]).toBe('durable');
    feed.push(event(ChangeKind.HEARTBEAT, 'durable')); await tick();
    await vi.advanceTimersByTimeAsync(100); refresh.mockClear();
    for (let i = 0; i < 200; i++) feed.push({...event(ChangeKind.MESSAGE, `cursor-${i}`), entryId: `entry-${i}`});
    await vi.advanceTimersByTimeAsync(100);
    expect(token).toBe('cursor-199');
    expect(refresh).toHaveBeenCalledOnce();
    expect(refresh).toHaveBeenCalledWith(expect.objectContaining({events: [], full: true}));
  });
  it('retries a failed final refresh without a new event and stays stale until it succeeds', async () => {
    let feed!: ReturnType<typeof channel>;
    const state = vi.fn();
    const refresh = vi.fn<() => Promise<void>>().mockResolvedValueOnce().mockRejectedValueOnce(new Error('synthetic final read failure')).mockResolvedValue();
    stop = watchChanges({cursor: async () => '', stream: (_, signal) => feed = channel(signal), apply: async () => {}, refresh, state, isOnline: () => true, random: () => .5});
    await tick(); expect(state).toHaveBeenLastCalledWith('reconnecting');
    feed.push(event(ChangeKind.RESET, 'initial')); await tick(); await vi.advanceTimersByTimeAsync(100);
    expect(state).toHaveBeenLastCalledWith('current');
    feed.push(event(ChangeKind.MESSAGE, 'final')); await tick(); await vi.advanceTimersByTimeAsync(100);
    expect(state).toHaveBeenLastCalledWith('syncing');
    await vi.advanceTimersByTimeAsync(499); expect(refresh).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); expect(refresh).toHaveBeenCalledTimes(3);
    expect(refresh).toHaveBeenLastCalledWith(expect.objectContaining({events: [], full: false, jobs: false}));
    expect(state).toHaveBeenLastCalledWith('current');
    feed.push(event(ChangeKind.HEARTBEAT, 'final')); await tick(); await vi.advanceTimersByTimeAsync(10000);
    expect(refresh).toHaveBeenCalledTimes(3);
  });
  it('never overlaps refreshes or loses changes arriving during an in-flight snapshot', async () => {
    let feed!: ReturnType<typeof channel>;
    let finish!: () => void;
    const state = vi.fn();
    const refresh = vi.fn<() => Promise<void>>().mockImplementationOnce(() => new Promise(resolve => {finish = resolve;})).mockResolvedValue();
    stop = watchChanges({cursor: async () => '', stream: (_, signal) => feed = channel(signal), apply: async () => {}, refresh, state, isOnline: () => true});
    await tick(); feed.push(event(ChangeKind.RESET, 'initial')); await tick(); await vi.advanceTimersByTimeAsync(100);
    feed.push(event(ChangeKind.MESSAGE, 'new')); await tick(); await vi.advanceTimersByTimeAsync(1000);
    expect(refresh).toHaveBeenCalledTimes(1); expect(state).toHaveBeenLastCalledWith('syncing');
    finish(); await tick();
    expect(state).toHaveBeenLastCalledWith('syncing');
    await vi.advanceTimersByTimeAsync(100);
    expect(refresh).toHaveBeenLastCalledWith(expect.objectContaining({events: [event(ChangeKind.MESSAGE, 'new')], full: false}));
    expect(state).toHaveBeenLastCalledWith('current');
  });
  it('pauses dirty retries offline and requires a successful ready-stream snapshot before trusting reconnect', async () => {
    let online = true;
    let feed!: ReturnType<typeof channel>;
    let finish!: () => void;
    const state = vi.fn();
    const refresh = vi.fn<() => Promise<void>>().mockRejectedValueOnce(new Error('synthetic failure')).mockImplementationOnce(() => new Promise(resolve => {finish = resolve;})).mockResolvedValue();
    stop = watchChanges({cursor: async () => 'cursor', stream: (_, signal) => feed = channel(signal), apply: async () => {}, refresh, state, isOnline: () => online, random: () => .5});
    await tick(); feed.push(event(ChangeKind.RESET, 'cursor')); await tick(); await vi.advanceTimersByTimeAsync(100);
    expect(state).toHaveBeenLastCalledWith('syncing');
    online = false; window.dispatchEvent(new Event('offline')); await tick();
    expect(state).toHaveBeenLastCalledWith('offline');
    await vi.advanceTimersByTimeAsync(60000); expect(refresh).toHaveBeenCalledTimes(1);
    online = true; window.dispatchEvent(new Event('online')); await tick();
    expect(state).toHaveBeenLastCalledWith('reconnecting');
    await vi.advanceTimersByTimeAsync(1000); expect(refresh).toHaveBeenCalledTimes(1);
    feed.push(event(ChangeKind.HEARTBEAT, 'cursor')); await tick(); await vi.advanceTimersByTimeAsync(100);
    expect(state).toHaveBeenLastCalledWith('syncing');
    finish(); await tick(); expect(state).toHaveBeenLastCalledWith('current');
    stop.stop(); await vi.advanceTimersByTimeAsync(60000); expect(vi.getTimerCount()).toBe(0);
  });
  it('backs off repeated dirty failures and aborts an owned refresh with no timers left on unmount', async () => {
    let feed!: ReturnType<typeof channel>;
    let active!: AbortSignal;
    const refresh = vi.fn(async ({signal}: {signal: AbortSignal}) => {
      active = signal;
      if (refresh.mock.calls.length < 4) throw new Error('synthetic unavailable');
      await new Promise<void>((_, reject) => signal.addEventListener('abort', () => reject(new Error('synthetic abort')), {once: true}));
    });
    stop = watchChanges({cursor: async () => '', stream: (_, signal) => feed = channel(signal), apply: async () => {}, refresh, isOnline: () => true, random: () => .5});
    await tick(); feed.push(event(ChangeKind.RESET, 'last')); await tick(); await vi.advanceTimersByTimeAsync(100);
    expect(refresh).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(500); expect(refresh).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(999); expect(refresh).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); expect(refresh).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(1999); expect(refresh).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(1); expect(refresh).toHaveBeenCalledTimes(4);
    expect(active.aborted).toBe(false);
    stop.stop(); await tick(); expect(active.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(60000); expect(refresh).toHaveBeenCalledTimes(4); expect(vi.getTimerCount()).toBe(0);
  });
});
