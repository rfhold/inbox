import { beforeAll, describe, expect, it, vi } from 'vitest';
import 'fake-indexeddb/auto';
import { cached, clearMail, confirmDraft, drafts, jobs, newDraft, createStorage } from './storage';
beforeAll(() => {
  vi.stubGlobal('location', {origin: 'http://localhost:8080'});
  vi.stubGlobal('navigator', {onLine: true});
});
describe('durable local state', () => {
  it('persists dismissals per account without removing delivery jobs or drafts', async () => {
    const first = createStorage(() => 'notifications-first');
    const second = createStorage(() => 'notifications-second');
    const job = {id: 'job', status: 1};
    await first.jobs.save(job);
    await first.notifications.dismiss(['job-pending']);
    expect(await first.notifications.dismissed()).toEqual(['job-pending']);
    expect(await second.notifications.dismissed()).toEqual([]);
    expect(await first.jobs.all()).toContainEqual(job);
    await first.clearMail();
    expect(await first.notifications.dismissed()).toEqual(['job-pending']);
    await first.notifications.dismiss(Array.from({length: 110}, (_, i) => String(i)));
    expect(await first.notifications.dismissed()).toHaveLength(100);
    first.close(); second.close();
  });
  it('keeps draft identity and uncertain submission key across reads', async () => {
    const draft = {...newDraft('sender'), subject: 'synthetic draft', locked: true};
    await drafts.save(draft);
    expect((await drafts.all()).find(d => d.id === draft.id)).toEqual(draft);
    expect(draft.id).not.toBe(draft.key);
    await clearMail();
    expect((await drafts.all()).find(d => d.id === draft.id)?.key).toBe(draft.key);
    await confirmDraft(draft, {id: 'synthetic-job', status: 1});
    expect((await drafts.all()).some(d => d.id === draft.id)).toBe(false);
    expect(await jobs.all()).toContainEqual({id: 'synthetic-job', status: 1});
  });
  it('reads only explicitly cached views offline, including bigint protobuf values', async () => {
    const fetcher = vi.fn(async () => ({textBody: 'Synthetic cached content', rawSizeBytes: 12n}));
    await cached('test-detail', fetcher);
    vi.stubGlobal('navigator', {onLine: false});
    expect(await cached('test-detail', fetcher)).toEqual({textBody: 'Synthetic cached content', rawSizeBytes: 12n});
    expect(fetcher).toHaveBeenCalledTimes(1);
    await expect(cached('never-loaded', fetcher)).rejects.toThrow('Not cached');
    await clearMail();
    await expect(cached('test-detail', fetcher)).rejects.toThrow('Not cached');
    vi.stubGlobal('navigator', {onLine: true});
  });
  it('does not conceal an application rejection behind stale data', async () => {
    await cached('test-rejection', async () => 'old');
    await expect(cached('test-rejection', async () => {throw new Error('invalid argument');})).rejects.toThrow('invalid argument');
  });
  it('evicts old mail records without evicting local drafts', async () => {
    await clearMail();
    const draft = newDraft('synthetic-sender');
    await drafts.save(draft);
    const clock = vi.spyOn(Date, 'now');
    for (let i = 0; i < 251; i++) {
      clock.mockReturnValue(1000 + i);
      await cached(`bounded:${i}`, async () => ({index: i}));
    }
    clock.mockRestore();
    vi.stubGlobal('navigator', {onLine: false});
    await expect(cached('bounded:0', async () => null)).rejects.toThrow('Not cached');
    expect(await cached('bounded:250', async () => null)).toEqual({index: 250});
    expect((await drafts.all()).some(value => value.id === draft.id)).toBe(true);
    vi.stubGlobal('navigator', {onLine: true});
  });
});
