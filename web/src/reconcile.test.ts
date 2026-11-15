import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryClient, QueryObserver } from '@tanstack/solid-query';
import { ChangeKind } from './gen/inbox/v1/inbox_pb';
import { mailReconciler } from './reconcile';
import { jobs } from './storage';
vi.mock('./storage', () => ({jobs: {all: vi.fn(), save: vi.fn()}}));

describe('unfinished reconciliation work', () => {
  let qc: QueryClient;
  const unsubscribers: (() => void)[] = [];
  const batch = (full = false) => ({events: [], full, jobs: false, signal: new AbortController().signal});
  function observe(key: readonly unknown[], queryFn: () => Promise<string>) {
    const observer = new QueryObserver(qc, {queryKey: key, queryFn, initialData: 'cached', staleTime: Infinity});
    unsubscribers.push(observer.subscribe(() => {}));
  }
  beforeEach(() => {
    qc = new QueryClient({defaultOptions: {queries: {retry: false, gcTime: Infinity}}});
    vi.mocked(jobs.all).mockReset().mockResolvedValue([]);
    vi.mocked(jobs.save).mockReset().mockResolvedValue('saved');
  });
  afterEach(() => {unsubscribers.splice(0).forEach(unsubscribe => unsubscribe()); qc.clear();});
  it('retries only failed views and jobs after the final notification, without redoing successful work', async () => {
    const boxes = vi.fn(async () => 'fresh boxes');
    const list = vi.fn<() => Promise<string>>().mockRejectedValueOnce(new Error('synthetic unavailable')).mockResolvedValue('fresh list');
    observe(['boxes'], boxes); observe(['messages', 'box', 1], list);
    vi.mocked(jobs.all).mockResolvedValue([{id: 'failed-job', status: 1}, {id: 'successful-job', status: 1}]);
    let failedOnce = false;
    const readJob = vi.fn(async (id: string) => {
      if (id === 'failed-job' && !failedOnce) {failedOnce = true; throw new Error('synthetic unavailable');}
      return {id, status: 2};
    });
    const refresh = mailReconciler(qc, readJob, vi.fn(async () => {}));
    await expect(refresh(batch(true))).rejects.toThrow('Mail reconciliation incomplete');
    expect(qc.getQueryState(['messages', 'box', 1])?.status).toBe('error');
    await refresh(batch());
    expect(qc.getQueryData(['messages', 'box', 1])).toBe('fresh list');
    expect(list).toHaveBeenCalledTimes(2); expect(boxes).toHaveBeenCalledTimes(1);
    expect(readJob.mock.calls.filter(([id]) => id === 'failed-job')).toHaveLength(2);
    expect(readJob.mock.calls.filter(([id]) => id === 'successful-job')).toHaveLength(1);
    expect(jobs.all).toHaveBeenCalledTimes(1);
    await refresh(batch());
    expect(list).toHaveBeenCalledTimes(2); expect(readJob).toHaveBeenCalledTimes(3);
  });
  it('retains failed local job discovery and local view updates without refetching an already-saved job', async () => {
    vi.mocked(jobs.all).mockRejectedValueOnce(new Error('synthetic storage unavailable')).mockResolvedValue([{id: 'job', status: 1}]);
    const readJob = vi.fn(async () => ({id: 'job', status: 2}));
    const showJobs = vi.fn<() => Promise<void>>().mockRejectedValueOnce(new Error('synthetic view unavailable')).mockResolvedValue();
    const refresh = mailReconciler(qc, readJob, showJobs);
    await expect(refresh({...batch(), jobs: true})).rejects.toThrow('Mail reconciliation incomplete');
    await expect(refresh(batch())).rejects.toThrow('Mail reconciliation incomplete');
    await refresh(batch());
    expect(jobs.all).toHaveBeenCalledTimes(2); expect(readJob).toHaveBeenCalledTimes(1); expect(showJobs).toHaveBeenCalledTimes(2);
  });
  it('honors affected query scope and retries nothing unrelated', async () => {
    const unrelated = vi.fn(async () => 'unrelated');
    const affected = vi.fn<() => Promise<string>>().mockRejectedValueOnce(new Error('synthetic')).mockResolvedValue('fresh');
    observe(['boxes'], unrelated); observe(['messages', 'box', 2], affected);
    const readJob = vi.fn(async () => ({id: 'unused', status: 2}));
    const refresh = mailReconciler(qc, readJob, async () => {});
    await expect(refresh({...batch(), events: [{kind: ChangeKind.MESSAGE, resumeToken: 'final', mailboxId: 'box', entryId: '', submissionId: ''}]})).rejects.toThrow('Mail reconciliation incomplete');
    await refresh(batch());
    expect(unrelated).not.toHaveBeenCalled(); expect(readJob).not.toHaveBeenCalled(); expect(affected).toHaveBeenCalledTimes(2);
  });
  it('refreshes domain selectors and mailbox policy state without polling healthy mail or jobs', async () => {
    const domains = vi.fn(async () => 'fresh domains');
    const boxes = vi.fn(async () => 'fresh boxes');
    const mail = vi.fn(async () => 'unchanged mail');
    observe(['domains'], domains); observe(['boxes'], boxes); observe(['messages', 'box', 1], mail);
    const readJob = vi.fn(async () => ({id: 'unused', status: 2}));
    const refresh = mailReconciler(qc, readJob, async () => {});
    await refresh({...batch(), events: [{kind: ChangeKind.DOMAIN, domainId: 'domain', resumeToken: 'domain-change', mailboxId: '', entryId: '', submissionId: ''}]});
    await refresh(batch());
    expect(domains).toHaveBeenCalledTimes(1); expect(boxes).toHaveBeenCalledTimes(1);
    expect(mail).not.toHaveBeenCalled(); expect(readJob).not.toHaveBeenCalled();
  });
});
