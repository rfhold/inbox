import type { QueryClient } from '@tanstack/solid-query';
import { ChangeKind, SubmissionStatus } from './gen/inbox/v1/inbox_pb';
import { affectedQuery, type RefreshBatch } from './changes';
import { jobs, type Job } from './storage';

export function mailReconciler(qc: QueryClient, readJob: (id: string, signal: AbortSignal) => Promise<Job>, showJobs: () => Promise<unknown>, jobStore = jobs) {
  const dirtyQueries = new Map<string, readonly unknown[]>();
  const dirtyJobs = new Set<string>();
  const wantedJobs = new Set<string>();
  let pendingJobs = false;
  let allJobs = false;
  let jobsViewDirty = false;
  return async ({events, full, jobs: localJobs, signal}: RefreshBatch) => {
    const predicate = (query: {queryKey: readonly unknown[]}) => full || events.some(event => affectedQuery(query.queryKey, event));
    for (const query of qc.getQueryCache().findAll({predicate, type: 'active'})) dirtyQueries.set(query.queryHash, query.queryKey);
    pendingJobs ||= full || localJobs;
    for (const event of events) if (event.kind === ChangeKind.SUBMISSION) {
      if (event.submissionId) wantedJobs.add(event.submissionId);
      else pendingJobs = true;
    }
    if (wantedJobs.size > 64) {allJobs = true; wantedJobs.clear();}
    if (signal.aborted) throw new Error('Reconciliation interrupted');
    await qc.invalidateQueries({predicate, refetchType: 'none'});
    let failed = false;
    if (pendingJobs || allJobs || wantedJobs.size) {
      try {
        for (const job of await jobStore.all()) if (allJobs || wantedJobs.has(job.id) || (pendingJobs && job.status === SubmissionStatus.PENDING)) dirtyJobs.add(job.id);
        pendingJobs = false; allJobs = false; wantedJobs.clear(); jobsViewDirty = true;
      } catch {failed = true;}
    }
    const queries = [...dirtyQueries];
    const cancel = () => {void qc.cancelQueries({predicate: query => queries.some(([hash]) => query.queryHash === hash)});};
    signal.addEventListener('abort', cancel, {once: true});
    if (signal.aborted) cancel();
    try {
      await Promise.all([
        ...queries.map(async ([hash, key]) => {
          try {
            const query = qc.getQueryCache().get(hash);
            if (query?.isActive()) {
              if (signal.aborted) throw new Error('View refresh interrupted');
              await qc.refetchQueries({queryKey: key, exact: true, type: 'active'}, {throwOnError: true});
              if (signal.aborted || qc.getQueryState(key)?.status === 'error') throw new Error('View refresh incomplete');
            }
            dirtyQueries.delete(hash);
          } catch {failed = true;}
        }),
        (async () => {
          for (const id of [...dirtyJobs]) {
            if (signal.aborted) {failed = true; break;}
            try {
              const job = await readJob(id, signal);
              if (signal.aborted) throw new Error('Job refresh interrupted');
              await jobStore.save(job);
              dirtyJobs.delete(id); jobsViewDirty = true;
            } catch {failed = true;}
          }
          if (jobsViewDirty && !signal.aborted) {
            try {await showJobs(); jobsViewDirty = false;} catch {failed = true;}
          }
        })(),
      ]);
    } finally {signal.removeEventListener('abort', cancel);}
    if (failed || signal.aborted) throw new Error('Mail reconciliation incomplete');
  };
}
