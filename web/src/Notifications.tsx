import { createEffect, createSignal, For, onCleanup, onMount, Show } from 'solid-js';
import type { Job, createStorage } from './storage';

export const deliveryKey = (job: Job) => JSON.stringify([job.id, job.status, job.recipients ?? []]);
const statusName = (status: number) => ['Delivery status unknown', 'Delivery pending', 'Message delivered', 'Delivery partially failed', 'Delivery failed'][status] ?? 'Delivery status unknown';
export function Notifications(props: {jobs: Job[]; loaded: boolean; store: ReturnType<typeof createStorage>['notifications']; fail: (error: unknown) => void}) {
  const [dismissed, setDismissed] = createSignal<string[]>([]);
  const [ready, setReady] = createSignal(false);
  const [toasts, setToasts] = createSignal<Job[]>([]);
  const seen = new Map<string, string>();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  let initialized = false;
  let writes = Promise.resolve();
  onMount(() => {void props.store.dismissed().then(keys => {setDismissed(keys); setReady(true);}).catch(props.fail);});
  onCleanup(() => {for (const timer of timers) clearTimeout(timer);});
  createEffect(() => {
    if (!ready() || !props.loaded) return;
    const jobs = props.jobs;
    for (const job of jobs) {
      const key = deliveryKey(job);
      if (initialized && seen.get(job.id) !== key && !dismissed().includes(key)) {
        setToasts(previous => [...previous.filter(value => value.id !== job.id), job].slice(-3));
        if (job.status !== 3 && job.status !== 4) {
          const timer = setTimeout(() => {setToasts(values => values.filter(value => deliveryKey(value) !== key)); timers.delete(timer);}, 8000);
          timers.add(timer);
        }
      }
      seen.set(job.id, key);
    }
    initialized = true;
  });
  const visible = () => ready() ? props.jobs.slice(-100).filter(job => !dismissed().includes(deliveryKey(job))).reverse() : [];
  const dismiss = (job: Job) => {
    writes = writes.then(async () => {
      const keys = [...dismissed().filter(key => key !== deliveryKey(job)), deliveryKey(job)].slice(-100);
      await props.store.dismiss(keys);
      setDismissed(keys); setToasts(values => values.filter(value => deliveryKey(value) !== deliveryKey(job)));
    }).catch(props.fail);
  };
  const description = (job: Job) => <><strong>{statusName(job.status)}</strong><For each={job.recipients}>{recipient =>
    <p>{recipient.address}: {['Unknown', 'Pending', 'Delivered', 'Failed'][recipient.status] ?? 'Unknown'}<Show when={recipient.errorCode}> · {recipient.errorCode}</Show></p>
  }</For></>;
  return <>
    <details class="notifications"><summary>Notifications <span class="count">{visible().length || ''}</span></summary><section aria-label="Notifications">
      <Show when={visible().length} fallback={<p>No notifications.</p>}><For each={visible()}>{job => <article>{description(job)}<button onClick={() => dismiss(job)}>Dismiss notification</button></article>}</For></Show>
    </section></details>
    <div class="toast-stack" aria-live="polite" aria-relevant="additions"><For each={toasts()}>{job => <div class="toast" role="status">{description(job)}
      <button aria-label="Dismiss delivery toast" onClick={() => setToasts(values => values.filter(value => value.id !== job.id))}>Dismiss</button>
    </div>}</For></div>
  </>;
}
