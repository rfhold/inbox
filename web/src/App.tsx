import { createSignal, createEffect, onCleanup, onMount, For, Show } from 'solid-js';
import { createInfiniteQuery, createQuery, useQueryClient } from '@tanstack/solid-query';
import { accountApi, Folder, submissionPayload } from './api';
import { createStorage, type Draft, type Job } from './storage';
import type { Account } from './session';
import { watchChanges, affectedQuery, type SyncState, type WatchControl } from './changes';
import { mailReconciler } from './reconcile';
import { safeEvent } from './telemetry';
import { Icon } from './Icon';
import { Domains } from './Domains';
import { commandIntent, definitiveRejection, sendRejected, type Intent } from './intents';
import { useLocation, useNavigate, useBeforeLeave } from '@solidjs/router';
import { mailPath, readRoute, type MailFolder } from './routes';
import { Notifications } from './Notifications';

const localJob = (submission: {id: string; status: number; recipients: {address: string; status: number; errorCode: string}[]}): Job => ({id: submission.id, status: submission.status, recipients: submission.recipients.map(({address, status, errorCode}) => ({address, status, errorCode}))});
const initials = (address: string) => address.slice(0, 1).toUpperCase() || '?';
const displaySender = (address: string) => address.split('@')[0] || address;
const friendlyDate = (value: string, full = false) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Unknown date';
  if (full) return date.toLocaleString(undefined, {month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit'});
  return date.toDateString() === new Date().toDateString() ? date.toLocaleTimeString(undefined, {hour: 'numeric', minute: '2-digit'}) : date.toLocaleDateString(undefined, {month: 'short', day: 'numeric'});
};
export default function App(props: {account: Account; logout: () => void}) {
  const storage = createStorage(() => props.account.namespace, props.account.assert);
  onCleanup(storage.close);
  const {drafts, newDraft, clearMail, confirmDraft, jobs, commands, invalidateMail, resumeToken} = storage;
  const {client, changeStream, mailboxes, allDomains, messages, message} = accountApi(props.account, storage);
  const qc = useQueryClient();
  const [online, setOnline] = createSignal(navigator.onLine);
  const [sync, setSync] = createSignal<SyncState>(navigator.onLine ? 'reconnecting' : 'offline');
  const location = useLocation();
  const routerNavigate = useNavigate();
  let internalNavigation = false;
  const navigate = (to: string, options?: {replace?: boolean}) => {
    internalNavigation = true;
    try {routerNavigate(to, options);} finally {internalNavigation = false;}
  };
  const route = () => readRoute(location.pathname);
  const mailRoute = () => {const value = route(); return value.page === 'mail' ? value : undefined;};
  const [lastAddress, setLastAddress] = createSignal('');
  const navigationAddress = () => mailRoute()?.address || lastAddress() || allBoxes()[0]?.address;
  createEffect(() => {const selected = mailRoute(); if (selected) setLastAddress(selected.address);});
  const box = () => allBoxes().find(value => value.address === mailRoute()?.address)?.id ?? '';
  const folder = () => mailRoute()?.folder === 'sent' ? Folder.SENT : Folder.INBOX;
  const entry = () => mailRoute()?.folder === 'drafts' ? '' : mailRoute()?.entry ?? '';
  const setEntry = (id: string) => {const selected = mailRoute(); if (selected) navigate(mailPath(selected.address, selected.folder, id || undefined));};
  const setBox = (id: string) => {const selected = allBoxes().find(value => value.id === id); if (selected) navigate(mailPath(selected.address));};
  const [draft, setDraft] = createSignal<Draft>();
  const [savedDrafts, setSavedDrafts] = createSignal<Draft[]>([]);
  const [savedJobs, setSavedJobs] = createSignal<Job[]>([]);
  const [jobsReady, setJobsReady] = createSignal(false);
  const [saved, setSaved] = createSignal(true);
  const [notice, setNotice] = createSignal('');
  const [busy, setBusy] = createSignal(false);
  const [creating, setCreating] = createSignal(false);
  const [localPart, setLocalPart] = createSignal('');
  const [name, setName] = createSignal('');
  const [domainId, setDomainId] = createSignal('');
  const [createIntent, setCreateIntent] = createSignal<Intent<{localPart: string; name: string; domainId: string}>>();
  const [createUnconfirmed, setCreateUnconfirmed] = createSignal(false);
  const [commandsReady, setCommandsReady] = createSignal(false);
  const showDrafts = () => mailRoute()?.folder === 'drafts';
  const setShowDrafts = (value: boolean) => {const address = navigationAddress(); if (address && value !== showDrafts()) navigate(mailPath(address, value ? 'drafts' : 'inbox'));};
  const [pane, setPane] = createSignal<'list' | 'detail'>('list');
  let timer: ReturnType<typeof setTimeout> | undefined;
  let writeQueue = Promise.resolve();
  let watch: WatchControl | undefined;
  let lastCreateIntent: NonNullable<ReturnType<typeof createIntent>> | undefined;
  let readIntent: {mailboxId: string; entryId: string; isRead: boolean; requestId: string} | undefined;
  const persist = (value: Draft) => {
    setSaved(false);
    writeQueue = writeQueue.then(async () => {await drafts.save(value); await loadDrafts(); if (JSON.stringify(draft()) === JSON.stringify(value)) setSaved(true);}).catch(() => { setNotice('Draft could not be saved. Keep this window open.'); safeEvent('application_error'); });
    return writeQueue;
  };
  const flush = () => { clearTimeout(timer); const value = draft(); return value ? persist(value) : writeQueue; };
  const loadDrafts = async () => setSavedDrafts(await drafts.all());
  const loadJobs = async () => {setSavedJobs(await jobs.all()); setJobsReady(true);};
  const fail = (_error: unknown) => {if (props.account.signal.aborted) return; setNotice('Request could not be completed. Reconnect or retry safely.'); safeEvent('application_error');};
  const boxes = createInfiniteQuery(() => ({queryKey: ['boxes'], initialPageParam: '', queryFn: ({pageParam, signal}) => mailboxes(pageParam, signal), getNextPageParam: last => last.nextPageToken || undefined}));
  const domainList = createQuery(() => ({queryKey: ['domains'], queryFn: ({signal}) => allDomains(signal)}));
  createEffect(() => {if (!domainId() && domainList.data?.length) setDomainId(domainList.data.at(-1)!.id);});
  const allBoxes = () => boxes.data?.pages.flatMap(p => p.mailboxes) ?? [];
  createEffect(() => {
    if (route().page === 'home' && allBoxes().length) navigate(mailPath(allBoxes()[0].address), {replace: true});
    if (mailRoute() && !box() && boxes.hasNextPage && !boxes.isFetchingNextPage) void boxes.fetchNextPage();
  });
  useBeforeLeave(event => {
    if (busy() && !internalNavigation) {event.preventDefault(); return;}
    const value = draft();
    if (value && !saved()) {
      event.preventDefault();
      void flush().then(() => {if (saved()) event.retry(true);});
    }
  });
  createEffect(() => {
    const selected = mailRoute();
    if (selected?.folder === 'drafts' && selected.entry) {
      const value = savedDrafts().find(value => value.id === selected.entry && value.from === box());
      if (value && draft()?.id !== value.id) {setDraft(value); setSaved(true); setPane('detail');}
    } else {
      setDraft(undefined);
      setPane(selected?.entry ? 'detail' : 'list');
    }
  });
  const list = createInfiniteQuery(() => ({queryKey: ['messages', box(), folder()], enabled: !!box(), initialPageParam: '', queryFn: ({pageParam, signal}) => messages(box(), folder(), pageParam, signal), getNextPageParam: last => last.nextPageToken || undefined}));
  const detail = createQuery(() => ({queryKey: ['detail', box(), entry()], enabled: !!box() && !!entry(), queryFn: ({signal}) => message(box(), entry(), signal)}));
  const activeBox = () => allBoxes().find(b => b.id === box());
  const senderDisabled = (id: string) => {
    const mailbox = allBoxes().find(b => b.id === id);
    return domainList.data?.find(domain => domain.id === mailbox?.domainId)?.sendEnabled === false;
  };
  const refreshDomains = () => qc.invalidateQueries({predicate: query => query.queryKey[0] === 'domains' || query.queryKey[0] === 'boxes'});
  const refresh = () => qc.invalidateQueries();
  onMount(() => {
    void commands.get<NonNullable<ReturnType<typeof createIntent>>>('createMailbox').then(intent => {
      if (intent) {setCreateIntent(intent); setCreateUnconfirmed(true); setLocalPart(intent.localPart); setName(intent.name); setDomainId(intent.domainId); setCreating(true);}
      setCommandsReady(true);
    }).catch(fail);
    void loadDrafts().catch(fail); void loadJobs().catch(fail);
    watch = watchChanges({
      stream: changeStream,
      cursor: resumeToken,
      state: setSync,
      apply: async event => {
        const predicate = (query: {queryKey: readonly unknown[]}) => affectedQuery(query.queryKey, event);
        await qc.cancelQueries({predicate});
        await invalidateMail(event);
        await qc.invalidateQueries({predicate, refetchType: 'none'});
      },
      refresh: mailReconciler(qc, async (id, signal) => {
        const result = await client.getSubmission({submissionId: id}, {signal});
        if (!result.submission) throw new Error('Submission refresh incomplete');
        return localJob(result.submission);
      }, loadJobs, jobs),
    });
    const connected = () => {setOnline(navigator.onLine);};
    const hidden = () => {if (document.hidden) void flush();};
    window.addEventListener('online', connected); window.addEventListener('offline', connected);
    document.addEventListener('visibilitychange', hidden);
    onCleanup(() => {watch?.stop(); clearTimeout(timer); window.removeEventListener('online', connected); window.removeEventListener('offline', connected); document.removeEventListener('visibilitychange', hidden);});
  });
  async function openDraft(value: Draft) {
    if (busy()) return;
    await flush();
    if (!saved()) return;
    const stored = (await drafts.all()).find(saved => saved.id === value.id);
    const current = draft();
    const selected = current?.id === value.id ? current : stored;
    if (!selected) return;
    const mailbox = allBoxes().find(box => box.id === selected.from);
    if (!mailbox) {setNotice('Load the sender mailbox before opening this draft.'); return;}
    setDraft(selected); setSaved(JSON.stringify(selected) === JSON.stringify(stored)); navigate(mailPath(mailbox.address, 'drafts', selected.id)); setPane('detail');
  }
  function edit(field: keyof Draft, value: string) {
    const current = draft(); if (!current || current.locked) return;
    const changed = {...current, [field]: value}; setDraft(changed); setSaved(false);
    clearTimeout(timer); timer = setTimeout(() => void persist(changed), 250);
  }
  async function compose() {if (!box() || busy()) return; const value = newDraft(box()); await drafts.save(value); await loadDrafts(); await openDraft(value);}
  async function reply() {
    const msg = detail.data?.message; if (!msg?.entry) return;
    const value = {...newDraft(box()), to: msg.replyToAddress || msg.entry.fromAddress, subject: /^re:/i.test(msg.entry.subject) ? msg.entry.subject : `Re: ${msg.entry.subject}`, replyEntry: msg.entry.id, sourceMailbox: box()};
    await drafts.save(value); await loadDrafts(); await openDraft(value);
  }
  async function send(event: SubmitEvent) {
    event.preventDefault(); if (!online() || busy()) return;
    const value = draft(); if (!value) return;
    if (!value.locked && senderDisabled(value.from)) return;
    const locked = {...value, locked: true, rejected: false};
    setBusy(true); setNotice('');
    try {
      clearTimeout(timer); await flush();
      // Persist the immutable payload and key before any network side effect.
      await drafts.save(locked); setDraft(locked); await loadDrafts();
      const result = await client.submitMessage(submissionPayload(locked), {timeoutMs: 30000});
      if (!result.submission) throw new Error('No send confirmation received. Retry safely with the same key.');
      await confirmDraft(locked, localJob(result.submission));
      watch?.refreshJobs();
      setDraft(undefined); setShowDrafts(false); setPane('list'); await loadDrafts(); await loadJobs(); void refresh();
      setNotice('Message accepted. Delivery updates appear in notifications.'); safeEvent('message_submitted');
    } catch (error) {
      if (sendRejected(error)) {
        const rejected = {...locked, rejected: true};
        await drafts.save(rejected); setDraft(rejected); await loadDrafts();
      }
      fail(error);
    } finally {setBusy(false);}
  }
  async function createBox(event: SubmitEvent) {
    event.preventDefault(); if (!online() || busy() || !commandsReady()) return;
    setBusy(true);
    const request = createIntent() ?? commandIntent(lastCreateIntent, {localPart: localPart(), name: name(), domainId: domainId()});
    lastCreateIntent = request;
    setCreateIntent(request);
    try {
      await commands.save('createMailbox', request);
      const result = await client.createMailbox(request);
      if (!result.mailbox) throw new Error('Mailbox creation unconfirmed. Retry the same request.');
      await commands.remove('createMailbox', request.requestId); setCreateIntent(undefined); setCreateUnconfirmed(false); lastCreateIntent = undefined;
      await qc.invalidateQueries({queryKey: ['boxes']}); navigate(mailPath(result.mailbox.address)); setCreating(false); setLocalPart(''); setName('');
    } catch(error) {
      if (definitiveRejection(error)) {await commands.remove('createMailbox', request.requestId); setCreateIntent(undefined); setCreateUnconfirmed(false);}
      else setCreateUnconfirmed(true);
      fail(error);
    }
    finally {setBusy(false);}
  }
  async function mark(isRead: boolean) {
    if (readIntent?.mailboxId !== box() || readIntent?.entryId !== entry() || readIntent?.isRead !== isRead) readIntent = {mailboxId: box(), entryId: entry(), isRead, requestId: crypto.randomUUID()};
    const request = readIntent;
    try {await client.setReadState(request); if (readIntent === request) readIntent = undefined; await refresh();} catch(error) {fail(error);}
  }
  async function download() {
    try {const result = await client.getRawMessage({mailboxId: box(), entryId: entry()}, {timeoutMs: 30000}); const url = URL.createObjectURL(new Blob([new Uint8Array(result.rawMessage)], {type: 'message/rfc822'})); const link = document.createElement('a'); link.href = url; link.download = 'message.eml'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);} catch(error) {fail(error);}
  }
   async function selectBox(id: string) {if (busy()) return; await flush(); if (!saved()) return; setBox(id);}
   async function selectFolder(next?: Folder) {if (busy()) return; await flush(); if (!saved()) return; const address = navigationAddress(); if (address) navigate(mailPath(address, next === undefined ? 'drafts' : next === Folder.SENT ? 'sent' : 'inbox'));}
  return <div class="app">
    <header class="top"><div class="brand"><span class="brand-mark"><Icon name="mail" /></span><h1>Inbox</h1><span class="workspace-label">{props.account.identity.mode === 'disabled' ? 'Shared workspace' : 'Personal workspace'}</span></div><div class="top-actions"><span class="connection" role="note" aria-live="polite" aria-label="Mail synchronization" classList={{offline: !online() || sync() !== 'current'}}><span class="connection-dot" /><span>{!online() ? 'Offline · cached mail may be stale' : sync() === 'current' ? 'Up to date' : sync() === 'syncing' ? 'Syncing · cached mail may be stale' : 'Reconnecting · cached mail may be stale'}</span></span><button class="refresh" onClick={() => void refresh()}><Icon name="refresh" /><span>Refresh</span></button><Show when={props.account.identity.mode === 'oidc'}><button onClick={props.logout}>Sign out</button></Show></div></header>
    <Show when={notice()}><div role="status" class="notice toast">{notice()}<button aria-label="Dismiss notification" onClick={() => setNotice('')}>Dismiss</button></div></Show>
    <div class="workspace" classList={{'show-detail': pane() === 'detail'}}>
      <aside class="sidebar" aria-label="Mail navigation">
        <div class="account"><div class="section-head"><label for="mailbox-selector">Mailbox</label><button class="icon-button" aria-label="Create mailbox" aria-expanded={creating()} onClick={() => setCreating(!creating())}><Icon name="plus" /></button></div>
          <select id="mailbox-selector" value={box()} title={activeBox()?.address} disabled={busy()} onChange={e => void selectBox(e.currentTarget.value)}><Show when={!allBoxes().length}><option value="">Select a mailbox</option></Show><For each={allBoxes()}>{mailbox => <option value={mailbox.id} data-address={mailbox.address}>{mailbox.name || mailbox.localPart} · {mailbox.address}</option>}</For></select>
          <Show when={boxes.hasNextPage}><button class="text-button" disabled={boxes.isFetchingNextPage} onClick={() => void boxes.fetchNextPage()}>More mailboxes</button></Show>
        </div>
        <button class="primary compose" onClick={() => void compose()} disabled={!box() || busy()}><Icon name="compose" />Compose</button>
        <Show when={creating()}><form class="mailbox-form" onSubmit={createBox}><fieldset disabled={!commandsReady() || !!createIntent() || busy()}><label>Name<input required value={name()} onInput={e => setName(e.currentTarget.value)} /></label><label>Address local part<input required pattern="[a-zA-Z0-9._+-]+" value={localPart()} onInput={e => setLocalPart(e.currentTarget.value)} placeholder="alex" /></label><label>Mailbox domain<select required value={domainId()} onChange={e => setDomainId(e.currentTarget.value)}><Show when={!domainList.data?.some(domain => domain.id === domainId())}><option value={domainId()}>{domainId() ? 'Saved domain' : 'Select a domain'}</option></Show><For each={domainList.data}>{domain => <option value={domain.id}>{domain.name}</option>}</For></select></label></fieldset><Show when={createUnconfirmed()}><p class="warning">Creation unconfirmed. Retry the original mailbox and domain before changing it.</p></Show><button disabled={!online() || busy() || !commandsReady() || (!createIntent() && (!domainId() || domainList.isPending || !!domainList.error))}>{createUnconfirmed() ? 'Retry mailbox creation safely' : 'Create'}</button></form></Show>
        <Show when={domainList.error}><p role="alert">Domain directory unavailable. Connect and try again.</p></Show>
         <button aria-current={route().page === 'domains' ? 'page' : undefined} onClick={async () => {if (busy()) return; await flush(); if (saved()) navigate('/domains');}}>Domains</button>
        <Show when={boxes.error}><p role="alert">Mailboxes unavailable. Connect and try again.</p></Show>
        <Show when={!boxes.isPending && !allBoxes().length}><p class="muted">Create a mailbox to get started.</p></Show>
        <nav class="folders" aria-label="Folders"><button classList={{selected: !showDrafts() && folder() === Folder.INBOX}} aria-current={!showDrafts() && folder() === Folder.INBOX ? 'page' : undefined} onClick={() => void selectFolder(Folder.INBOX)}><Icon name="inbox" />Inbox</button><button classList={{selected: !showDrafts() && folder() === Folder.SENT}} aria-current={!showDrafts() && folder() === Folder.SENT ? 'page' : undefined} onClick={() => void selectFolder(Folder.SENT)}><Icon name="sent" />Sent</button><button classList={{selected: showDrafts()}} aria-current={showDrafts() ? 'page' : undefined} onClick={() => void selectFolder()}><Icon name="draft" />Drafts <span class="count">{savedDrafts().length}</span></button></nav>
         <div class="sidebar-bottom"><Notifications jobs={savedJobs()} loaded={jobsReady()} store={storage.notifications} fail={fail} />
          <details class="privacy"><summary>Device storage</summary><p>{props.account.identity.mode === 'disabled' ? 'Shared mail. No account required.' : 'Mail and drafts are isolated to your account.'}<br />Drafts stay on this device.</p><button onClick={async () => {await clearMail(); setEntry(''); await qc.resetQueries(); setNotice('Cached mail cleared. Local drafts preserved.');}}>Clear cached mail</button></details>
          <span class="shared-note">{props.account.identity.mode === 'disabled' ? 'Shared mail' : 'Personal mail'} · local drafts</span>
        </div>
      </aside>
      <Show when={route().page === 'domains'} fallback={<Show when={route().page !== 'missing'} fallback={<main class="welcome"><h2>Page not found</h2></main>}>
      <section class="message-list" aria-label="Message list"><div class="list-heading"><h2>{showDrafts() ? 'Drafts' : folder() === Folder.SENT ? 'Sent' : 'Inbox'}</h2><span>{showDrafts() ? 'Saved on this device' : activeBox()?.name || activeBox()?.address || 'Your messages'}</span></div><div class="list-scroll">
        <Show when={mailRoute() && !box() && !boxes.isPending && !boxes.hasNextPage}><p role="alert" class="empty">Mailbox unavailable for this account.</p></Show>
        <Show when={showDrafts()} fallback={<>
        <Show when={list.isPending && box()}><p class="empty">Loading messages…</p></Show><Show when={list.error}><p role="alert" class="empty">Messages unavailable. Connect to load this view.</p></Show>
         <For each={list.data?.pages.flatMap(p => p.entries)}>{item => <button class="message-row" classList={{selected: entry() === item.id, unread: !item.isRead}} onClick={async () => {if (busy()) return; await flush(); if (!saved()) return; setDraft(undefined); setEntry(item.id); setPane('detail');}}><span class="avatar">{initials(folder() === Folder.SENT ? item.to[0] || '' : item.fromAddress)}</span><span class="row-content"><span class="row-meta"><span class="sender">{folder() === Folder.SENT ? `To: ${item.to.map(displaySender).join(', ')}` : displaySender(item.fromAddress)}</span><time dateTime={item.createdAt}>{friendlyDate(item.createdAt)}</time></span><strong>{item.subject || '(No subject)'}</strong><span class="preview">{item.preview || 'No preview'}</span><small class="read-state">{item.isRead ? 'Read' : 'Unread'}</small></span></button>}</For>
        <Show when={box() && list.data?.pages[0].entries.length === 0}><p class="empty">Nothing here yet.</p></Show><Show when={list.hasNextPage}><button class="more" disabled={list.isFetchingNextPage} onClick={() => void list.fetchNextPage()}>Load more messages</button></Show>
        </>}><For each={savedDrafts()}>{value => <button class="draft-link" classList={{selected: draft()?.id === value.id}} onClick={() => void openDraft(value)}><span class="avatar draft-avatar"><Icon name="draft" /></span><span class="row-content"><span class="row-meta"><span class="sender">{value.to ? `To: ${value.to}` : 'No recipients'}</span><small>Draft</small></span><strong>{value.subject || 'Untitled draft'}</strong><span class="preview">{value.body || 'No message yet'}</span><small classList={{'draft-warning': !!value.locked}}>{value.rejected ? 'Rejected · correction available' : value.locked ? 'Unconfirmed · safe retry' : 'Saved on this device'}</small></span></button>}</For><Show when={!savedDrafts().length}><div class="empty"><Icon name="draft" /><p>No local drafts.</p><small>Your drafts are saved only on this device.</small></div></Show></Show>
        </div></section>
      <main class="detail" aria-label="Reading pane">
         <button class="mobile-back" onClick={async () => {await flush(); if (saved()) setEntry('');}}><Icon name="back" />Back to {showDrafts() ? 'drafts' : 'messages'}</button>
        <Show when={draft()} fallback={<Show when={entry()} fallback={<div class="welcome"><span class="welcome-icon"><Icon name="mail" /></span><h2>Select a message</h2><p>Choose a conversation to read, or compose a new message.</p><small>Plain-text mail.</small></div>}><Show when={detail.isPending}><p class="empty">Loading message…</p></Show><Show when={detail.error}><p role="alert" class="empty">Message unavailable. Connect to load this view.</p></Show><Show when={detail.data?.message}>{msg => <article>
          <div class="detail-actions"><div class="toolbar-group"><button onClick={() => void reply()}><Icon name="reply" />Reply</button></div><div class="toolbar-group"><button disabled={!online()} onClick={() => void mark(!msg().entry?.isRead)}><Icon name="read" />{msg().entry?.isRead ? 'Mark unread' : 'Mark read'}</button><button disabled={!online()} onClick={() => void download()}><Icon name="download" />Download EML</button></div></div>
          <div class="message-header"><h2>{msg().entry?.subject || '(No subject)'}</h2><div class="sender-line"><span class="avatar">{initials(msg().entry?.fromAddress || '')}</span><div><strong>{displaySender(msg().entry?.fromAddress || '')}</strong><span>{msg().entry?.fromAddress}</span></div><time dateTime={msg().entry?.sentAt || msg().entry?.createdAt}>{friendlyDate(msg().entry?.sentAt || msg().entry?.createdAt || '', true)}</time></div><dl><dt>To</dt><dd>{msg().entry?.to.join(', ')}</dd><Show when={msg().entry?.cc.length}><dt>Cc</dt><dd>{msg().entry?.cc.join(', ')}</dd></Show></dl></div><pre class="body">{msg().textBody || 'No plain-text content.'}</pre></article>}</Show></Show>}>
          {value => <form class="composer" onSubmit={send}>
             <div class="composer-title"><h2>{value().replyEntry ? 'Reply' : 'New message'}</h2><button type="button" disabled={busy()} onClick={async () => {await flush(); if (!saved()) return; setDraft(undefined); setEntry(''); setPane('list');}}>Close</button></div>
            <p class="muted">{saved() ? 'Saved on this device' : 'Saving draft…'}</p>
            <Show when={value().locked}><p class="warning">{value().rejected ? 'The server rejected the entire submission. Copy it to a new draft to correct the recipients or content.' : 'Submission unconfirmed. This draft is locked to its original payload. Retry is safe, including after a reload.'}</p></Show>
            <fieldset disabled={value().locked || busy()}>
              <label>From<select value={value().from} onChange={e => edit('from', e.currentTarget.value)}>
                <Show when={!allBoxes().some(mailbox => mailbox.id === value().from)}><option value={value().from} selected>Saved sender (load mailboxes for address)</option></Show>
                <For each={allBoxes()}>{mailbox => <option value={mailbox.id} selected={mailbox.id === value().from}>{mailbox.address}</option>}</For>
              </select></label>
              <label>To<input required value={value().to} onInput={e => edit('to', e.currentTarget.value)} placeholder="alex@inbox.test" /></label>
              <label>Cc<input value={value().cc} onInput={e => edit('cc', e.currentTarget.value)} /></label>
              <label>Subject<input value={value().subject} onInput={e => edit('subject', e.currentTarget.value)} /></label>
              <label class="body-label">Message<textarea value={value().body} onInput={e => edit('body', e.currentTarget.value)} rows={14} /></label>
            </fieldset>
            <div class="send-actions">
                <button class="primary" disabled={!online() || busy() || !value().to.trim() || (!value().locked && senderDisabled(value().from))}><Icon name="sent" />{busy() ? 'Sending…' : value().locked ? 'Retry send safely' : 'Send message'}</button>
               <Show when={!value().locked && senderDisabled(value().from)}><small>Sending is disabled for this sender's domain. Choose another sender or update its policy.</small></Show>
              <Show when={value().rejected}><button type="button" disabled={busy()} onClick={async () => {
                const corrected = {...value(), id: crypto.randomUUID(), key: crypto.randomUUID(), locked: false, rejected: false};
                await drafts.save(corrected); await loadDrafts(); await openDraft(corrected);
              }}>Copy to new draft</button></Show>
               <Show when={!value().locked || value().rejected}><button type="button" disabled={busy()} onClick={async () => {clearTimeout(timer); await writeQueue; await drafts.remove(value().id); setDraft(undefined); setEntry(''); setPane('list'); await loadDrafts();}}>Discard draft</button></Show>
              <Show when={!online()}><small>Connect to send. Your draft is safe.</small></Show>
            </div>
          </form>}
        </Show>
      </main>
      </Show>}>
        <main class="domains-page"><h2>Domains</h2><Show when={domainList.error}><p role="alert">Domain directory unavailable.</p></Show><Domains domains={domainList.data ?? []} online={online()} refresh={refreshDomains} fail={fail} administrator={props.account.identity.administrator} client={client} commands={commands} /></main>
      </Show>
    </div>
  </div>;
}
