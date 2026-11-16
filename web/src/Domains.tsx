import { createEffect, createSignal, For, Show, onMount } from 'solid-js';
import { client } from './api';
import { commandIntent, definitiveRejection, type Intent } from './intents';
import type { Domain } from './gen/inbox/v1/inbox_pb';
import { commands } from './storage';

type Policy = {domainId: string; receiveEnabled: boolean; sendEnabled: boolean};
type Creation = {name: string; receiveEnabled: boolean; sendEnabled: boolean};
type Access = {administrator: boolean; client: typeof client; commands: typeof commands};
export function Domains(props: Access & {domains: Domain[]; online: boolean; refresh: () => Promise<unknown>; fail: (error: unknown) => void}) {
  const {client, commands} = props;
  const [name, setName] = createSignal('');
  const [receive, setReceive] = createSignal(true);
  const [send, setSend] = createSignal(true);
  const [intent, setIntent] = createSignal<Intent<Creation>>();
  let lastIntent: Intent<Creation> | undefined;
  const [busy, setBusy] = createSignal(false);
  const [ready, setReady] = createSignal(false);
  onMount(() => {if (!props.administrator) return; void commands.get<Intent<Creation>>('createDomain').then(saved => {
    if (saved) {setIntent(saved); setName(saved.name); setReceive(saved.receiveEnabled); setSend(saved.sendEnabled);}
    setReady(true);
  }).catch(props.fail);});
  async function createDomain(event: SubmitEvent) {
    event.preventDefault();
    if (!props.administrator || !props.online || busy() || !ready()) return;
    const request = intent() ?? commandIntent(lastIntent, {name: name(), receiveEnabled: receive(), sendEnabled: send()});
    lastIntent = request;
    setIntent(request); setBusy(true);
    try {
      await commands.save('createDomain', request);
      const result = await client.createDomain(request);
      if (!result.domain) throw new Error('Domain creation unconfirmed. Retry with the same request.');
      await commands.remove('createDomain', request.requestId); setIntent(undefined); lastIntent = undefined; setName(''); await props.refresh();
    } catch (error) {if (definitiveRejection(error)) {await commands.remove('createDomain', request.requestId); setIntent(undefined);} props.fail(error);}
    finally {setBusy(false);}
  }
  return <section class="domains" aria-label="Domain management">
    <p class="muted">Local delivery only. Names cannot be renamed or deleted. Receive and send policies are independent; mail stays readable.</p>
    <For each={props.domains.map(domain => domain.id)}>{id => <Show when={props.administrator} fallback={<p>{props.domains.find(domain => domain.id === id)!.name}</p>}><DomainPolicy domain={props.domains.find(domain => domain.id === id)!} online={props.online} refresh={props.refresh} fail={props.fail} client={client} commands={commands} /></Show>}</For>
    <Show when={props.administrator}><form class="domain-create" onSubmit={createDomain}>
      <fieldset disabled={!ready() || !!intent() || busy()}>
        <label>Domain name<input required value={name()} placeholder="example.test" onInput={e => setName(e.currentTarget.value)} /></label>
        <label class="check"><input type="checkbox" checked={receive()} onChange={e => setReceive(e.currentTarget.checked)} />Receive enabled</label>
        <label class="check"><input type="checkbox" checked={send()} onChange={e => setSend(e.currentTarget.checked)} />Send enabled</label>
      </fieldset>
      <Show when={intent()}><p class="warning">Creation unconfirmed. Retry the same request before changing it. Keep this window open.</p></Show>
      <button disabled={!ready() || !props.online || busy()}>{intent() ? 'Retry domain creation safely' : 'Create domain'}</button>
    </form></Show>
  </section>;
}
function DomainPolicy(props: Omit<Access, 'administrator'> & {domain: Domain; online: boolean; refresh: () => Promise<unknown>; fail: (error: unknown) => void}) {
  const {client, commands} = props;
  const [receive, setReceive] = createSignal(props.domain.receiveEnabled);
  const [send, setSend] = createSignal(props.domain.sendEnabled);
  const [intent, setIntent] = createSignal<Intent<Policy>>();
  let lastIntent: Intent<Policy> | undefined;
  const [busy, setBusy] = createSignal(false);
  const [ready, setReady] = createSignal(false);
  const storageKey = `policy:${props.domain.id}`;
  onMount(() => {void commands.get<Intent<Policy>>(storageKey).then(saved => {
    if (saved) {setIntent(saved); setReceive(saved.receiveEnabled); setSend(saved.sendEnabled);}
    setReady(true);
  }).catch(props.fail);});
  createEffect(() => {
    const domain = props.domain;
    if (!intent()) {setReceive(domain.receiveEnabled); setSend(domain.sendEnabled);}
  });
  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (!props.online || busy() || !ready()) return;
    const request = intent() ?? commandIntent(lastIntent, {domainId: props.domain.id, receiveEnabled: receive(), sendEnabled: send()});
    lastIntent = request;
    setIntent(request); setBusy(true);
    try {
      await commands.save(storageKey, request);
      const result = await client.setDomainPolicy(request);
      if (!result.domain) throw new Error('Policy update unconfirmed. Retry with the same request.');
      await commands.remove(storageKey, request.requestId); await props.refresh(); setIntent(undefined); lastIntent = undefined;
    } catch (error) {if (definitiveRejection(error)) {await commands.remove(storageKey, request.requestId); setIntent(undefined);} props.fail(error);}
    finally {setBusy(false);}
  }
  return <form class="domain-policy" aria-label={`Policy for ${props.domain.name}`} onSubmit={save}>
    <strong>{props.domain.name}</strong>
    <fieldset disabled={!ready() || !!intent() || busy()}>
      <label class="check"><input type="checkbox" checked={receive()} onChange={e => setReceive(e.currentTarget.checked)} />Receive enabled</label>
      <label class="check"><input type="checkbox" checked={send()} onChange={e => setSend(e.currentTarget.checked)} />Send enabled</label>
    </fieldset>
    <Show when={intent()}><p class="warning">Update unconfirmed. Retry the original policy before changing it. Keep this window open.</p></Show>
    <button disabled={!ready() || !props.online || busy()}>{intent() ? 'Retry policy safely' : 'Save policy'}</button>
  </form>;
}
