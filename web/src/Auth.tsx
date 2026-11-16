import { createSignal, onCleanup, onMount, Show } from 'solid-js';
import { QueryClient, QueryClientProvider } from '@tanstack/solid-query';
import App from './App';
import { Session, type Account, type SessionState } from './session';
import { legacyLogoutKey, logoutBarrierKey } from './logout-barrier';

function Workspace(props: {account: Account; logout: () => void}) {
  const queries = new QueryClient({defaultOptions: {queries: {retry: false, networkMode: 'always', staleTime: 15000}}});
  onCleanup(() => {void queries.cancelQueries(); queries.clear();});
  return <QueryClientProvider client={queries}><App account={props.account} logout={props.logout} /></QueryClientProvider>;
}
export default function Auth() {
  const [state, setState] = createSignal<SessionState>({status: 'loading'});
  const [checking, setChecking] = createSignal(false);
  const channel = typeof BroadcastChannel === 'undefined' ? undefined : new BroadcastChannel('inbox:session');
  let environment: Promise<string> | undefined;
  const session = new Session(setState, () => channel?.postMessage('changed'), () => environment ??= (async () => {
    const response = await fetch('/config.json', {cache: 'no-store', credentials: 'same-origin', redirect: 'error', signal: AbortSignal.timeout(10000)});
    if (!response.ok) throw new Error('Configuration unavailable');
    const value = await response.json();
    if (typeof value.environment !== 'string' || !value.environment || value.environment.length > 128) throw new Error('Invalid environment');
    return value.environment;
  })().catch(error => {environment = undefined; throw error;}), () => {
    location.replace(`/auth/login?return_to=${encodeURIComponent(location.pathname + location.search + location.hash)}`);
  });
  let poll: ReturnType<typeof setInterval>;
  onMount(() => {
    channel?.postMessage('changed');
    void session.resolve();
    const external = () => {session.lock(); if (navigator.onLine && !document.hidden) void session.resolve();};
    const storage = (event: StorageEvent) => {if (event.key === legacyLogoutKey || event.key === logoutBarrierKey) external();};
    const reconnect = () => {
      const current = state();
      if (current.status !== 'ready') {void session.resolve(); return;}
      if (!navigator.onLine) return;
      setChecking(true);
      void current.account.verify().catch(() => {}).finally(() => setChecking(false));
    };
    const visible = () => {if (!document.hidden) reconnect();};
    channel?.addEventListener('message', external);
    document.addEventListener('visibilitychange', visible);
    window.addEventListener('online', reconnect);
    window.addEventListener('pageshow', reconnect);
    window.addEventListener('focus', reconnect);
    window.addEventListener('storage', storage);
    poll = setInterval(() => {
      const current = state();
      if (current.status === 'ready' && navigator.onLine) void current.account.verify().catch(() => {});
    }, 10000);
    onCleanup(() => {
      clearInterval(poll); channel?.close(); session.lock();
      document.removeEventListener('visibilitychange', visible);
      window.removeEventListener('online', reconnect); window.removeEventListener('pageshow', reconnect);
      window.removeEventListener('focus', reconnect);
      window.removeEventListener('storage', storage);
    });
  });
  const account = () => {const current = state(); return current.status === 'ready' ? current.account : undefined;};
  const unsafeLogout = () => {const current = state(); return current.status !== 'ready' && current.unsafeLogout;};
   return <Show when={account()} keyed fallback={<main class="welcome"><h1>Inbox</h1><Show when={state().status === 'loading'} fallback={<><p>{unsafeLogout() ? 'Sign-out could not be secured on this device. Keep this tab open and reconnect to complete sign-out before leaving.' : state().status === 'error' ? 'Unable to verify your session. Connect and try again.' : 'Sign in to open your personal workspace.'}</p><p>Offline access requires a session verified in this open tab. Stored mail and drafts are not proof of identity.</p><button onClick={() => void session.resolve()}>Check session</button><a href="/auth/login?return_to=%2F">Sign in</a></>}><p>Checking session…</p></Show></main>}>
    {value => <><Show when={checking()}><main class="welcome"><p>Checking session…</p></main></Show><div style={{display: checking() ? 'none' : 'contents'}}><Workspace account={value} logout={() => void session.logout()} /></div></>}
  </Show>;
}
