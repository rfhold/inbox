export interface Identity {
  mode: 'oidc' | 'disabled';
  authenticated: true;
  user_id: string;
  administrator: boolean;
  expires_at: string | null;
}
export type SessionState = {status: 'loading' | 'locked' | 'error'; unsafeLogout?: boolean} | {status: 'ready'; account: Account};
export interface Account {
  identity: Identity;
  namespace: string;
  signal: AbortSignal;
  assert: () => void;
  verify: () => Promise<void>;
  reject: () => void;
}
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export class Session {
  state: SessionState = {status: 'loading'};
  private generation = 0;
  private controller = new AbortController();
  private expiry?: ReturnType<typeof setTimeout>;
  private pending?: Promise<void>;
  private loggingOut = false;
  private logoutPending = false;
  constructor(private changed: (state: SessionState) => void, private notify = () => {}, private environment: () => Promise<string> = async () => 'default') {}
  private publish(state: SessionState) {this.state = state; this.changed(state);}
  lock(status: 'loading' | 'locked' | 'error' = 'locked') {
    this.generation++;
    this.controller.abort();
    this.controller = new AbortController();
    clearTimeout(this.expiry);
    this.publish({status});
  }
  async resolve() {
    if (this.loggingOut) return;
    this.lock('loading');
    const generation = this.generation;
    try {
      this.logoutPending ||= await pendingLogout();
      if (generation !== this.generation) return;
      if (this.logoutPending) {await this.logout(); return;}
      const identity = await this.read();
      const environment = identity?.mode === 'oidc' ? await this.environment() : '';
      if (identity?.mode === 'oidc') await reserveLogoutBarrier();
      if (generation !== this.generation) return;
      if (!identity) {this.lock(); return;}
      const signal = this.controller.signal;
      const assert = () => {
        if (generation !== this.generation || signal.aborted || (identity.expires_at !== null && Date.parse(identity.expires_at) <= Date.now()))
          throw new DOMException('Session changed. Sign in again.', 'AbortError');
      };
      const account: Account = {
        identity, signal, assert,
        namespace: identity.mode === 'disabled' ? `inbox:${location.origin}:shared` : `inbox:${location.origin}:${encodeURIComponent(environment)}:oidc:${identity.user_id}`,
        reject: () => {
          if (!signal.aborted) {this.lock(); this.notify();}
        },
        verify: async () => {
          assert();
          try {this.logoutPending ||= await pendingLogout();}
          catch (error) {if (!signal.aborted) this.lock('error'); throw error;}
          assert();
          if (this.logoutPending) {this.lock(); throw new Error('Sign-out must complete before reopening mail');}
          if (!navigator.onLine) throw new Error('Connect to verify your session.');
          await (this.pending ??= this.check(account).finally(() => {this.pending = undefined;}));
          assert();
        },
      };
      if (identity.expires_at) this.expiry = setTimeout(() => {this.lock(); this.notify();}, Math.min(2147483647, Date.parse(identity.expires_at) - Date.now()));
      this.publish({status: 'ready', account});
    } catch {if (generation === this.generation) this.lock('error');}
  }
  private async read(): Promise<Identity | undefined> {
    // Only the explicitly unauthenticated local workspace can reopen offline.
    // Authenticated identities are never persisted as an unlocking credential.
    if (!navigator.onLine) {
      try {
        if (sessionStorage.getItem('inbox:local-mode') === location.origin)
          return {mode: 'disabled', authenticated: true, user_id: '00000000-0000-4000-8000-000000000001', administrator: true, expires_at: null};
      } catch { /* Storage may be unavailable. */ }
      throw new Error('Connect to verify your session');
    }
    const response = await fetch('/auth/session', {cache: 'no-store', credentials: 'same-origin', redirect: 'error', signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(10000)])});
    if (response.status === 401) {try {sessionStorage.removeItem('inbox:local-mode');} catch {} return;}
    if (!response.ok) throw new Error('Session unavailable');
    const value = await response.json();
    if (!value || !['oidc', 'disabled'].includes(value.mode) || value.authenticated !== true || !uuid.test(value.user_id) || typeof value.administrator !== 'boolean'
      || (value.mode === 'oidc' ? typeof value.expires_at !== 'string' || !Number.isFinite(Date.parse(value.expires_at)) || Date.parse(value.expires_at) <= Date.now() : value.expires_at !== null || value.user_id !== '00000000-0000-4000-8000-000000000001'))
      throw new Error('Invalid session');
    try {
      if (value.mode === 'disabled') sessionStorage.setItem('inbox:local-mode', location.origin);
      else sessionStorage.removeItem('inbox:local-mode');
    } catch { /* Offline reopening remains unavailable without tab storage. */ }
    return value;
  }
  private async check(account: Account) {
    try {
      const identity = await this.read();
      account.assert();
      if (!identity || identity.user_id !== account.identity.user_id || identity.mode !== account.identity.mode || identity.administrator !== account.identity.administrator) {
        this.lock(); this.notify(); throw new Error('Session changed');
      }
    } catch (error) {
      if (!account.signal.aborted) this.lock('error');
      throw error;
    }
  }
  async logout() {
    if (this.loggingOut) return;
    this.loggingOut = true;
    this.logoutPending = true;
    this.lock('loading');
    this.notify();
    try {sessionStorage.removeItem('inbox:local-mode');} catch {}
    let status: 'locked' | 'error' = 'locked';
    let persisted = false;
    try {
      try {await persistLogout(); persisted = true; this.notify();} catch {status = 'error';}
      const response = await fetch('/auth/logout', {method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error', signal: AbortSignal.timeout(15000)});
      if (!response.ok && response.status !== 401) status = 'error';
      else {
        await clearLogoutBarrier();
        this.logoutPending = false;
        status = 'locked';
      }
    } catch {status = 'error';}
    finally {
      this.loggingOut = false; this.lock(status);
      if (status === 'error' && !persisted) this.publish({status: 'error', unsafeLogout: true});
      this.notify();
    }
  }
}
import { clearLogoutBarrier, pendingLogout, persistLogout, reserveLogoutBarrier } from './logout-barrier';
