import { Code, ConnectError, createClient } from '@connectrpc/connect';
import { createGrpcWebTransport } from '@connectrpc/connect-web';
import { MailService, Folder } from './gen/inbox/v1/inbox_pb';
import { cached, type Draft } from './storage';
import { createStorage } from './storage';
import type { Account } from './session';
export { Folder };
export const client = createClient(MailService, createGrpcWebTransport({baseUrl: location.origin, defaultTimeoutMs: 15000}));
// Connect 2.x treats an explicit zero timeout as no deadline, overriding the unary default.
export const changeStream = (resumeToken: string, signal: AbortSignal) => client.watchChanges({resumeToken}, {signal, timeoutMs: 0});
export const mailboxes = (page: string, signal?: AbortSignal) => cached(`boxes:${page}`, () => client.listMailboxes({pageSize: 50, pageToken: page}, {signal}));
export const domains = (page: string, signal?: AbortSignal) => cached(`domains:${page}`, () => client.listDomains({pageSize: 50, pageToken: page}, {signal}));
export function accountApi(account: Account, storage: ReturnType<typeof createStorage>) {
  const scoped = createClient(MailService, createGrpcWebTransport({
    baseUrl: location.origin, defaultTimeoutMs: 15000,
    fetch: async (input, init) => {
      await account.verify();
      account.assert();
      const signal = init?.signal ? AbortSignal.any([init.signal, account.signal]) : account.signal;
      const headers = new Headers(init?.headers);
      headers.set('x-inbox-expected-user', account.identity.user_id.toLowerCase());
      const response = await fetch(input, {...init, headers, signal, credentials: 'same-origin', cache: 'no-store'});
      if (response.status === 401) account.reject();
      account.assert();
      return response;
    },
    interceptors: [next => async request => {
      account.assert();
      let response;
      try {response = await next(request);}
      catch (error) {if (error instanceof ConnectError && error.code === Code.Unauthenticated) account.reject(); throw error;}
      account.assert();
      if (response.stream) {
        const source = response.message;
        return {...response, message: (async function* () {
          try {for await (const value of source) {account.assert(); yield value;}}
          catch (error) {if (error instanceof ConnectError && error.code === Code.Unauthenticated) account.reject(); throw error;}
        })()};
      }
      return response;
    }],
  }));
  return {
    client: scoped,
    changeStream: (resumeToken: string, signal: AbortSignal) => scoped.watchChanges({resumeToken}, {signal, timeoutMs: 0}),
    mailboxes: (page: string, signal?: AbortSignal) => storage.cached(`boxes:${page}`, () => scoped.listMailboxes({pageSize: 50, pageToken: page}, {signal})),
    allDomains: async (signal?: AbortSignal) => (await collectPages(page => storage.cached(`domains:${page}`, () => scoped.listDomains({pageSize: 50, pageToken: page}, {signal})), signal)).flatMap(page => page.domains),
    messages: (mailboxId: string, folder: Folder, page: string, signal?: AbortSignal) => storage.cached(`list:${mailboxId}:${folder}:${page}`, () => scoped.listMessages({mailboxId, folder, pageSize: 30, pageToken: page}, {signal})),
    message: (mailboxId: string, entryId: string, signal?: AbortSignal) => storage.cached(`detail:${mailboxId}:${entryId}`, () => scoped.getMessage({mailboxId, entryId}, {signal})),
  };
}
export async function collectPages<T>(read: (page: string) => Promise<{nextPageToken: string} & T>, signal?: AbortSignal) {
  const pages: T[] = [];
  const seen = new Set<string>();
  let page = '';
  do {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    if (seen.has(page) || seen.size >= 1000) throw new Error('Directory pagination did not complete. Refresh to try again.');
    seen.add(page);
    const result = await read(page);
    pages.push(result);
    page = result.nextPageToken;
  } while (page);
  return pages;
}
export const allDomains = async (signal?: AbortSignal) => (await collectPages(page => domains(page, signal), signal)).flatMap(page => page.domains);
export const messages = (mailboxId: string, folder: Folder, page: string, signal?: AbortSignal) => cached(`list:${mailboxId}:${folder}:${page}`, () => client.listMessages({mailboxId, folder, pageSize: 30, pageToken: page}, {signal}));
export const message = (mailboxId: string, entryId: string, signal?: AbortSignal) => cached(`detail:${mailboxId}:${entryId}`, () => client.getMessage({mailboxId, entryId}, {signal}));
export const addresses = (value: string) => value.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
export function submissionPayload(d: Draft) {
  return {fromMailboxId: d.from, to: addresses(d.to), cc: addresses(d.cc), subject: d.subject, textBody: d.body,
    replyToEntryId: d.replyEntry, sourceMailboxId: d.sourceMailbox, idempotencyKey: d.key};
}
