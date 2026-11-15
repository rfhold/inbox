import { expect, test, type Page } from '@playwright/test';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { ChangeEventSchema, ChangeKind, CreateMailboxRequestSchema, CreateMailboxResponseSchema, Folder, GetMessageResponseSchema, GetSubmissionRequestSchema, GetSubmissionResponseSchema, ListMailboxesResponseSchema, ListMessagesResponseSchema, SetReadStateRequestSchema, SetReadStateResponseSchema, SubmissionStatus, SubmitMessageResponseSchema, WatchChangesRequestSchema } from '../src/gen/inbox/v1/inbox_pb';
import { grpcFrame, openDrafts, domainReply, domainFields } from './helpers';
import { CreateDomainRequestSchema, CreateDomainResponseSchema, ListDomainsRequestSchema, ListDomainsResponseSchema, SetDomainPolicyRequestSchema, SetDomainPolicyResponseSchema } from '../src/gen/inbox/v1/inbox_pb';
import { mockLocalSession } from './helpers';
test.beforeEach(async ({context}) => {await mockLocalSession(context);});

declare global {
  interface Window {
    watchFixture: {
      requests: {bytes: number[]; timeout: string | null; traceparent: string | null}[];
      active: number;
      maximum: number;
      emit: (bytes: number[]) => void;
      disconnect: () => void;
    };
  }
}
async function installStream(page: Page) {
  await page.addInitScript(() => {
    const original = window.fetch.bind(window);
    let current: ReadableStreamDefaultController<Uint8Array> | undefined;
    window.watchFixture = {requests: [], active: 0, maximum: 0,
      emit: bytes => current!.enqueue(new Uint8Array(bytes)),
      disconnect: () => current!.error(new TypeError('Synthetic disconnect')),
    };
    window.fetch = async (input, init) => {
      const url = input instanceof Request ? input.url : String(input);
      if (!url.endsWith('/inbox.v1.MailService/WatchChanges')) return original(input, init);
      const state = window.watchFixture;
      state.requests.push({bytes: [...new Uint8Array(init!.body as ArrayBuffer)], timeout: new Headers(init?.headers).get('grpc-timeout'), traceparent: new Headers(init?.headers).get('traceparent')});
      state.active++; state.maximum = Math.max(state.maximum, state.active);
      const body = new ReadableStream<Uint8Array>({start(controller) {current = controller;}});
      const controller = current!;
      init!.signal!.addEventListener('abort', () => {state.active--; try {controller.error(new DOMException('Aborted', 'AbortError'));} catch { /* Already disconnected. */ }}, {once: true});
      return new Response(body, {headers: {'content-type': 'application/grpc-web+proto'}});
    };
  });
}
async function emit(page: Page, kind: ChangeKind, resumeToken: string, scope: {mailboxId?: string; entryId?: string; submissionId?: string; domainId?: string} = {}) {
  const payload = toBinary(ChangeEventSchema, create(ChangeEventSchema, {kind, resumeToken, ...scope}));
  const bytes = new Uint8Array(5 + payload.length);
  new DataView(bytes.buffer).setUint32(1, payload.length); bytes.set(payload, 5);
  await page.evaluate(bytes => window.watchFixture.emit(bytes), [...bytes]);
}
async function cursor(page: Page) {
  return page.evaluate(() => new Promise<string>((resolve, reject) => {
    const request = indexedDB.open(`inbox:${location.origin}:shared`);
    request.onsuccess = () => {
      const tx = request.result.transaction('metadata');
      const state = tx.objectStore('metadata').get('cache');
      tx.oncomplete = () => {request.result.close(); resolve(state.result?.cursor ?? '');};
      tx.onerror = () => reject(tx.error);
    };
  }));
}

test('mailbox projection refetch preserves the displayed and persisted draft sender when options are replaced', async ({page}) => {
  await installStream(page);
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  let projection = 0;
  let sendEnabled = true;
  let mailboxReads = 0;
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = toBinary(ListDomainsResponseSchema, create(ListDomainsResponseSchema, {
      domains: [{id: domainFields.domainId, name: domainFields.domain, receiveEnabled: true, sendEnabled}],
    }));
    else if (method === 'ListMailboxes') {
      mailboxReads++;
      payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [
        {...domainFields, id: 'first-option-not-sender', localPart: 'other', address: 'other@inbox.test', name: `Other projection ${projection}`},
        {...domainFields, id: 'saved-sender', localPart: 'sender', address: 'sender@inbox.test', name: `Sender projection ${projection}`},
      ]}));
    } else if (method === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {}));
    else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await emit(page, ChangeKind.RESET, 'sender-projection-initial');
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText('Up to date');
  await page.getByRole('combobox', {name: 'Mailbox', exact: true}).selectOption('saved-sender');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill('other@inbox.test');
  await page.getByLabel('Subject', {exact: true}).fill('Unchanged sender across projection replacement');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  const sender = page.getByRole('combobox', {name: 'From', exact: true});
  await expect(sender).toHaveValue('saved-sender');
  const readDrafts = () => page.evaluate(() => new Promise<unknown[]>((resolve, reject) => {
    const request = indexedDB.open(`inbox:${location.origin}:shared`);
    request.onsuccess = () => {
      const tx = request.result.transaction('drafts');
      const drafts = tx.objectStore('drafts').getAll();
      tx.oncomplete = () => {request.result.close(); resolve(drafts.result);};
      tx.onerror = () => reject(tx.error);
    };
    request.onerror = () => reject(request.error);
  }));
  const before = await readDrafts();
  expect(before).toHaveLength(1);
  expect(before[0]).toMatchObject({from: 'saved-sender', subject: 'Unchanged sender across projection replacement'});
  await sender.locator('option[value="saved-sender"]').evaluate(option => option.setAttribute('data-before-projection', 'true'));
  const readsBefore = mailboxReads;
  projection++; sendEnabled = false;
  await emit(page, ChangeKind.DOMAIN, 'sender-projection-changed', {domainId: domainFields.domainId});
  await expect.poll(() => mailboxReads).toBeGreaterThan(readsBefore);
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText('Up to date');
  await expect(sender.locator('option[value="saved-sender"]')).not.toHaveAttribute('data-before-projection', 'true');
  await expect(sender).toHaveValue('saved-sender');
  expect(await readDrafts()).toEqual(before);
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
  // Opening the durable draft again must show the same sender and immutable key.
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await openDrafts(page);
  await page.locator('.draft-link').filter({hasText: 'Unchanged sender across projection replacement'}).click();
  await expect(sender).toHaveValue('saved-sender');
  expect(await readDrafts()).toEqual(before);
});

test('opener-cloned tabs keep separate uncertain domain intents through reload and successful sibling cleanup', async ({page: a, context}) => {
  await context.addInitScript(() => {Object.assign(window, {initialCommandTab: sessionStorage.getItem('inbox:command-tab')});});
  await context.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const attempts = new Map<string, ReturnType<typeof fromBinary<typeof CreateDomainRequestSchema>>[]>();
  const directory = [{id: 'clone-default', name: 'inbox.test', receiveEnabled: true, sendEnabled: true}];
  await context.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = toBinary(ListDomainsResponseSchema, create(ListDomainsResponseSchema, {domains: directory}));
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {}));
    else if (method === 'CreateDomain') {
      const raw = route.request().postDataBuffer()!;
      const request = fromBinary(CreateDomainRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1)));
      const calls = attempts.get(request.name) ?? [];
      calls.push(request); attempts.set(request.name, calls);
      if (calls.length === 1) return route.abort('failed');
      const domain = {id: request.requestId, name: request.name, receiveEnabled: request.receiveEnabled, sendEnabled: request.sendEnabled};
      directory.push(domain);
      payload = toBinary(CreateDomainResponseSchema, create(CreateDomainResponseSchema, {domain}));
    } else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await a.goto('/');
  await a.locator('.domains > summary').click();
  await a.getByLabel('Domain name', {exact: true}).fill('clone-a.test');
  await a.getByRole('button', {name: 'Create domain', exact: true}).click();
  await expect(a.getByRole('button', {name: 'Retry domain creation safely', exact: true})).toBeEnabled();
  const ownerA = await a.evaluate(() => sessionStorage.getItem('inbox:command-tab'));
  const popup = a.waitForEvent('popup');
  await a.evaluate(() => {window.open('/');});
  const b = await popup;
  try {
    await b.waitForLoadState();
    expect(await b.evaluate(() => (window as Window & {initialCommandTab?: string | null}).initialCommandTab)).toBe(ownerA);
    await b.locator('.domains > summary').click();
    await expect(b.getByLabel('Domain name', {exact: true})).toBeEnabled();
    expect(await b.evaluate(() => sessionStorage.getItem('inbox:command-tab'))).not.toBe(ownerA);
    await b.getByLabel('Domain name', {exact: true}).fill('clone-b.test');
    await b.locator('.domain-create').getByLabel('Receive enabled').uncheck();
    await b.getByRole('button', {name: 'Create domain', exact: true}).click();
    await expect(b.getByRole('button', {name: 'Retry domain creation safely', exact: true})).toBeEnabled();
    for (const [tab, name] of [[a, 'clone-a.test'], [b, 'clone-b.test']] as const) {
      await tab.reload();
      await tab.locator('.domains > summary').click();
      await expect(tab.getByLabel('Domain name', {exact: true})).toHaveValue(name);
      await expect(tab.getByLabel('Domain name', {exact: true})).toBeDisabled();
      await expect(tab.getByRole('button', {name: 'Retry domain creation safely', exact: true})).toBeEnabled();
    }
    await b.getByRole('button', {name: 'Retry domain creation safely', exact: true}).click();
    await expect(b.getByRole('form', {name: 'Policy for clone-b.test', exact: true})).toBeVisible();
    await a.reload();
    await a.locator('.domains > summary').click();
    await expect(a.getByLabel('Domain name', {exact: true})).toHaveValue('clone-a.test');
    await a.getByRole('button', {name: 'Retry domain creation safely', exact: true}).click();
    await expect(a.getByRole('form', {name: 'Policy for clone-a.test', exact: true})).toBeVisible();
    for (const name of ['clone-a.test', 'clone-b.test']) {
      expect(attempts.get(name)).toHaveLength(2);
      expect(attempts.get(name)![1]).toEqual(attempts.get(name)![0]);
      expect(attempts.get(name)![0].requestId).toBeTruthy();
    }
    expect(attempts.get('clone-a.test')![0].requestId).not.toBe(attempts.get('clone-b.test')![0].requestId);
  } finally {await b.close();}
});

test('domain selectors load every page and uncertain domain create/policy commands survive reload and invalidation unchanged', async ({page}) => {
  await installStream(page);
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const creates: ReturnType<typeof fromBinary<typeof CreateDomainRequestSchema>>[] = [];
  const policies: ReturnType<typeof fromBinary<typeof SetDomainPolicyRequestSchema>>[] = [];
  const directory = [
    {id: 'directory-one', name: 'one.test', receiveEnabled: true, sendEnabled: true},
    {id: 'directory-two', name: 'two.test', receiveEnabled: false, sendEnabled: false},
  ];
  const tokens: string[] = [];
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    const raw = route.request().postDataBuffer()!;
    const data = raw.subarray(5, 5 + raw.readUInt32BE(1));
    let payload: Uint8Array;
    if (method === 'ListDomains') {
      const request = fromBinary(ListDomainsRequestSchema, data);
      tokens.push(request.pageToken);
      payload = toBinary(ListDomainsResponseSchema, create(ListDomainsResponseSchema, {
        domains: request.pageToken ? directory.slice(1) : directory.slice(0, 1),
        nextPageToken: request.pageToken ? '' : 'page-two',
      }));
    } else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {}));
    else if (method === 'CreateDomain') {
      const request = fromBinary(CreateDomainRequestSchema, data);
      creates.push(request);
      if (creates.length === 1) return route.abort('failed');
      const domain = {id: 'directory-new', name: request.name, receiveEnabled: request.receiveEnabled, sendEnabled: request.sendEnabled};
      directory.push(domain);
      payload = toBinary(CreateDomainResponseSchema, create(CreateDomainResponseSchema, {domain}));
    } else if (method === 'SetDomainPolicy') {
      const request = fromBinary(SetDomainPolicyRequestSchema, data);
      policies.push(request);
      if (policies.length === 1) return route.abort('failed');
      const domain = directory.find(domain => domain.id === request.domainId)!;
      Object.assign(domain, {receiveEnabled: request.receiveEnabled, sendEnabled: request.sendEnabled});
      payload = toBinary(SetDomainPolicyResponseSchema, create(SetDomainPolicyResponseSchema, {domain}));
    } else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Create mailbox', exact: true}).click();
  await expect(page.getByLabel('Mailbox domain').locator('option')).toHaveCount(2);
  await page.getByLabel('Mailbox domain').selectOption({label: 'two.test'});
  expect(tokens).toEqual(['', 'page-two']);
  await page.getByRole('button', {name: 'Create mailbox', exact: true}).click();
  await page.locator('.domains > summary').click();
  await page.getByLabel('Domain name', {exact: true}).fill('new.test');
  await page.locator('.domain-create').getByLabel('Receive enabled').uncheck();
  await page.getByRole('button', {name: 'Create domain', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Retry domain creation safely', exact: true})).toBeEnabled();
  await expect(page.getByLabel('Domain name', {exact: true})).toBeDisabled();
  await page.reload();
  await page.locator('.domains > summary').click();
  await page.getByRole('button', {name: 'Retry domain creation safely', exact: true}).click();
  await expect(page.getByRole('form', {name: 'Policy for new.test', exact: true})).toBeVisible();
  expect(creates).toHaveLength(2);
  expect(creates[1]).toEqual(creates[0]);
  expect(creates[0].requestId).toBeTruthy();
  const form = page.getByRole('form', {name: 'Policy for one.test', exact: true});
  await form.getByLabel('Receive enabled').uncheck();
  await form.getByRole('button', {name: 'Save policy', exact: true}).click();
  await expect(form.getByRole('button', {name: 'Retry policy safely', exact: true})).toBeEnabled();
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await emit(page, ChangeKind.RESET, 'initial-domain');
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText('Up to date');
  directory[0].sendEnabled = false;
  await emit(page, ChangeKind.DOMAIN, 'changed-domain', {domainId: 'directory-one'});
  await expect.poll(() => cursor(page)).toBe('changed-domain');
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText('Up to date');
  // An unrelated policy refresh cannot overwrite the immutable unconfirmed intent.
  await expect(form.getByLabel('Receive enabled')).not.toBeChecked();
  await expect(form.getByLabel('Send enabled')).toBeChecked();
  await expect(form.getByLabel('Send enabled')).toBeDisabled();
  await page.reload();
  await page.locator('.domains > summary').click();
  await form.getByRole('button', {name: 'Retry policy safely', exact: true}).click();
  await expect(form.getByRole('button', {name: 'Save policy', exact: true})).toBeEnabled();
  expect(policies).toHaveLength(2);
  expect(policies[1]).toEqual(policies[0]);
  expect(policies[0]).toMatchObject({domainId: 'directory-one', receiveEnabled: false, sendEnabled: true});
  expect(policies[0].requestId).toBeTruthy();
});

test('partial gRPC-Web events refresh mail, cross-client read state, mailbox and delivery without polling or touching an active draft', async ({page, context}) => {
  await page.clock.install();
  await installStream(page);
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const mailbox = {...domainFields, id: 'live-box', name: 'Synthetic live mail', address: 'live@inbox.test', localPart: 'live'};
  let mailboxName = mailbox.name;
  let entries = [{id: 'live-entry', mailboxId: mailbox.id, messageId: 'synthetic-message', folder: Folder.INBOX, fromAddress: 'synthetic@inbox.test', to: [mailbox.address], subject: 'Before live change', preview: 'Synthetic preview', isRead: false}];
  const statuses = new Map<string, SubmissionStatus>();
  let sends = 0;
  const calls = new Map<string, number>();
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    calls.set(method, (calls.get(method) ?? 0) + 1);
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = domainReply();
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...mailbox, name: mailboxName}]}));
    else if (method === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {entries}));
    else if (method === 'GetMessage') payload = toBinary(GetMessageResponseSchema, create(GetMessageResponseSchema, {message: {entry: entries[0], textBody: 'Synthetic cached live body'}}));
    else if (method === 'GetSubmission') {
      const raw = route.request().postDataBuffer()!;
      const {submissionId} = fromBinary(GetSubmissionRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1)));
      payload = toBinary(GetSubmissionResponseSchema, create(GetSubmissionResponseSchema, {submission: {id: submissionId, status: statuses.get(submissionId)}}));
    }
    else if (method === 'SubmitMessage') {
      const id = `live-job-${++sends}`;
      statuses.set(id, sends === 1 ? SubmissionStatus.SUCCEEDED : SubmissionStatus.PENDING);
      if (sends === 1) {
        // Delivery wins the race against the local confirmation write.
        await emit(page, ChangeKind.SUBMISSION, 'delivery-before-confirmation', {submissionId: id, mailboxId: mailbox.id});
        await expect.poll(() => cursor(page)).toBe('delivery-before-confirmation');
      }
      payload = toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {submission: {id, status: SubmissionStatus.PENDING}}));
    } else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await emit(page, ChangeKind.RESET, 'initial');
  await expect(page.locator('.message-row')).toHaveCount(1);
  await expect.poll(() => cursor(page)).toBe('initial');
  entries = [{...entries[0], subject: 'Arrived through live stream'}];
  await emit(page, ChangeKind.MESSAGE, 'message-1', {mailboxId: mailbox.id, entryId: entries[0].id});
  await expect(page.locator('.message-row')).toContainText('Arrived through live stream');
  await page.locator('.message-row').click();
  await expect(page.getByRole('button', {name: 'Mark read', exact: true})).toBeVisible();
  entries = [{...entries[0], isRead: true}];
  await emit(page, ChangeKind.MESSAGE, 'read-cross-client', {mailboxId: mailbox.id, entryId: entries[0].id});
  await expect(page.getByRole('button', {name: 'Mark unread', exact: true})).toBeVisible();
  expect(calls.get('SetReadState') ?? 0).toBe(0);
  mailboxName = 'Renamed by another client';
  await emit(page, ChangeKind.MAILBOX, 'mailbox-1', {mailboxId: mailbox.id});
  await expect(page.getByLabel('Mailbox', {exact: true}).locator('option:checked')).toContainText(mailboxName);
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill(mailbox.address);
  await page.getByLabel('Subject', {exact: true}).fill('Local draft must survive changes');
  await page.getByLabel('Message', {exact: true}).fill('Newest unsaved local text');
  await emit(page, ChangeKind.MESSAGE, 'while-composing', {mailboxId: mailbox.id, entryId: entries[0].id});
  await expect.poll(() => cursor(page)).toBe('while-composing');
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Newest unsaved local text');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  const count = [...calls];
  await emit(page, ChangeKind.MESSAGE, 'while-composing', {mailboxId: mailbox.id, entryId: entries[0].id});
  await emit(page, ChangeKind.HEARTBEAT, 'while-composing');
  await page.clock.runFor(10000);
  expect([...calls]).toEqual(count);
  expect(await page.evaluate(() => window.watchFixture.requests[0].timeout)).toBeNull();
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByRole('status')).toContainText('Message accepted');
  await page.locator('.delivery > summary').click();
  await expect(page.locator('.job > summary')).toHaveText('Delivered');
  const submissionCalls = calls.get('GetSubmission');
  await emit(page, ChangeKind.HEARTBEAT, 'delivery-before-confirmation');
  await page.clock.runFor(10000);
  expect(calls.get('GetSubmission')).toBe(submissionCalls);
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill(mailbox.address);
  await page.getByLabel('Subject', {exact: true}).fill('Delivery after local confirmation');
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.locator('.job > summary').filter({hasText: 'Pending delivery'})).toHaveCount(1);
  statuses.set('live-job-2', SubmissionStatus.SUCCEEDED);
  await emit(page, ChangeKind.SUBMISSION, 'delivery-after-confirmation', {submissionId: 'live-job-2', mailboxId: mailbox.id});
  await expect(page.locator('.job > summary').filter({hasText: /^Delivered$/})).toHaveCount(2);
  await page.locator('.message-row').click();
  await expect(page.locator('.body')).toContainText('Synthetic cached live body');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('Subject', {exact: true}).fill('Offline live draft');
  await page.getByLabel('Message', {exact: true}).fill('Offline edits stay local');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await expect.poll(() => page.evaluate(() => !!navigator.serviceWorker.controller)).toBe(true);
  await context.setOffline(true);
  await page.reload();
  await expect(page.locator('.message-row')).toHaveCount(1);
  await page.locator('.message-row').click();
  await expect(page.locator('.body')).toContainText('Synthetic cached live body');
  await openDrafts(page);
  await page.locator('.draft-link').filter({hasText: 'Offline live draft'}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Offline edits stay local');
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
  await context.setOffline(false);
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await emit(page, ChangeKind.RESET, 'reset-after-reconnect');
  await expect.poll(() => cursor(page)).toBe('reset-after-reconnect');
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Offline edits stay local');
  expect(await page.evaluate(() => window.watchFixture.maximum)).toBe(1);
});

test('mailbox and read-state mutation retries retain the same nonempty request ID and payload', async ({page}) => {
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const creates: ReturnType<typeof fromBinary<typeof CreateMailboxRequestSchema>>[] = [];
  const reads: ReturnType<typeof fromBinary<typeof SetReadStateRequestSchema>>[] = [];
  const box = {...domainFields, id: 'intent-box', address: 'intent@inbox.test', name: 'Synthetic intent', localPart: 'intent'};
  let entry = {id: 'intent-entry', mailboxId: box.id, subject: 'Synthetic read intent', fromAddress: box.address, folder: Folder.INBOX, isRead: false};
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = domainReply();
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [box]}));
    else if (method === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {entries: [entry]}));
    else if (method === 'GetMessage') payload = toBinary(GetMessageResponseSchema, create(GetMessageResponseSchema, {message: {entry, textBody: 'Synthetic intent body'}}));
    else if (method === 'CreateMailbox') {
      const raw = route.request().postDataBuffer()!;
      creates.push(fromBinary(CreateMailboxRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1))));
      if (creates.length === 1) return route.abort('failed');
      payload = toBinary(CreateMailboxResponseSchema, create(CreateMailboxResponseSchema, {mailbox: box}));
    } else if (method === 'SetReadState') {
      const raw = route.request().postDataBuffer()!;
      reads.push(fromBinary(SetReadStateRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1))));
      if (reads.length === 1) return route.abort('failed');
      entry = {...entry, isRead: true};
      payload = toBinary(SetReadStateResponseSchema, create(SetReadStateResponseSchema, {entry}));
    } else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Create mailbox', exact: true}).click();
  await page.getByLabel('Name', {exact: true}).fill(box.name);
  await page.getByLabel('Address local part').fill(box.localPart);
  await page.getByRole('button', {name: 'Create', exact: true}).click();
  await expect.poll(() => creates.length).toBe(1);
  await expect(page.getByLabel('Mailbox domain')).toBeDisabled();
  await page.reload();
  await expect(page.getByRole('button', {name: 'Retry mailbox creation safely', exact: true})).toBeEnabled();
  await page.getByRole('button', {name: 'Retry mailbox creation safely', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Retry mailbox creation safely', exact: true})).toHaveCount(0);
  expect(creates[0].requestId).toBeTruthy(); expect(creates[1]).toEqual(creates[0]);
  expect(creates[0].domainId).toBe(domainFields.domainId);
  await page.locator('.message-row').click();
  await page.getByRole('button', {name: 'Mark read', exact: true}).click();
  await expect.poll(() => reads.length).toBe(1);
  await expect(page.getByRole('status')).toContainText('Request could not be completed');
  await page.getByRole('button', {name: 'Mark read', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Mark unread', exact: true})).toBeVisible();
  expect(reads[0].requestId).toBeTruthy(); expect(reads[1]).toEqual(reads[0]);
});

test('resumed streaming traces propagate safely without cursor, IDs, payloads or disconnect errors in local Faro capture', async ({page}) => {
  await installStream(page);
  const collected: {body: string; headers: Record<string, string>}[] = [];
  const secret = 'SYNTHETIC_PRIVATE_WATCH_CURSOR';
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '/local-watch-collector', environment: 'test', version: 'test'}}));
  await page.route('**/local-watch-collector', async route => {
    collected.push({body: route.request().postData() || '', headers: await route.request().allHeaders()});
    await route.fulfill({status: 202, body: ''});
  });
  await page.route('**/inbox.v1.MailService/*', route => route.fulfill({body: grpcFrame(route.request().url().endsWith('/ListDomains') ? domainReply() : toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {}))), contentType: 'application/grpc-web+proto'}));
  await page.goto(`/?private=${secret}`);
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await emit(page, ChangeKind.RESET, secret);
  await expect.poll(() => cursor(page)).toBe(secret);
  await page.evaluate(() => window.watchFixture.disconnect());
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(2);
  const resumed = await page.evaluate(() => window.watchFixture.requests[1]);
  const bytes = Buffer.from(resumed.bytes);
  expect(fromBinary(WatchChangesRequestSchema, bytes.subarray(5, 5 + bytes.readUInt32BE(1))).resumeToken).toBe(secret);
  expect(resumed.traceparent).toMatch(/^00-[a-f0-9]{32}-[a-f0-9]{16}-01$/);
  await emit(page, ChangeKind.HEARTBEAT, secret);
  await expect.poll(() => collected.some(item => item.body.includes('MailService/WatchChanges'))).toBe(true);
  const body = collected.map(item => item.body).join('\n');
  expect(body).not.toContain(secret);
  expect(body).not.toContain('application_error');
  for (const item of collected) {
    expect(item.headers.traceparent).toBeUndefined(); expect(item.headers['idempotency-key']).toBeUndefined();
    const payload = JSON.parse(item.body);
    expect(payload.meta.page.url).toBe('/'); expect(payload.meta.session).toBeUndefined(); expect(payload.meta.user).toBeUndefined();
  }
  expect(await page.evaluate(() => window.watchFixture.maximum)).toBe(1);
});

test('a failed final notification retries only unfinished mail and job reads, retaining stale status and local drafts until success', async ({page, context}) => {
  await page.clock.install();
  await installStream(page);
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const box = {...domainFields, id: 'retry-final-box', name: 'Synthetic final notification', address: 'final@inbox.test', localPart: 'final'};
  let entry = {id: 'retry-final-entry', mailboxId: box.id, messageId: 'retry-final-message', folder: Folder.INBOX, fromAddress: box.address, subject: 'Before final event', isRead: false};
  let status = SubmissionStatus.PENDING;
  let failList = 0;
  let failJob = 0;
  let listCalls = 0;
  let jobCalls = 0;
  let holdJob = false;
  let release!: () => void;
  const gate = new Promise<void>(resolve => {release = resolve;});
  const sync = page.getByRole('note', {name: 'Mail synchronization'});
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = route.request().url().split('/').pop()!;
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = domainReply();
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [box]}));
    else if (method === 'ListMessages') {
      listCalls++;
      payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {entries: [entry]}));
      if (failList) {failList--; return route.fulfill({body: grpcFrame(payload, 14), contentType: 'application/grpc-web+proto'});}
    } else if (method === 'GetMessage') payload = toBinary(GetMessageResponseSchema, create(GetMessageResponseSchema, {message: {entry, textBody: 'Synthetic cached final body'}}));
    else if (method === 'SubmitMessage') payload = toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {submission: {id: 'final-job', status}}));
    else if (method === 'GetSubmission') {
      jobCalls++;
      payload = toBinary(GetSubmissionResponseSchema, create(GetSubmissionResponseSchema, {submission: {id: 'final-job', status}}));
      if (failJob) {failJob--; return route.fulfill({body: grpcFrame(payload, 14), contentType: 'application/grpc-web+proto'});}
      if (holdJob) await gate;
    } else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await expect(sync).toContainText('Reconnecting');
  await emit(page, ChangeKind.RESET, 'initial-final');
  await expect(sync).toHaveText('Up to date');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill(box.address);
  await page.getByLabel('Subject', {exact: true}).fill('Synthetic pending job');
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByRole('status')).toContainText('Message accepted');
  await expect.poll(() => jobCalls).toBeGreaterThan(0);
  await expect(sync).toHaveText('Up to date');
  await page.locator('.message-row').click();
  await expect(page.locator('.body')).toHaveText('Synthetic cached final body');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('Subject', {exact: true}).fill('Durable draft during failed refresh');
  await page.getByLabel('Message', {exact: true}).fill('Newest local draft text');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  const beforeList = listCalls, beforeJobs = jobCalls;
  entry = {...entry, subject: 'Updated by final notification'};
  status = SubmissionStatus.SUCCEEDED;
  failList = 1; failJob = 1; holdJob = true;
  await emit(page, ChangeKind.SUBMISSION, 'final-no-more-events', {mailboxId: box.id, submissionId: 'final-job'});
  await expect.poll(() => cursor(page)).toBe('final-no-more-events');
  await expect.poll(() => listCalls).toBeGreaterThanOrEqual(beforeList + 2);
  await expect.poll(() => jobCalls).toBe(beforeJobs + 2);
  await expect(sync).toContainText('Syncing · cached mail may be stale');
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Newest local draft text');
  release();
  await expect(sync).toHaveText('Up to date');
  await page.locator('.delivery > summary').click();
  await expect(page.locator('.job > summary')).toHaveText('Delivered');
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await page.getByRole('button', {name: 'Inbox', exact: true}).click();
  await expect(page.locator('.message-row')).toContainText('Updated by final notification');
  const stable = {listCalls, jobCalls};
  await emit(page, ChangeKind.HEARTBEAT, 'final-no-more-events');
  await page.clock.runFor(10000);
  expect({listCalls, jobCalls}).toEqual(stable);
  expect(await page.evaluate(() => window.watchFixture.requests.length)).toBe(1);
  await context.setOffline(true);
  await expect(sync).toContainText('Offline · cached mail may be stale');
  await context.setOffline(false);
  await expect.poll(() => page.evaluate(() => window.watchFixture.requests.length)).toBe(2);
  await expect(sync).toContainText('Reconnecting · cached mail may be stale');
  await emit(page, ChangeKind.HEARTBEAT, 'final-no-more-events');
  await expect(sync).toHaveText('Up to date');
  await openDrafts(page);
  await page.locator('.draft-link').filter({hasText: 'Durable draft during failed refresh'}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Newest local draft text');
});
