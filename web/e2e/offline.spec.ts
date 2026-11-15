import { test, expect } from '@playwright/test';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { ListMailboxesResponseSchema, ListMessagesResponseSchema, GetMessageResponseSchema, SubmitMessageRequestSchema, SubmitMessageResponseSchema, SubmissionStatus, Folder } from '../src/gen/inbox/v1/inbox_pb';
import { openDrafts, grpcFrame as frame, domainReply, domainFields } from './helpers';
import { mockLocalSession } from './helpers';
test.beforeEach(async ({page}) => {await mockLocalSession(page);});

test('clicking the active sidebar draft before autosave preserves the newest edits', async ({page}) => {
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = new URL(route.request().url()).pathname.split('/').pop();
    if (method === 'WatchChanges') return route.abort();
    const payload = method === 'ListDomains' ? domainReply() : method === 'ListMailboxes'
      ? toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...domainFields, id: 'draft-regression-box', name: 'Synthetic drafts', address: 'drafts@inbox.test', localPart: 'drafts'}]}))
      : toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {}));
    await route.fulfill({body: frame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('Subject', {exact: true}).fill('Synthetic sidebar regression');
  await page.getByLabel('Message', {exact: true}).fill('Previously saved body');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await page.clock.install();
  await page.clock.pauseAt(new Date());
  await page.getByLabel('Message', {exact: true}).fill('Newest body before debounce');
  await expect(page.locator('.composer .muted')).toHaveText('Saving draft…');
  await page.getByRole('button', {name: /Synthetic sidebar regression/}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Newest body before debounce');
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await page.clock.resume();
  await page.reload();
  await openDrafts(page);
  await page.getByRole('button', {name: /Synthetic sidebar regression/}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Newest body before debounce');
});

for (const rejectionCode of [3, 9]) test(`authoritative rejection (${rejectionCode}) expires before an accepted retry loses its response`, async ({page}) => {
  const submitted: ReturnType<typeof fromBinary<typeof SubmitMessageRequestSchema>>[] = [];
  const deliveries = new Set<string>();
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = new URL(route.request().url()).pathname.split('/').pop();
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = domainReply();
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...domainFields, id: 'synthetic-retry-box', name: 'Synthetic retry sender', address: 'retry-sender@inbox.test', localPart: 'retry-sender'}]}));
    else if (method === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {}));
    else if (method === 'SubmitMessage') {
      const raw = route.request().postDataBuffer()!;
      const request = fromBinary(SubmitMessageRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1)));
      submitted.push(request);
      if (submitted.length === 1) return route.fulfill({body: frame(toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {})), rejectionCode), contentType: 'application/grpc-web+proto'});
      deliveries.add(request.idempotencyKey);
      if (submitted.length === 2) return route.abort('failed');
      payload = toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {submission: {id: 'synthetic-confirmed-retry', status: SubmissionStatus.SUCCEEDED}}));
    } else return route.abort();
    await route.fulfill({body: frame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill('initially-unknown@inbox.test');
  await page.getByLabel('Subject', {exact: true}).fill('Synthetic rejection transition');
  await page.getByLabel('Message', {exact: true}).fill('Preserve this immutable retry payload.');
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByText('The server rejected the entire submission.', {exact: false})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Discard draft', exact: true})).toBeVisible();
  expect(deliveries.size).toBe(0);
  await page.getByRole('button', {name: 'Retry send safely', exact: true}).click();
  await expect(page.getByText('Submission unconfirmed.', {exact: false})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Discard draft', exact: true})).toHaveCount(0);
  expect(deliveries.size).toBe(1);
  expect(submitted).toHaveLength(2);
  expect(submitted[1]).toEqual(submitted[0]);
  await page.reload();
  await openDrafts(page);
  await page.locator('.draft-link').filter({hasText: 'Synthetic rejection transition'}).click();
  await expect(page.getByLabel('Message', {exact: true})).toBeDisabled();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Discard draft', exact: true})).toHaveCount(0);
  expect(submitted).toHaveLength(2);
  await page.getByRole('button', {name: 'Retry send safely', exact: true}).click();
  await expect(page.getByRole('status')).toContainText('Message accepted');
  expect(submitted).toHaveLength(3);
  expect(submitted[2]).toEqual(submitted[0]);
  expect(deliveries.size).toBe(1);
  await openDrafts(page);
  await expect(page.locator('.draft-link').filter({hasText: 'Synthetic rejection transition'})).toHaveCount(0);
});

test('production shell, cached plaintext and durable drafts survive actual offline reload', async ({page, context}) => {
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const entry = {id: 'synthetic-entry', mailboxId: 'synthetic-box', messageId: 'synthetic-message', folder: Folder.INBOX, fromAddress: 'sender@inbox.test', to: ['reader@inbox.test'], subject: 'Cached synthetic subject', preview: 'Cached plaintext', createdAt: '2026-01-01T00:00:00Z'};
  const replies: Record<string, Uint8Array> = {
    ListDomains: domainReply(),
    ListMailboxes: toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...domainFields, id: 'synthetic-box', name: 'Offline reader', address: 'reader@inbox.test', localPart: 'reader'}]})),
    ListMessages: toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {entries: [entry]})),
    GetMessage: toBinary(GetMessageResponseSchema, create(GetMessageResponseSchema, {message: {entry, textBody: 'Cached synthetic plaintext body', replyToAddress: 'preferred@inbox.test'}})),
  };
  await page.route('**/inbox.v1.MailService/*', async route => {
    const payload = replies[new URL(route.request().url()).pathname.split('/').pop()!];
    if (!payload) return route.abort();
    await route.fulfill({body: frame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.evaluate(async () => {await navigator.serviceWorker.ready;});
  await expect.poll(() => page.evaluate(() => !!navigator.serviceWorker.controller)).toBe(true);
  await page.locator('.message-row').click();
  await expect(page.locator('.body')).toHaveText('Cached synthetic plaintext body');
  await page.getByRole('button', {name: 'Reply', exact: true}).click();
  await expect(page.getByLabel('To', {exact: true})).toHaveValue('preferred@inbox.test');
  await page.getByRole('button', {name: 'Discard draft', exact: true}).click();
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill('sender@inbox.test');
  await page.getByLabel('Subject', {exact: true}).fill('Durable synthetic offline draft');
  await page.getByLabel('Message', {exact: true}).fill('This draft survives reload.');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await context.setOffline(true);
  await page.reload();
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText(/Offline.*cached mail may be stale/);
  await openDrafts(page);
  await page.getByRole('button', {name: /Durable synthetic offline draft/}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('This draft survives reload.');
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await page.getByRole('button', {name: 'Inbox', exact: true}).click();
  await page.locator('.message-row').click();
  await expect(page.locator('.body')).toHaveText('Cached synthetic plaintext body');
  const cached = await page.evaluate(async () => Promise.all((await caches.keys()).map(async key => (await (await caches.open(key)).keys()).map(request => new URL(request.url).pathname))));
  expect(cached.flat().some(path => path.includes('MailService') || path.includes('config') || path.includes('/auth/') || path.includes('/oauth'))).toBe(false);
  await page.getByRole('button', {name: 'Sent', exact: true}).click();
  await expect(page.getByRole('alert').first()).toContainText('Connect to load this view');
  await page.getByText('Device storage', {exact: true}).click();
  await page.getByRole('button', {name: 'Clear cached mail', exact: true}).click();
  await expect(page.getByRole('alert').first()).toContainText('Connect to load this view');
  await openDrafts(page);
  await expect(page.getByRole('button', {name: /Durable synthetic offline draft/})).toBeVisible();
});

test('uncertain send retains immutable key across reload; local Faro capture excludes content and IDs', async ({page}) => {
  const collected: {body: string; headers: Record<string,string>}[] = [];
  const submitted: ReturnType<typeof fromBinary<typeof SubmitMessageRequestSchema>>[] = [];
  const traceHeaders: string[] = [];
  const marker = 'SYNTHETIC_PRIVATE_SENTINEL';
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '/local-test-collector', environment: 'test', version: 'test'}}));
  await page.route('**/local-test-collector', async route => {
    collected.push({body: route.request().postData() || '', headers: await route.request().allHeaders()});
    await route.fulfill({status: 202, body: ''});
  });
  await page.route('**/inbox.v1.MailService/*', async route => {
    const name = new URL(route.request().url()).pathname.split('/').pop();
    const headers = await route.request().allHeaders();
    if (headers.traceparent) traceHeaders.push(headers.traceparent);
    let payload: Uint8Array;
    if (name === 'ListDomains') payload = domainReply();
    else if (name === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...domainFields, id: 'PRIVATE_BOX_ID_SENTINEL', name: 'Synthetic sender', address: 'private-synthetic@inbox.test', localPart: 'private-synthetic'}]}));
    else if (name === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {}));
    else if (name === 'SubmitMessage') {
      const raw = route.request().postDataBuffer()!;
      submitted.push(fromBinary(SubmitMessageRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1))));
      if (submitted.length === 1) return route.abort('failed');
      payload = toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {submission: {id: 'PRIVATE_JOB_ID_SENTINEL', status: SubmissionStatus.SUCCEEDED}}));
    } else return route.abort();
    await route.fulfill({body: frame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto(`/?private=${marker}`);
  await expect.poll(() => collected.some(item => item.body.includes('application_started'))).toBe(true);
  await page.evaluate(value => {console.error(value); window.dispatchEvent(new ErrorEvent('error', {message: value, error: new Error(value)}));}, marker);
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill('recipient-synthetic@inbox.test');
  await page.getByLabel('Subject', {exact: true}).fill(marker);
  await page.getByLabel('Message', {exact: true}).fill(marker);
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByText('Submission unconfirmed.', {exact: false})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Discard draft', exact: true})).toHaveCount(0);
  expect(submitted).toHaveLength(1);
  await expect.poll(() => collected.some(item => item.body.includes('application_error'))).toBe(true);
  await page.reload();
  await openDrafts(page);
  await page.getByRole('button', {name: new RegExp(marker)}).click();
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue(marker);
  await expect(page.getByLabel('Message', {exact: true})).toBeDisabled();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Discard draft', exact: true})).toHaveCount(0);
  expect(submitted).toHaveLength(1);
  await page.getByRole('button', {name: 'Retry send safely', exact: true}).click();
  await expect(page.getByRole('status')).toContainText('Message accepted');
  expect(submitted).toHaveLength(2);
  expect(submitted[1]).toEqual(submitted[0]);
  await expect(page.getByRole('button', {name: new RegExp(marker)})).toHaveCount(0);
  await expect.poll(() => collected.some(item => item.body.includes('application_error'))).toBe(true);
  await expect.poll(() => collected.some(item => item.body.includes('MailService/SubmitMessage'))).toBe(true);
  expect(traceHeaders.some(value => /^00-[a-f0-9]{32}-[a-f0-9]{16}-01$/.test(value))).toBe(true);
  const body = collected.map(item => item.body).join('\n');
  for (const secret of [marker, domainFields.domainId, 'private-synthetic@inbox.test', 'recipient-synthetic@inbox.test', 'PRIVATE_BOX_ID_SENTINEL', 'PRIVATE_JOB_ID_SENTINEL', submitted[0].idempotencyKey]) expect(body).not.toContain(secret);
  for (const item of collected) {
    expect(item.headers['idempotency-key']).toBeUndefined();
    expect(item.headers.traceparent).toBeUndefined();
    const parsed = JSON.parse(item.body);
    expect(parsed.meta.page.url).toBe('/');
    expect(parsed.meta.user).toBeUndefined();
    expect(parsed.meta.session).toBeUndefined();
    expect(parsed.meta.browser).toBeUndefined();
  }
});
