import {test, expect} from '@playwright/test';
import {create, toBinary} from '@bufbuild/protobuf';
import {ListMailboxesResponseSchema, ListMessagesResponseSchema, GetMessageResponseSchema, SubmitMessageResponseSchema, GetSubmissionResponseSchema, ChangeEventSchema, ChangeKind} from '../src/gen/inbox/v1/inbox_pb';
import {domainFields, domainReply, grpcFrame, mockLocalSession} from './helpers';

const entry = '00000000-0000-4000-8000-000000000001';
test.beforeEach(async ({page}) => {
  await mockLocalSession(page);
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: ''}}));
  await page.route('**/inbox.v1.MailService/*', async route => {
    const method = new URL(route.request().url()).pathname.split('/').pop();
    const summary = {id: entry, mailboxId: 'box-two', folder: 1, subject: 'Synthetic URL message', fromAddress: 'sender@inbox.test'};
    let payload: Uint8Array;
    if (method === 'ListDomains') payload = domainReply();
    else if (method === 'ListMailboxes') payload = toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {
      mailboxes: ['one', 'two'].map(value => ({...domainFields, id: `box-${value}`, name: value, localPart: value, address: `${value}@inbox.test`})),
    }));
    else if (method === 'ListMessages') payload = toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {entries: [summary]}));
    else if (method === 'GetMessage') payload = toBinary(GetMessageResponseSchema, create(GetMessageResponseSchema, {message: {entry: summary, textBody: 'Synthetic body'}}));
    else if (method === 'SubmitMessage') payload = toBinary(SubmitMessageResponseSchema, create(SubmitMessageResponseSchema, {
      submission: {id: entry, status: 1, recipients: [{address: 'recipient@inbox.test', status: 1}]},
    }));
    else if (method === 'GetSubmission') payload = toBinary(GetSubmissionResponseSchema, create(GetSubmissionResponseSchema, {
      submission: {id: entry, status: 2, recipients: [{address: 'recipient@inbox.test', status: 2}]},
    }));
    else return route.abort();
    await route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
});

test('mailbox, detail, history and reload follow email URLs; domains is a separate page', async ({page}) => {
  await page.goto('/mail/two@inbox.test/inbox');
  await expect(page.getByLabel('Mailbox', {exact: true})).toHaveValue('box-two');
  await page.locator('.message-row').first().click();
  await expect(page).toHaveURL(`/mail/two@inbox.test/inbox/${entry}`);
  await expect(page.locator('.body')).toContainText('Synthetic body');
  await page.reload();
  await expect(page.getByLabel('Mailbox', {exact: true})).toHaveValue('box-two');
  await expect(page.locator('.body')).toContainText('Synthetic body');
  await page.getByRole('button', {name: 'Domains', exact: true}).click();
  await expect(page).toHaveURL('/domains');
  await expect(page.getByRole('heading', {name: 'Domains', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Sent', exact: true}).click();
  await expect(page).toHaveURL('/mail/two@inbox.test/sent');
  await page.goBack();
  await expect(page).toHaveURL('/domains');
  await page.goBack();
  await expect(page).toHaveURL(`/mail/two@inbox.test/inbox/${entry}`);
});

test('drafts survive URL navigation and reopen after reload', async ({page}) => {
  await page.goto('/mail/two@inbox.test/inbox');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await expect(page).toHaveURL(/\/mail\/two@inbox.test\/drafts\/[a-f0-9-]+$/);
  await page.getByLabel('Subject', {exact: true}).fill('Synthetic retained draft');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await page.reload();
  await expect(page.getByLabel('Subject', {exact: true})).toHaveValue('Synthetic retained draft');
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await expect(page).toHaveURL('/mail/two@inbox.test/drafts');
  await expect(page.locator('.composer')).toHaveCount(0);
});

test('the cached shell supports mailbox and domains deep links offline', async ({page, context}) => {
  await page.goto('/mail/two@inbox.test/inbox');
  await expect(page.locator('.message-row')).toHaveCount(1);
  await page.evaluate(async () => {await navigator.serviceWorker.ready;});
  await expect.poll(() => page.evaluate(() => !!navigator.serviceWorker.controller)).toBe(true);
  await context.setOffline(true);
  await page.reload();
  await expect(page.getByLabel('Mailbox', {exact: true})).toHaveValue('box-two');
  await page.getByRole('button', {name: 'Domains', exact: true}).click();
  await page.reload();
  await expect(page.getByRole('heading', {name: 'Domains', exact: true})).toBeVisible();
});

test('unknown email and UUID mailbox routes never select a different mailbox', async ({page}) => {
  await page.goto('/mail/unknown@inbox.test/inbox');
  await expect(page.getByRole('alert')).toContainText('Mailbox');
  await page.goto(`/mail/${entry}/inbox`);
  await expect(page.getByRole('heading', {name: 'Page not found', exact: true})).toBeVisible();
});

test('delivery updates use dismissible toasts and persistent notifications, not a Delivery section', async ({page}) => {
  const frame = (kind: ChangeKind) => {
    const payload = toBinary(ChangeEventSchema, create(ChangeEventSchema, {kind, submissionId: entry, resumeToken: `synthetic-${kind}`}));
    const bytes = new Uint8Array(5 + payload.length);
    new DataView(bytes.buffer).setUint32(1, payload.length); bytes.set(payload, 5);
    return [...bytes];
  };
  await page.addInitScript(initial => {
    const original = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = input instanceof Request ? input.url : String(input);
      if (!url.endsWith('/inbox.v1.MailService/WatchChanges')) return original(input, init);
      let receive: (event: Event) => void;
      const body = new ReadableStream<Uint8Array>({start(controller) {
        receive = event => controller.enqueue(new Uint8Array((event as CustomEvent<number[]>).detail));
        window.addEventListener('ux-change', receive);
        controller.enqueue(new Uint8Array(initial));
        init?.signal?.addEventListener('abort', () => {
          window.removeEventListener('ux-change', receive);
          try {controller.error(new DOMException('Aborted', 'AbortError'));} catch {}
        }, {once: true});
      }});
      return new Response(body, {headers: {'content-type': 'application/grpc-web+proto'}});
    };
  }, frame(ChangeKind.HEARTBEAT));
  await page.goto('/mail/two@inbox.test/inbox');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill('recipient@inbox.test');
  await page.getByLabel('Subject', {exact: true}).fill('Synthetic notification');
  await page.getByLabel('Message', {exact: true}).fill('Synthetic body');
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.locator('.toast-stack')).toContainText('Delivery pending');
  await page.evaluate(bytes => window.dispatchEvent(new CustomEvent('ux-change', {detail: bytes})), frame(ChangeKind.SUBMISSION));
  await expect(page.locator('.toast-stack')).toContainText('Message delivered');
  await page.getByRole('button', {name: 'Dismiss delivery toast'}).click();
  await expect(page.locator('.toast-stack .toast')).toHaveCount(0);
  await page.locator('.notifications summary').click();
  await expect(page.getByRole('region', {name: 'Notifications'})).toContainText('Message delivered');
  await page.getByRole('region', {name: 'Notifications'}).getByRole('button', {name: 'Dismiss notification', exact: true}).click();
  await expect(page.getByRole('region', {name: 'Notifications'})).toContainText('No notifications.');
  await page.reload();
  await page.locator('.notifications summary').click();
  await expect(page.getByRole('region', {name: 'Notifications'})).toContainText('No notifications.');
  await expect(page.locator('.delivery')).toHaveCount(0);
});
