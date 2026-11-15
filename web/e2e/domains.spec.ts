import { expect, test, type Page } from '@playwright/test';
import { fromBinary } from '@bufbuild/protobuf';
import { SubmitMessageRequestSchema, SubmitMessageResponseSchema } from '../src/gen/inbox/v1/inbox_pb';
import { openDrafts } from './helpers';

async function createDomain(page: Page, name: string) {
  const details = page.locator('.domains');
  if (!await details.evaluate(element => (element as HTMLDetailsElement).open)) await details.locator(':scope > summary').click();
  await details.getByLabel('Domain name', {exact: true}).fill(name);
  await details.getByRole('button', {name: 'Create domain', exact: true}).click();
  await expect(page.getByRole('form', {name: `Policy for ${name}`, exact: true})).toBeVisible();
}
async function policy(page: Page, name: string, receive: boolean, send: boolean) {
  const form = page.getByRole('form', {name: `Policy for ${name}`, exact: true});
  await form.getByLabel('Receive enabled', {exact: true}).setChecked(receive);
  await form.getByLabel('Send enabled', {exact: true}).setChecked(send);
  await form.getByRole('button', {name: 'Save policy', exact: true}).click();
  await expect(form.getByRole('button', {name: 'Save policy', exact: true})).toBeEnabled();
  await expect(form.getByLabel('Receive enabled', {exact: true})).toBeChecked({checked: receive});
  await expect(form.getByLabel('Send enabled', {exact: true})).toBeChecked({checked: send});
}
async function selectAddress(page: Page, address: string) {
  const selector = page.getByLabel('Mailbox', {exact: true});
  const option = selector.locator('option').filter({hasText: address});
  while (!await option.count() && await page.getByRole('button', {name: 'More mailboxes', exact: true}).count()) {
    const count = await selector.locator('option').count();
    await page.getByRole('button', {name: 'More mailboxes', exact: true}).click();
    await expect.poll(async () => await option.count() > 0 || await selector.locator('option').count() > count).toBe(true);
  }
  await expect(option).toHaveCount(1);
  await selector.selectOption((await option.getAttribute('value'))!);
}
async function createBox(page: Page, localPart: string, domain: string) {
  await page.getByRole('button', {name: 'Create mailbox', exact: true}).click();
  await page.getByLabel('Name', {exact: true}).fill('Synthetic duplicate local part');
  await page.getByLabel('Address local part').fill(localPart);
  await page.getByRole('combobox', {name: 'Mailbox domain', exact: true}).selectOption({label: domain});
  await page.getByRole('button', {name: 'Create', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Create', exact: true})).toHaveCount(0);
  await selectAddress(page, `${localPart}@${domain}`);
}
async function compose(page: Page, to: string, subject: string) {
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('To', {exact: true}).fill(to);
  await page.getByLabel('Subject', {exact: true}).fill(subject);
  await page.getByLabel('Message', {exact: true}).fill('Synthetic cross-domain local delivery');
}
test('real domains allow duplicate local parts, independent policy, local delivery, definitive rejection and offline draft preservation', async ({page, context}) => {
  await page.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  await page.goto('/');
  const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
  const first = `sender-${suffix}.test`, second = `recipient-${suffix}.test`, local = `team-${suffix}`;
  await createDomain(page, first);
  await createDomain(page, second);
  await policy(page, first, false, true);
  await policy(page, second, false, false);
  await createBox(page, local, first);
  await createBox(page, local, second);
  await policy(page, second, true, false);
  const sender = `${local}@${first}`, recipient = `${local}@${second}`;
  const subject = `Synthetic domain delivery ${suffix}`;
  await selectAddress(page, sender);
  await compose(page, recipient, subject);
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByRole('status')).toContainText('Message accepted');
  await selectAddress(page, recipient);
  await expect(page.locator('.message-row').filter({hasText: subject})).toHaveCount(1);
  await page.locator('.message-row').filter({hasText: subject}).click();
  await expect(page.locator('.body')).toHaveText('Synthetic cross-domain local delivery');
  await policy(page, second, false, false);
  // Existing mail remains readable after both flags are disabled.
  await expect(page.locator('.body')).toHaveText('Synthetic cross-domain local delivery');
  await selectAddress(page, sender);
  await compose(page, recipient, `Synthetic receive-disabled rejection ${suffix}`);
  await page.getByRole('button', {name: 'Send message', exact: true}).click();
  await expect(page.getByText('The server rejected the entire submission.', {exact: false})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toBeVisible();
  await page.getByRole('button', {name: 'Copy to new draft', exact: true}).click();
  await page.getByLabel('Subject', {exact: true}).fill(`Synthetic domain offline draft ${suffix}`);
  const savedSender = await page.getByRole('combobox', {name: 'From', exact: true}).inputValue();
  await policy(page, first, false, false);
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
  await expect(page.getByRole('combobox', {name: 'From', exact: true})).toHaveValue(savedSender);
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await page.evaluate(async () => {await navigator.serviceWorker.ready;});
  await expect.poll(() => page.evaluate(() => !!navigator.serviceWorker.controller)).toBe(true);
  await context.setOffline(true);
  await page.reload();
  await openDrafts(page);
  await page.locator('.draft-link').filter({hasText: `Synthetic domain offline draft ${suffix}`}).click();
  await expect(page.getByRole('combobox', {name: 'From', exact: true})).toHaveValue(savedSender);
  await page.getByLabel('Message', {exact: true}).fill('Offline edits survive policy refresh');
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await context.setOffline(false);
  await expect(page.getByRole('note', {name: 'Mail synchronization'})).toHaveText('Up to date');
  await expect(page.getByLabel('Message', {exact: true})).toHaveValue('Offline edits survive policy refresh');
  await expect(page.getByRole('combobox', {name: 'From', exact: true})).toHaveValue(savedSender);
  await expect(page.getByRole('button', {name: 'Send message', exact: true})).toBeDisabled();
});

test('real accepted identical retry survives disabled policy, lost confirmation and a live domain event without rewriting a locked draft', async ({page, browser, baseURL}) => {
  const writerContext = await browser.newContext({baseURL});
  const writer = await writerContext.newPage();
  for (const tab of [page, writer]) await tab.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  const attempts: ReturnType<typeof fromBinary<typeof SubmitMessageRequestSchema>>[] = [];
  const accepted: string[] = [];
  await page.route('**/inbox.v1.MailService/SubmitMessage', async route => {
    const raw = route.request().postDataBuffer()!;
    attempts.push(fromBinary(SubmitMessageRequestSchema, raw.subarray(5, 5 + raw.readUInt32BE(1))));
    const response = await route.fetch();
    const body = await response.body();
    const result = fromBinary(SubmitMessageResponseSchema, body.subarray(5, 5 + body.readUInt32BE(1)));
    accepted.push(result.submission!.id);
    if (attempts.length === 1) await route.abort('failed');
    else await route.fulfill({response});
  });
  try {
    const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
    const domain = `retry-${suffix}.test`, local = `team-${suffix}`, subject = `Synthetic accepted retry ${suffix}`;
    await page.goto('/');
    await createDomain(page, domain);
    await createBox(page, local, domain);
    await compose(page, `${local}@${domain}`, subject);
    await page.getByRole('button', {name: 'Send message', exact: true}).click();
    await expect(page.getByText('Submission unconfirmed.', {exact: false})).toBeVisible();
    await writer.goto('/');
    await writer.locator('.domains > summary').click();
    await policy(writer, domain, false, false);
    // This must arrive through DOMAIN reconciliation in the other context, not Refresh.
    const readerPolicy = page.getByRole('form', {name: `Policy for ${domain}`, exact: true});
    await expect(readerPolicy.getByLabel('Send enabled', {exact: true})).not.toBeChecked();
    await expect(readerPolicy.getByLabel('Receive enabled', {exact: true})).not.toBeChecked();
    await expect(page.getByRole('button', {name: 'Retry send safely', exact: true})).toBeEnabled();
    await expect(page.getByLabel('Message', {exact: true})).toBeDisabled();
    await expect(page.getByRole('button', {name: 'Copy to new draft', exact: true})).toHaveCount(0);
    await page.reload();
    await openDrafts(page);
    await page.locator('.draft-link').filter({hasText: subject}).click();
    await page.getByRole('button', {name: 'Retry send safely', exact: true}).click();
    await expect(page.getByRole('status')).toContainText('Message accepted');
    expect(attempts).toHaveLength(2);
    expect(attempts[1]).toEqual(attempts[0]);
    expect(accepted).toEqual([accepted[0], accepted[0]]);
    await selectAddress(page, `${local}@${domain}`);
    await page.getByRole('button', {name: 'Inbox', exact: true}).click();
    await expect(page.locator('.message-row').filter({hasText: subject})).toHaveCount(1);
  } finally {await writerContext.close();}
});
