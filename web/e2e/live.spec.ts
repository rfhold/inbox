import { expect, test } from '@playwright/test';
import { selectMailbox } from './helpers';

test('two real browser contexts receive mailbox, delivery, read-state and reconnect changes without Refresh', async ({page: reader, browser, baseURL}) => {
  const writerContext = await browser.newContext({baseURL});
  const writer = await writerContext.newPage();
  const disabledTelemetry = {faro_url: '', environment: 'test', version: 'test'};
  await reader.route('**/config.json', route => route.fulfill({json: disabledTelemetry}));
  await writer.route('**/config.json', route => route.fulfill({json: disabledTelemetry}));
  try {
    await reader.goto('/');
    await expect.poll(() => reader.evaluate(() => new Promise<string>(resolve => {
      const request = indexedDB.open(`inbox:${location.origin}:shared`);
      request.onsuccess = () => {
        if (!request.result.objectStoreNames.contains('metadata')) {request.result.close(); resolve(''); return;}
        const tx = request.result.transaction('metadata');
        const state = tx.objectStore('metadata').get('cache');
        tx.oncomplete = () => {request.result.close(); resolve(state.result?.cursor ?? '');};
      };
    }))).not.toBe('');
    await writer.goto('/');
    const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
    const sender = `live-sender-${suffix}`;
    const recipient = `live-reader-${suffix}`;
    for (const local of [sender, recipient]) {
      await writer.getByRole('button', {name: 'Create mailbox', exact: true}).click();
      await writer.getByLabel('Name', {exact: true}).fill(`Synthetic ${local}`);
      await writer.getByLabel('Address local part').fill(local);
      await writer.getByRole('button', {name: 'Create', exact: true}).click();
      await expect(writer.getByRole('button', {name: 'Create', exact: true})).toHaveCount(0);
      await expect(reader.getByLabel('Mailbox', {exact: true}).locator('option').filter({hasText: `${local}@`})).toHaveCount(1);
    }
    const address = await selectMailbox(reader, recipient);
    await selectMailbox(writer, sender);
    const subject = `Synthetic live delivery ${suffix}`;
    async function send(subject: string) {
      await writer.getByRole('button', {name: 'Compose', exact: true}).click();
      await writer.getByLabel('To', {exact: true}).fill(address);
      await writer.getByLabel('Subject', {exact: true}).fill(subject);
      await writer.getByLabel('Message', {exact: true}).fill('Synthetic notification-driven body');
      await writer.getByRole('button', {name: 'Send message', exact: true}).click();
      await expect(writer.getByRole('status')).toContainText('Message accepted');
    }
    await send(subject);
    await expect(reader.locator('.message-row').filter({hasText: subject})).toBeVisible();
    await reader.locator('.message-row').filter({hasText: subject}).click();
    await expect(reader.locator('.body')).toContainText('Synthetic notification-driven body');
    await expect(reader.getByRole('button', {name: 'Mark read', exact: true})).toBeVisible();
    await selectMailbox(writer, recipient);
    await writer.locator('.message-row').filter({hasText: subject}).click();
    await writer.getByRole('button', {name: 'Mark read', exact: true}).click();
    await expect(reader.getByRole('button', {name: 'Mark unread', exact: true})).toBeVisible();
    await writer.locator('.delivery > summary').click();
    await expect(writer.locator('.job > summary').filter({hasText: /^Delivered$/})).toHaveCount(1);
    await reader.getByRole('button', {name: 'Compose', exact: true}).click();
    await reader.getByLabel('Subject', {exact: true}).fill('Synthetic local draft across live reconnect');
    await reader.getByLabel('Message', {exact: true}).fill('Never replace local draft edits');
    await expect(reader.locator('.composer .muted')).toHaveText('Saved on this device');
    await reader.context().setOffline(true);
    await selectMailbox(writer, sender);
    const missedSubject = `Synthetic missed live delivery ${suffix}`;
    await send(missedSubject);
    await reader.context().setOffline(false);
    await expect(reader.getByLabel('Message', {exact: true})).toHaveValue('Never replace local draft edits');
    await reader.getByRole('button', {name: 'Close', exact: true}).click();
    await reader.getByRole('button', {name: 'Inbox', exact: true}).click();
    await expect(reader.locator('.message-row').filter({hasText: missedSubject})).toBeVisible();
  } finally {await writerContext.close();}
});
