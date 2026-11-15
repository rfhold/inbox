import { expect, type Page } from '@playwright/test';
import { create, toBinary } from '@bufbuild/protobuf';
import { ListDomainsResponseSchema } from '../src/gen/inbox/v1/inbox_pb';

export const domainFields = {domainId: 'synthetic-domain', domain: 'inbox.test'};
export async function mockLocalSession(page: Pick<Page, 'route'>) {
  await page.route('**/auth/session', route => route.fulfill({json: {
    mode: 'disabled', authenticated: true, user_id: '00000000-0000-4000-8000-000000000001', administrator: true, expires_at: null,
  }}));
}
export const domainReply = () => toBinary(ListDomainsResponseSchema, create(ListDomainsResponseSchema, {
  domains: [{id: domainFields.domainId, name: domainFields.domain, receiveEnabled: true, sendEnabled: true}],
}));

export async function selectMailbox(page: Page, local: string) {
  const selector = page.getByLabel('Mailbox', {exact: true});
  const option = selector.locator('option').filter({hasText: `${local}@`});
  while (await option.count() === 0 && await page.getByRole('button', {name: 'More mailboxes', exact: true}).count()) {
    const count = await selector.locator('option').count();
    await page.getByRole('button', {name: 'More mailboxes', exact: true}).click();
    await expect.poll(async () => await option.count() > 0 || await selector.locator('option').count() > count,
      {message: 'Wait for either the requested mailbox or the next page; live refresh may replace the first page'}).toBe(true);
  }
  await expect(option).toHaveCount(1);
  const address = (await option.getAttribute('data-address'))!;
  await selector.selectOption((await option.getAttribute('value'))!);
  return address;
}

export function grpcFrame(payload: Uint8Array, status = 0) {
  const trailer = Buffer.from(`grpc-status: ${status}\r\n`);
  const data = Buffer.alloc(10 + payload.length + trailer.length);
  data.writeUInt32BE(payload.length, 1); data.set(payload, 5);
  data[5 + payload.length] = 128; data.writeUInt32BE(trailer.length, 6 + payload.length); data.set(trailer, 10 + payload.length);
  return data;
}

export async function openDrafts(page: Page) {
  await page.getByRole('button', {name: /^Drafts\s+\d+$/}).click();
}
