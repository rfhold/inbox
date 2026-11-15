import { expect, test } from '@playwright/test';
import { create, toBinary } from '@bufbuild/protobuf';
import { ListMailboxesResponseSchema, ListMessagesResponseSchema } from '../src/gen/inbox/v1/inbox_pb';
import { domainFields, domainReply, grpcFrame, openDrafts } from './helpers';

test('account changes, offline locking, logout and other tabs never expose another account draft', async ({page, context}) => {
  const A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  const B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  let principal: string | undefined = A;
  await context.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  await context.route('**/auth/session', route => principal ? route.fulfill({json: {
    mode: 'oidc', authenticated: true, user_id: principal, administrator: false, expires_at: new Date(Date.now() + 60000).toISOString(),
  }}) : route.fulfill({status: 401, json: {mode: 'oidc', authenticated: false}}));
  await context.route('**/auth/logout', route => {principal = undefined; return route.fulfill({status: 204});});
  await context.route('**/inbox.v1.MailService/*', async route => {
    if (route.request().headers()['x-inbox-expected-user'] !== principal) return route.fulfill({status: 401});
    const method = route.request().url().split('/').pop();
    if (method === 'WatchChanges') return route.abort();
    const payload = method === 'ListDomains' ? domainReply() : method === 'ListMailboxes'
      ? toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {mailboxes: [{...domainFields, id: `${principal}-box`, address: 'me@inbox.test', localPart: 'me', name: 'Personal'}]}))
      : toBinary(ListMessagesResponseSchema, create(ListMessagesResponseSchema, {}));
    return route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Compose', exact: true}).click();
  await page.getByLabel('Subject', {exact: true}).fill('Private draft A');
  await expect(page.locator('.composer .muted')).toHaveText('Saved on this device');
  await page.getByRole('button', {name: 'Close', exact: true}).click();
  await page.locator('.domains > summary').click();
  await expect(page.getByRole('button', {name: 'Create domain', exact: true})).toHaveCount(0);
  await expect(page.getByLabel('Domain name', {exact: true})).toHaveCount(0);
  await context.setOffline(true);
  await openDrafts(page);
  await expect(page.getByText('Private draft A', {exact: true})).toBeVisible();
  await page.reload();
  await expect(page.getByRole('button', {name: 'Check session'})).toBeVisible();
  await expect(page.getByText('Private draft A', {exact: true})).toHaveCount(0);
  principal = B; await context.setOffline(false);
  await expect(page.getByRole('button', {name: 'Compose', exact: true})).toBeEnabled();
  await openDrafts(page);
  await expect(page.getByText('Private draft A', {exact: true})).toHaveCount(0);
  const other = await context.newPage(); await other.goto('/');
  await expect(other.getByRole('button', {name: 'Compose', exact: true})).toBeEnabled();
  await other.getByRole('button', {name: 'Sign out', exact: true}).click();
  await expect(page.getByRole('button', {name: 'Check session'})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Compose', exact: true})).toHaveCount(0);
  await expect.poll(() => other.evaluate(() => localStorage.getItem('inbox:logout-barrier:v1'))).toBe('0');
  await other.close();
  await page.reload();
  await expect(page.getByRole('button', {name: 'Check session'})).toBeVisible();
  await expect.poll(() => page.evaluate(() => localStorage.getItem('inbox:logout-barrier:v1'))).toBe('0');
  principal = A;
  await page.reload();
  await openDrafts(page);
  await expect(page.getByText('Private draft A', {exact: true})).toBeVisible();
});

test('a failed primary logout-marker write uses durable fallback across reload despite a still-valid cookie', async ({page, context}) => {
  const A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  let allowLogout = false;
  let revoked = false;
  let probes = 0;
  await context.addInitScript(() => {
    const original = Storage.prototype.setItem;
    original.call(localStorage, 'inbox:privacy-check', '1');
    localStorage.removeItem('inbox:privacy-check');
    Storage.prototype.setItem = function(key, value) {
      if (key === 'inbox:logout-barrier:v1' && value === '1') throw new DOMException('Synthetic quota failure', 'QuotaExceededError');
      return original.call(this, key, value);
    };
  });
  await context.route('**/config.json', route => route.fulfill({json: {faro_url: '', environment: 'test', version: 'test'}}));
  await context.route('**/auth/session', route => {
    probes++;
    return revoked ? route.fulfill({status: 401}) : route.fulfill({json: {
      mode: 'oidc', authenticated: true, user_id: A, administrator: false, expires_at: new Date(Date.now() + 60000).toISOString(),
    }});
  });
  await context.route('**/auth/logout', route => {
    if (!allowLogout) return route.abort('failed');
    revoked = true; return route.fulfill({status: 204});
  });
  await context.route('**/inbox.v1.MailService/*', route => {
    if (route.request().headers()['x-inbox-expected-user'] !== A) return route.fulfill({status: 401});
    if (route.request().url().endsWith('/WatchChanges')) return route.abort();
    const payload = route.request().url().endsWith('/ListDomains') ? domainReply()
      : toBinary(ListMailboxesResponseSchema, create(ListMailboxesResponseSchema, {}));
    return route.fulfill({body: grpcFrame(payload), contentType: 'application/grpc-web+proto'});
  });
  await page.goto('/');
  await page.getByRole('button', {name: 'Sign out', exact: true}).click();
  await expect(page.getByText('Unable to verify your session. Connect and try again.', {exact: true})).toBeVisible();
  expect(await page.evaluate(() => localStorage.getItem('inbox:logout-barrier:v1'))).toBe('0');
  const initialProbes = probes;
  await page.reload();
  await expect(page.getByText('Unable to verify your session. Connect and try again.', {exact: true})).toBeVisible();
  await expect(page.getByRole('button', {name: 'Sign out', exact: true})).toHaveCount(0);
  expect(probes).toBe(initialProbes);
  allowLogout = true;
  await page.getByRole('button', {name: 'Check session'}).click();
  await expect(page.getByText('Sign in to open your personal workspace.', {exact: true})).toBeVisible();
  expect(revoked).toBe(true);
  expect(probes).toBe(initialProbes);
});
