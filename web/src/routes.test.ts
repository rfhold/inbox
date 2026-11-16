import {describe, expect, it} from 'vitest';
import {mailPath, readRoute} from './routes';

describe('mail URLs', () => {
  const entry = '00000000-0000-4000-8000-000000000001';
  it('round trips an email, folder and entry without UUID mailbox aliases', () => {
    for (const folder of ['inbox', 'sent', 'drafts'] as const) {
      const url = mailPath('synthetic+tag@inbox.test', folder, entry);
      expect(url).toContain('synthetic+tag@inbox.test');
      expect(readRoute(url)).toEqual({page: 'mail', address: 'synthetic+tag@inbox.test', folder, entry});
    }
    expect(readRoute('/domains')).toEqual({page: 'domains'});
    expect(readRoute('/')).toEqual({page: 'home'});
  });
  it('rejects malformed addresses, entries, folders and old UUID identifiers', () => {
    for (const path of [
      `/mail/${entry}/inbox`, '/mail/Upper@inbox.test/inbox', '/mail/a@inbox.test/unknown',
      '/mail/a%2Fb@inbox.test/inbox', '/mail/%/inbox', '/mail/a@inbox.test/inbox/not-an-entry',
      '/mail/a@inbox.test/inbox/', '/mail/a@inbox.test/inbox/00000000---------------------------',
    ]) expect(readRoute(path)).toEqual({page: 'missing'});
  });
});
