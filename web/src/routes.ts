export type MailFolder = 'inbox' | 'sent' | 'drafts';
export type MailRoute = {page: 'mail'; address: string; folder: MailFolder; entry?: string} | {page: 'domains' | 'home' | 'missing'};
export function mailPath(address: string, folder: MailFolder = 'inbox', entry?: string) {
  return `/mail/${encodeURIComponent(address).replace(/%40/g, '@').replace(/%2B/g, '+')}/${folder}${entry ? `/${encodeURIComponent(entry)}` : ''}`;
}
export function readRoute(path: string): MailRoute {
  if (path === '/') return {page: 'home'};
  if (path === '/domains') return {page: 'domains'};
  try {
    const parts = path.split('/');
    const address = decodeURIComponent(parts[2] || '');
    if (parts[1] !== 'mail' || ![4, 5].includes(parts.length) ||
        !/^[a-z0-9._+-]+@[a-z0-9.-]+$/.test(address) || !['inbox', 'sent', 'drafts'].includes(parts[3]) ||
        (parts.length === 5 && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(parts[4]))) return {page: 'missing'};
    return {page: 'mail', address, folder: parts[3] as MailFolder, entry: parts[4]};
  } catch { return {page: 'missing'}; }
}
