export const logoutBarrierKey = 'inbox:logout-barrier:v1';
export const legacyLogoutKey = 'inbox:logout-pending';
type Marker = '0' | '1';
const valid = (value: unknown) => value === undefined || value === null || value === '0' || value === '1';

async function fallback(mode: 'read' | 'reserve' | Marker): Promise<Marker | undefined> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(`inbox:${location.origin}:logout-barrier`, 1);
    request.onupgradeneeded = () => request.result.createObjectStore('barrier');
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
    request.onblocked = () => reject(new Error('Sign-out storage unavailable'));
  });
  try {
    return await new Promise((resolve, reject) => {
      const tx = database.transaction('barrier', mode === 'read' ? 'readonly' : 'readwrite');
      const store = tx.objectStore('barrier');
      let result: Marker | undefined;
      const request = store.get('logout');
      request.onsuccess = () => {
        if (!valid(request.result) || request.result === null) {tx.abort(); return;}
        result = request.result;
        if (mode === 'reserve' && result === undefined) {result = '0'; store.put(result, 'logout');}
        else if (mode === '0' || mode === '1') {result = mode; store.put(result, 'logout');}
      };
      tx.oncomplete = () => resolve(result);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error ?? new Error('Invalid sign-out storage'));
    });
  } finally {database.close();}
}
function primary(): Marker | null {
  const value = localStorage.getItem(logoutBarrierKey);
  if (!valid(value)) throw new Error('Invalid sign-out storage');
  return value as Marker | null;
}
function writePrimary(value: Marker) {
  localStorage.setItem(logoutBarrierKey, value);
  if (primary() !== value) throw new Error('Sign-out storage did not persist');
}
export async function pendingLogout(): Promise<boolean> {
  const local = Promise.resolve().then(() => localStorage.getItem(legacyLogoutKey) === 'true' || primary() === '1');
  const durable = fallback('read');
  const [a, b] = await Promise.allSettled([local, durable]);
  if ((a.status === 'fulfilled' && a.value) || (b.status === 'fulfilled' && b.value === '1')) return true;
  if (a.status === 'rejected' || b.status === 'rejected') throw new Error('Sign-out storage unavailable');
  return false;
}
export async function reserveLogoutBarrier() {
  if (await pendingLogout()) throw new Error('Sign-out must complete first');
  // Allocate the actual one-character record before unlock; later state changes never grow it.
  if (primary() === null) writePrimary('0');
  else writePrimary(primary()!);
  if (await fallback('reserve') !== '0' || await pendingLogout()) throw new Error('Sign-out must complete first');
}
export async function persistLogout() {
  const results = await Promise.allSettled([
    Promise.resolve().then(() => writePrimary('1')),
    fallback('1'),
  ]);
  // A committed marker in either store blocks every subsequent unlock.
  if (results.every(result => result.status === 'rejected')) throw new Error('Sign-out could not be persisted');
}
export async function clearLogoutBarrier() {
  // Only call after server-confirmed revocation or an unauthenticated response.
  await fallback('0');
  writePrimary('0');
  localStorage.removeItem(legacyLogoutKey);
  if (await pendingLogout()) throw new Error('Sign-out storage remains locked');
}
