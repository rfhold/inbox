export async function claimCommandOwner(
  session: Pick<Storage, 'getItem' | 'setItem'>,
  locks: Pick<LockManager, 'request'>,
  lifetime: Promise<void> = new Promise(() => {}),
): Promise<string> {
  let candidate = session.getItem('inbox:command-tab') || crypto.randomUUID();
  for (;;) {
    const owner = candidate;
    const acquired = await new Promise<boolean>((resolve, reject) => {
      void locks.request(`inbox:commands:${owner}`, {ifAvailable: true}, async lock => {
        resolve(!!lock);
        if (lock) await lifetime;
      }).catch(reject);
    });
    if (acquired) {
      session.setItem('inbox:command-tab', owner);
      return owner;
    }
    candidate = crypto.randomUUID();
  }
}
