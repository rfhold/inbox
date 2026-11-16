const segment = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

export function validateNatsCredentials(value: string): string {
  if (Buffer.byteLength(value) > 16_384 || /\x00/.test(value) ||
      !/-----BEGIN NATS USER JWT-----\s+[A-Za-z0-9_.-]+\s+------END NATS USER JWT------/.test(value) ||
      !/-----BEGIN USER NKEY SEED-----\s+SU[A-Z2-7]+\s+------END USER NKEY SEED------/.test(value)) {
    throw new Error("Invalid NATS credentials");
  }
  return value;
}

export function natsAuthenticationEnv<T>(
  stack: string, allowAnonymous = false, user?: T, password?: T, credentials?: T,
): Record<string, T | string> {
  if (allowAnonymous) {
    if (stack !== "preview") throw new Error("allowAnonymousNats is allowed only on the preview stack");
    if (user || password || credentials) throw new Error("allowAnonymousNats cannot be combined with NATS credentials");
    return {};
  }
  if ((!!user !== !!password) || (!!credentials === !!user)) {
    throw new Error("configure exactly natsUser+natsPassword or secret natsCredentials; anonymous NATS requires preview allowAnonymousNats");
  }
  return credentials
    ? { INBOX_NATS_CREDENTIALS_FILE: "/var/run/secrets/inbox/nats/nats.creds" }
    : { INBOX_NATS_USER: user!, INBOX_NATS_PASSWORD: password! };
}

export function requireImmutableImage(value: string): string {
  if (!/^[^\s@]+@sha256:[0-9a-f]{64}$/.test(value)) {
    throw new Error("image must use an immutable sha256 digest");
  }
  return value;
}

export function validateSegment(value: string, name: string): string {
  if (!segment.test(value)) throw new Error(`${name} must be a DNS label`);
  return value;
}

export function validateHostname(value: string): string {
  if (value.length > 253 || !value.includes(".") || value.split(".").some((part) => !segment.test(part))) {
    throw new Error("hostname must be a fully qualified DNS name");
  }
  return value;
}

export function validateHttpsOrigin(value: string, name: string): string {
  const url = new URL(value);
  if (url.protocol !== "https:" || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
    throw new Error(`${name} must be a credential-free HTTPS origin`);
  }
  return url.origin;
}

export function validateWrappingKeys(versions: unknown, active: string): asserts versions is string[] {
  if (!Array.isArray(versions) || versions.length === 0 || versions.length > 32 ||
      versions.some((v) => typeof v !== "string" || !segment.test(v)) ||
      new Set(versions).size !== versions.length || !versions.includes(active)) {
    throw new Error("oauthWrappingKeyVersions must contain 1 to 32 unique DNS labels including oauthActiveWrappingKeyVersion");
  }
}

export function validateEnvironment(stack: string, namespace: string, hostname: string, slug: string, natsNamespace: string, protect: boolean): void {
  const expected = stack === "preview"
    ? ["inbox-preview", "preview-inbox.holdenitdown.net", "inbox-preview", "INBOX_PREVIEW"]
    : stack === "prod" ? ["inbox", "inbox.holdenitdown.net", "inbox", "INBOX_PROD"] : undefined;
  if (!expected || [namespace, hostname, slug, natsNamespace].some((v, i) => v !== expected[i]) ||
      (stack === "prod" && !protect)) {
    throw new Error("stack must preserve the fixed preview/prod identity, NATS namespace, and production protection");
  }
}
