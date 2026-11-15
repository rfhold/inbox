const segment = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

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
