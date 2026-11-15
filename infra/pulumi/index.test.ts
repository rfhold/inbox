import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { after, before, test } from "node:test";
import * as pulumi from "@pulumi/pulumi";
import { requireImmutableImage, validateEnvironment, validateHttpsOrigin, validateWrappingKeys } from "./policy";

interface Record { type: string; name: string; inputs: any; }
const resources: Record[] = [];
const previous = process.env.PULUMI_CONFIG;
const unwrap = (value: any): any => {
  if (value && typeof value === "object" && value["4dabf18193072939515e22adb298388d"]) return unwrap(value.value);
  if (Array.isArray(value)) return value.map(unwrap);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, unwrap(v)]));
  return value;
};
const resource = (name: string) => {
  const found = resources.find((r) => r.name === name);
  assert.ok(found, `missing resource ${name}`);
  return found.inputs;
};
before(async () => {
  process.env.PULUMI_CONFIG = JSON.stringify({
    "inbox:namespace": "inbox-preview", "inbox:hostname": "preview-inbox.holdenitdown.net",
    "inbox:displayName": "Inbox Preview", "inbox:slug": "inbox-preview",
    "inbox:image": `registry.example.test/inbox@sha256:${"a".repeat(64)}`,
    "inbox:authentikBaseUrl": "https://auth.example.test",
    "inbox:s3Endpoint": "https://s3.example.test", "inbox:backupEndpoint": "https://backup.example.test",
    "inbox:storageClass": "shared-fs", "inbox:bucketStorageClass": "default-bucket",
    "inbox:backupStorageClass": "default-bucket", "inbox:backupRetention": "14d",
    "inbox:databaseStorageSize": "10Gi", "inbox:protectData": "false",
    "inbox:adminGroup": "test-domain-admins", "inbox:bootstrapDomain": "mail.example.test",
    "inbox:natsNamespace": "INBOX_PREVIEW", "inbox:natsUser": "test-user", "inbox:natsPassword": "test-only",
    "inbox:oauthWrappingKeyVersions": '["v1","v2"]', "inbox:oauthActiveWrappingKeyVersion": "v2",
  });
  pulumi.runtime.setMocks({
    newResource: (args) => {
      resources.push({ type: args.type, name: args.name, inputs: unwrap(args.inputs) });
      const state: any = { ...args.inputs };
      if (args.type === "random:index/randomBytes:RandomBytes") state.base64 = Buffer.alloc(32).toString("base64");
      if (args.type === "random:index/randomPassword:RandomPassword") state.result = "mock-only-client-secret";
      if (args.type === "tls:index/privateKey:PrivateKey") state.privateKeyPem = "mock-key";
      if (args.type === "tls:index/selfSignedCert:SelfSignedCert") state.certPem = "mock-certificate";
      if (args.type === "authentik:index/providerOauth2:ProviderOauth2") state.providerOauth2Id = "1";
      if (args.type === "authentik:index/propertyMappingProviderScope:PropertyMappingProviderScope") state.propertyMappingProviderScopeId = args.name;
      if (args.name === "inbox-raw-generated-config") state.data = { BUCKET_NAME: "test-raw" };
      if (args.name === "inbox-backups-generated-config") state.data = { BUCKET_NAME: "test-backups" };
      if (args.name === "inbox-raw-generated-secret") state.data = {
        AWS_ACCESS_KEY_ID: Buffer.from("test-access").toString("base64"),
        AWS_SECRET_ACCESS_KEY: Buffer.from("test-secret").toString("base64"),
      };
      if (args.name === "inbox-postgres-generated-app") state.data = {
        username: Buffer.from("app+name").toString("base64"),
        password: Buffer.from("pass/@?#").toString("base64"),
      };
      return { id: `${args.name}-id`, state };
    },
    call: (args) => ({
      ...args.inputs, id: "lookup-id",
      data: {
        BUCKET_NAME: args.inputs.name?.includes("backups") ? "test-backups" : "test-raw",
        AWS_ACCESS_KEY_ID: Buffer.from("test-access").toString("base64"),
        AWS_SECRET_ACCESS_KEY: Buffer.from("test-secret").toString("base64"),
        username: Buffer.from("app+name").toString("base64"),
        password: Buffer.from("pass/@?#").toString("base64"),
      },
    }),
  }, "inbox", "preview", false);
  await import("./index");
  await pulumi.runtime.disconnect();
});
after(() => {
  if (previous === undefined) delete process.env.PULUMI_CONFIG;
  else process.env.PULUMI_CONFIG = previous;
});

test("rejects mutable images, cross-environment identities, insecure origins, and invalid key rotations", () => {
  assert.throws(() => requireImmutableImage("registry.test/inbox:main"));
  assert.throws(() => requireImmutableImage("registry.test/inbox@sha256:abc"));
  assert.doesNotThrow(() => requireImmutableImage(`registry.test/inbox@sha256:${"b".repeat(64)}`));
  assert.throws(() => validateHttpsOrigin("http://auth.test", "auth"));
  assert.throws(() => validateHttpsOrigin("https://user:pass@auth.test", "auth"));
  assert.throws(() => validateEnvironment("prod", "inbox", "inbox.holdenitdown.net", "inbox", "INBOX_PREVIEW", true));
  assert.throws(() => validateEnvironment("prod", "inbox", "inbox.holdenitdown.net", "inbox", "INBOX_PROD", false));
  assert.doesNotThrow(() => validateEnvironment("prod", "inbox", "inbox.holdenitdown.net", "inbox", "INBOX_PROD", true));
  for (const versions of [[], ["v1", "v1"], ["Bad"], ["v2"]]) {
    assert.throws(() => validateWrappingKeys(versions, "v1"));
  }
  assert.doesNotThrow(() => validateWrappingKeys(["v1", "v2"], "v2"));
});
test("declares hardened runtime, verify-full PostgreSQL, OIDC and separate shared NATS", () => {
  const spec = resources.find((r) => r.type === "kubernetes:apps/v1:Deployment")!.inputs.spec;
  assert.equal(spec.replicas, 1);
  assert.equal(spec.strategy.type, "Recreate");
  const pod = spec.template.spec;
  assert.equal(pod.automountServiceAccountToken, false);
  assert.equal(pod.securityContext.runAsUser, 10001);
  assert.equal(pod.securityContext.runAsGroup, 10001);
  assert.equal(pod.containers[0].securityContext.readOnlyRootFilesystem, true);
  assert.deepEqual(pod.containers[0].securityContext.capabilities.drop, ["ALL"]);
  assert.equal(pod.containers[0].readinessProbe.httpGet.path, "/ready");
  assert.equal(pod.volumes.find((v: any) => v.name === "postgres-ca").secret.secretName, "inbox-postgres-ca");
  const env = resource("inbox-app").stringData;
  assert.match(env.DATABASE_URL, /app%2Bname:pass%2F%40%3F%23@inbox-postgres-rw/);
  assert.match(env.DATABASE_URL, /sslmode=verify-full&sslrootcert=/);
  assert.equal(env.INBOX_AUTH_MODE, "oidc");
  assert.equal(env.INBOX_PUBLIC_URL, "https://preview-inbox.holdenitdown.net");
  assert.equal(env.INBOX_OIDC_ISSUER, "https://auth.example.test/application/o/inbox-preview/");
  assert.equal(env.INBOX_OIDC_CLIENT_ID, env.INBOX_OIDC_API_CLIENT_ID);
  assert.equal(env.INBOX_OIDC_API_AUDIENCE, "inbox-preview-api");
  assert.equal(env.INBOX_AUTH_ADMIN_GROUP, "test-domain-admins");
  assert.equal(env.INBOX_NATS_NAMESPACE, "INBOX_PREVIEW");
  assert.equal(env.INBOX_NATS_REPLICAS, "3");
  assert.equal(env.INBOX_NATS_URL, "nats://nats.nats.svc.cluster.local:4222");
  assert.equal(env.INBOX_WEB_DIR, "/app/web");
});
test("uses exactly one provider with UUID subjects, strict callbacks, verified groups and scoped API audience", () => {
  const providers = resources.filter((r) => r.type === "authentik:index/providerOauth2:ProviderOauth2");
  assert.equal(providers.length, 1);
  const provider = providers[0].inputs;
  assert.equal(provider.clientType, "confidential");
  assert.equal(provider.subMode, "user_uuid");
  assert.equal(provider.issuerMode, "per_provider");
  assert.equal(provider.includeClaimsInIdToken, true);
  assert.deepEqual(provider.allowedRedirectUris, [
    { matching_mode: "strict", url: "https://preview-inbox.holdenitdown.net/auth/callback" },
    { matching_mode: "strict", url: "https://preview-inbox.holdenitdown.net/oauth/oidc/callback" },
  ]);
  assert.equal(resource("inbox-identity-groups").scopeName, "profile");
  assert.match(resource("inbox-identity-groups").expression, /"groups": \[group.name for group in request.user.ak_groups.all\(\)\]/);
  assert.equal(resource("inbox-identity-api").scopeName, "inbox:api");
  assert.equal(resource("inbox-identity-api").expression, 'return {"aud": "inbox-preview-api"}');
});
test("retains versioned 32-byte base64url wrapping keys and separate raw and backup claims", () => {
  const keys = resources.filter((r) => r.type === "random:index/randomBytes:RandomBytes");
  assert.deepEqual(keys.map((r) => [r.name, r.inputs.length]), [
    ["inbox-oauth-wrapping-key-v1", 32], ["inbox-oauth-wrapping-key-v2", 32],
  ]);
  const keyring = JSON.parse(resource("inbox-oauth-wrapping-keys").stringData["keyring.json"]);
  assert.equal(keyring.schema_version, 1);
  assert.equal(keyring.active, "v2");
  assert.deepEqual(keyring.keys.map((k: any) => k.id), ["v1", "v2"]);
  for (const key of keyring.keys) assert.match(key.key, /^[A-Za-z0-9_-]{43}$/);
  assert.equal(resources.filter((r) => r.inputs.kind === "ObjectBucketClaim").length, 2);
  assert.equal(resource("inbox-raw-bucket").spec.generateBucketName, "inbox-preview-raw");
  assert.equal(resource("inbox-backups-bucket").spec.generateBucketName, "inbox-preview-backups");
  assert.equal(resource("inbox-postgres").spec.backup.retentionPolicy, "14d");
  assert.equal(resource("inbox-postgres-backup").spec.schedule, "0 0 2 * * *");
});
test("exposes only HTTP via Gateway and defaults SMTP to no ingress or service port", () => {
  const service = resources.find((r) => r.type === "kubernetes:core/v1:Service")!.inputs;
  assert.equal(service.spec.type, "ClusterIP");
  assert.deepEqual(service.spec.ports, [{ name: "http", port: 8080, targetPort: "http" }]);
  const route = resource("inbox-route").spec;
  assert.deepEqual(route.parentRefs, [{ group: "gateway.networking.k8s.io", kind: "Gateway", name: "default-gateway", namespace: "ingress" }]);
  assert.equal(route.rules[0].timeouts.request, "0s");
  assert.equal(route.rules[0].backendRefs[0].port, 8080);
  const policy = resource("inbox-network").spec;
  assert.deepEqual(policy.policyTypes, ["Ingress", "Egress"]);
  assert.equal(policy.ingress.length, 1);
  assert.equal(policy.ingress[0].ports[0].port, 8080);
  assert.ok(policy.egress.find((r: any) => r.ports.some((p: any) => p.port === 4222)));
  assert.equal(resources.filter((r) => /nats/i.test(r.type)).length, 0);
});
test("stack YAML has no copied secrets, salts, bootstrap-domain assumptions or mutable images; Tekton uses image config", () => {
  for (const stack of ["preview", "prod"]) {
    const source = readFileSync(`Pulumi.${stack}.yaml`, "utf8");
    assert.doesNotMatch(source, /encryptionsalt|secure:|inbox:image:|inbox:adminGroup:|inbox:bootstrapDomain:/);
    assert.match(source, new RegExp(`inbox:natsNamespace: INBOX_${stack === "prod" ? "PROD" : "PREVIEW"}`));
  }
  for (const name of ["preview", "release"]) {
    const source = readFileSync(`../../.tekton/inbox-${name}.yaml`, "utf8");
    assert.match(source, /value: infra\/pulumi/);
    assert.match(source, /name: image-config/);
    if (name === "preview") assert.match(source, /name: image-config\s+value: '\{"image":/);
    else assert.match(source, /"key":"image"/);
    assert.doesNotMatch(source, /name: (?:test|smoke)[-\n]/);
  }
});
