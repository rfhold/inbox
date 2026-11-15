import { createHash } from "node:crypto";
import * as authentik from "@pulumi/authentik";
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as random from "@pulumi/random";
import * as tls from "@pulumi/tls";
import { BrowserApplication } from "./authentik";
import { requireImmutableImage, validateEnvironment, validateHostname, validateHttpsOrigin, validateSegment, validateWrappingKeys } from "./policy";

const config = new pulumi.Config();
const namespaceName = validateSegment(config.require("namespace"), "namespace");
const hostname = validateHostname(config.require("hostname"));
const displayName = config.require("displayName");
const slug = validateSegment(config.require("slug"), "slug");
const image = requireImmutableImage(config.require("image"));
const authentikBaseUrl = validateHttpsOrigin(config.require("authentikBaseUrl"), "authentikBaseUrl");
const s3Endpoint = validateHttpsOrigin(config.require("s3Endpoint"), "s3Endpoint");
const backupEndpoint = validateHttpsOrigin(config.require("backupEndpoint"), "backupEndpoint");
const storageClass = config.require("storageClass");
const bucketStorageClass = config.require("bucketStorageClass");
const backupStorageClass = config.require("backupStorageClass");
const backupRetention = config.require("backupRetention");
const databaseStorageSize = config.require("databaseStorageSize");
const protectData = config.requireBoolean("protectData");
const adminGroup = config.require("adminGroup").trim();
const bootstrapDomain = validateHostname(config.require("bootstrapDomain"));
if (!adminGroup) throw new Error("adminGroup must name the explicitly authorized administrator group");
const natsNamespace = config.require("natsNamespace");
const environment = pulumi.getStack();
validateEnvironment(environment, namespaceName, hostname, slug, natsNamespace, protectData);
const versions = config.requireObject<unknown>("oauthWrappingKeyVersions");
const activeVersion = config.require("oauthActiveWrappingKeyVersion");
validateWrappingKeys(versions, activeVersion);
const natsUser = config.getSecret("natsUser");
const natsPassword = config.getSecret("natsPassword");
const natsCredentials = config.getSecret("natsCredentials");
if ((!!natsUser !== !!natsPassword) || (!!natsCredentials === !!natsUser)) {
  throw new Error("configure exactly natsUser+natsPassword or secret natsCredentials; anonymous shared NATS is not allowed");
}
const smtpNamespace = config.get("smtpSourceNamespace");
const smtpPodLabels = config.getObject<Record<string, string>>("smtpSourcePodLabels");
if (!!smtpNamespace !== !!smtpPodLabels || (smtpPodLabels && Object.keys(smtpPodLabels).length === 0)) {
  throw new Error("private SMTP ingress requires both smtpSourceNamespace and nonempty smtpSourcePodLabels");
}
if (smtpNamespace) validateSegment(smtpNamespace, "smtpSourceNamespace");
const publicUrl = `https://${hostname}`;
const labels = { "app.kubernetes.io/name": "inbox", "app.kubernetes.io/instance": slug, "app.kubernetes.io/managed-by": "pulumi" };
const workloadLabels = { ...labels, "app.kubernetes.io/component": "server" };
const protectedData = { protect: protectData };
const namespace = new k8s.core.v1.Namespace("inbox-namespace", {
  metadata: { name: namespaceName, labels },
}, protectedData);
const bucket = (name: string, storage: string) => new k8s.apiextensions.CustomResource(`inbox-${name}-bucket`, {
  apiVersion: "objectbucket.io/v1alpha1", kind: "ObjectBucketClaim",
  metadata: { name: `inbox-${name}`, namespace: namespace.metadata.name, labels },
  spec: { storageClassName: storage, generateBucketName: `${slug}-${name}` },
}, { dependsOn: [namespace], ...protectedData });
const rawBucket = bucket("raw", bucketStorageClass);
const backupBucket = bucket("backups", backupStorageClass);
const bucketConfig = (name: string, claim: k8s.apiextensions.CustomResource) =>
  pulumi.all([namespace.metadata.name, claim.id]).apply(([ns]) =>
    k8s.core.v1.ConfigMap.get(`inbox-${name}-generated-config`, `${ns}/inbox-${name}`));
const rawConfig = bucketConfig("raw", rawBucket);
const backupConfig = bucketConfig("backups", backupBucket);
const rawSecret = pulumi.all([namespace.metadata.name, rawBucket.id]).apply(([ns]) =>
  k8s.core.v1.Secret.get("inbox-raw-generated-secret", `${ns}/inbox-raw`));
const databaseCluster = new k8s.apiextensions.CustomResource("inbox-postgres", {
  apiVersion: "postgresql.cnpg.io/v1", kind: "Cluster",
  metadata: { name: "inbox-postgres", namespace: namespace.metadata.name, labels },
  spec: {
    instances: 1, enableSuperuserAccess: false,
    backup: {
      retentionPolicy: backupRetention,
      barmanObjectStore: {
        destinationPath: backupConfig.data.apply((values) => `s3://${values.BUCKET_NAME}/database`),
        endpointURL: backupEndpoint,
        s3Credentials: {
          accessKeyId: { name: "inbox-backups", key: "AWS_ACCESS_KEY_ID" },
          secretAccessKey: { name: "inbox-backups", key: "AWS_SECRET_ACCESS_KEY" },
        },
        wal: { compression: "gzip" }, data: { compression: "gzip" },
      },
    },
    storage: { size: databaseStorageSize, storageClass },
    resources: { requests: { cpu: "100m", memory: "256Mi" }, limits: { cpu: "1", memory: "1Gi" } },
  },
}, { dependsOn: [backupBucket], ...protectedData });
const scheduledBackup = new k8s.apiextensions.CustomResource("inbox-postgres-backup", {
  apiVersion: "postgresql.cnpg.io/v1", kind: "ScheduledBackup",
  metadata: { name: "inbox-postgres", namespace: namespace.metadata.name, labels },
  spec: {
    schedule: "0 0 2 * * *", backupOwnerReference: "self", cluster: { name: "inbox-postgres" },
    immediate: true, method: "barmanObjectStore",
  },
}, { dependsOn: [databaseCluster], ...protectedData });
const database = new k8s.apiextensions.CustomResource("inbox-database", {
  apiVersion: "postgresql.cnpg.io/v1", kind: "Database",
  metadata: { name: "inbox", namespace: namespace.metadata.name, labels },
  spec: { name: "inbox", owner: "app", cluster: { name: "inbox-postgres" } },
}, { dependsOn: [databaseCluster], ...protectedData });
const privateKey = new tls.PrivateKey("inbox-oidc-signing-key", { algorithm: "RSA", rsaBits: 4096 }, protectedData);
const certificate = new tls.SelfSignedCert("inbox-oidc-signing-certificate", {
  privateKeyPem: privateKey.privateKeyPem,
  subject: { commonName: `${slug} OIDC signing` }, validityPeriodHours: 87600,
  allowedUses: ["digital_signature", "key_encipherment"],
}, protectedData);
const signingKey = new authentik.CertificateKeyPair("inbox-oidc-signing-keypair", {
  name: `${displayName} OIDC signing key`, certificateData: certificate.certPem, keyData: privateKey.privateKeyPem,
}, protectedData);
const oidc = new BrowserApplication("inbox-identity", {
  displayName, slug, issuerBaseUrl: authentikBaseUrl, publicUrl, signingKeyId: signingKey.id, protectData,
});
const pgSecret = pulumi.all([namespace.metadata.name, databaseCluster.id]).apply(([ns]) =>
  k8s.core.v1.Secret.get("inbox-postgres-generated-app", `${ns}/inbox-postgres-app`));
const decodePg = (key: string) => pgSecret.data.apply((values) => Buffer.from(values[key], "base64").toString("utf8"));
const decodeRaw = (key: string) => rawSecret.data.apply((values) => Buffer.from(values[key], "base64").toString("utf8"));
const databaseUrl = pulumi.all([decodePg("username"), decodePg("password")]).apply(([username, password]) =>
  `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(password)}@inbox-postgres-rw.${namespaceName}.svc:5432/inbox?sslmode=verify-full&sslrootcert=%2Fvar%2Frun%2Fsecrets%2Finbox%2Fpostgres%2Fca.crt`);
const wrappingKeys = versions.map((version) => new random.RandomBytes(`inbox-oauth-wrapping-key-${version}`, { length: 32 }, protectedData).base64);
const keyring = pulumi.secret(pulumi.all(wrappingKeys).apply((keys) => JSON.stringify({
  schema_version: 1, active: activeVersion,
  keys: versions.map((version, index) => ({ id: version, key: Buffer.from(keys[index], "base64").toString("base64url") })),
})));
const wrappingSecret = new k8s.core.v1.Secret("inbox-oauth-wrapping-keys", {
  metadata: { name: "inbox-oauth-wrapping-keys", namespace: namespace.metadata.name, labels },
  type: "Opaque", stringData: { "keyring.json": keyring },
}, protectedData);
const credentialsSecret = natsCredentials ? new k8s.core.v1.Secret("inbox-nats-credentials", {
  metadata: { name: "inbox-nats-credentials", namespace: namespace.metadata.name, labels },
  type: "Opaque", stringData: { "nats.creds": natsCredentials },
}, protectedData) : undefined;
const appSecret = new k8s.core.v1.Secret("inbox-app", {
  metadata: { name: "inbox-app", namespace: namespace.metadata.name, labels },
  type: "Opaque",
  stringData: {
    DATABASE_URL: databaseUrl,
    INBOX_AUTH_MODE: "oidc", INBOX_PUBLIC_URL: publicUrl,
    INBOX_OIDC_ISSUER: oidc.issuer, INBOX_OIDC_CLIENT_ID: oidc.clientId,
    INBOX_OIDC_CLIENT_SECRET: oidc.clientSecret,
    INBOX_OIDC_API_AUDIENCE: `${slug}-api`, INBOX_OIDC_API_CLIENT_ID: oidc.clientId,
    INBOX_OIDC_API_SCOPES: "inbox:api", INBOX_AUTH_ADMIN_GROUP: adminGroup,
    INBOX_OAUTH_WRAPPING_KEYS_FILE: "/var/run/secrets/inbox/oauth/keyring.json",
    INBOX_NATS_URL: "nats://nats.nats.svc.cluster.local:4222",
    INBOX_NATS_NAMESPACE: natsNamespace, INBOX_NATS_REPLICAS: "3",
    ...(natsCredentials ? { INBOX_NATS_CREDENTIALS_FILE: "/var/run/secrets/inbox/nats/nats.creds" } :
      { INBOX_NATS_USER: natsUser!, INBOX_NATS_PASSWORD: natsPassword! }),
    INBOX_DOMAIN: bootstrapDomain, INBOX_ENVIRONMENT: environment,
    INBOX_HTTP_ADDR: "0.0.0.0:8080", INBOX_SMTP_ADDR: "0.0.0.0:2525",
    INBOX_WEB_DIR: "/app/web",
    INBOX_S3_ENDPOINT: s3Endpoint, INBOX_S3_REGION: "us-east-1",
    INBOX_S3_BUCKET: rawConfig.data.apply((values) => values.BUCKET_NAME),
    AWS_ACCESS_KEY_ID: decodeRaw("AWS_ACCESS_KEY_ID"), AWS_SECRET_ACCESS_KEY: decodeRaw("AWS_SECRET_ACCESS_KEY"),
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://telemetry.holdenitdown.net:4318",
    OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf", OTEL_SERVICE_NAME: "inbox-server",
    OTEL_RESOURCE_ATTRIBUTES: `service.namespace=inbox,deployment.environment.name=${environment}`,
  },
}, { dependsOn: [database, oidc, rawBucket], ...protectedData });
const deployment = new k8s.apps.v1.Deployment("inbox", {
  metadata: {
    name: "inbox", namespace: namespace.metadata.name, labels: workloadLabels,
    annotations: { "secret.reloader.stakater.com/reload": ["inbox-app", "inbox-oauth-wrapping-keys", "inbox-postgres-ca", ...(credentialsSecret ? ["inbox-nats-credentials"] : [])].join(",") },
  },
  spec: {
    replicas: 1, strategy: { type: "Recreate" }, selector: { matchLabels: workloadLabels },
    template: {
      metadata: {
        labels: workloadLabels,
        annotations: { "inbox.holdenitdown.net/wrapping-key-checksum": keyring.apply((value) => createHash("sha256").update(value).digest("hex")) },
      },
      spec: {
        automountServiceAccountToken: false, terminationGracePeriodSeconds: 30,
        securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001, seccompProfile: { type: "RuntimeDefault" } },
        containers: [{
          name: "inbox", image, imagePullPolicy: "IfNotPresent",
          ports: [{ name: "http", containerPort: 8080 }, { name: "smtp", containerPort: 2525 }],
          envFrom: [{ secretRef: { name: appSecret.metadata.name } }],
          securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } },
          resources: { requests: { cpu: "100m", memory: "256Mi" }, limits: { cpu: "2", memory: "1Gi" } },
          startupProbe: { httpGet: { path: "/health", port: "http" }, periodSeconds: 2, failureThreshold: 60 },
          readinessProbe: { httpGet: { path: "/ready", port: "http" }, periodSeconds: 5, failureThreshold: 3 },
          livenessProbe: { httpGet: { path: "/health", port: "http" }, periodSeconds: 10, failureThreshold: 3 },
          volumeMounts: [
            { name: "tmp", mountPath: "/tmp" },
            { name: "oauth", mountPath: "/var/run/secrets/inbox/oauth", readOnly: true },
            { name: "postgres-ca", mountPath: "/var/run/secrets/inbox/postgres", readOnly: true },
            ...(credentialsSecret ? [{ name: "nats", mountPath: "/var/run/secrets/inbox/nats", readOnly: true }] : []),
          ],
        }],
        volumes: [
          { name: "tmp", emptyDir: { sizeLimit: "256Mi" } },
          { name: "oauth", secret: { secretName: wrappingSecret.metadata.name, defaultMode: 0o440 } },
          { name: "postgres-ca", secret: { secretName: "inbox-postgres-ca", defaultMode: 0o444, items: [{ key: "ca.crt", path: "ca.crt" }] } },
          ...(credentialsSecret ? [{ name: "nats", secret: { secretName: credentialsSecret.metadata.name, defaultMode: 0o440 } }] : []),
        ],
      },
    },
  },
}, { dependsOn: [appSecret, wrappingSecret] });
new k8s.networking.v1.NetworkPolicy("inbox-network", {
  metadata: { name: "inbox", namespace: namespace.metadata.name, labels },
  spec: {
    podSelector: { matchLabels: workloadLabels }, policyTypes: ["Ingress", "Egress"],
    ingress: [
      { from: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "ingress" } } }], ports: [{ port: 8080, protocol: "TCP" }] },
      ...(smtpNamespace && smtpPodLabels ? [{
        from: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": smtpNamespace } }, podSelector: { matchLabels: smtpPodLabels } }],
        ports: [{ port: 2525, protocol: "TCP" as const }],
      }] : []),
    ],
    egress: [
      { to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "kube-system" } } }], ports: [{ port: 53, protocol: "UDP" }, { port: 53, protocol: "TCP" }] },
      { to: [{ podSelector: { matchLabels: { "cnpg.io/cluster": "inbox-postgres" } } }], ports: [{ port: 5432, protocol: "TCP" }] },
      { to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "nats" } } }], ports: [{ port: 4222, protocol: "TCP" }] },
      { ports: [{ port: 443, protocol: "TCP" }, { port: 4318, protocol: "TCP" }] },
    ],
  },
});
const service = new k8s.core.v1.Service("inbox", {
  metadata: { name: "inbox", namespace: namespace.metadata.name, labels },
  spec: {
    type: "ClusterIP", selector: workloadLabels,
    ports: [{ name: "http", port: 8080, targetPort: "http" }, ...(smtpNamespace ? [{ name: "smtp", port: 2525, targetPort: "smtp" }] : [])],
  },
});
const route = new k8s.apiextensions.CustomResource("inbox-route", {
  apiVersion: "gateway.networking.k8s.io/v1", kind: "HTTPRoute",
  metadata: { name: "inbox", namespace: namespace.metadata.name, labels },
  spec: {
    parentRefs: [{ group: "gateway.networking.k8s.io", kind: "Gateway", name: "default-gateway", namespace: "ingress" }],
    hostnames: [hostname],
    rules: [{ matches: [{ path: { type: "PathPrefix", value: "/" } }], backendRefs: [{ name: service.metadata.name, port: 8080 }], timeouts: { request: "0s" } }],
  },
});
export const deployedNamespace = namespace.metadata.name;
export const deployedUrl = publicUrl;
export const databaseClusterName = databaseCluster.metadata.name;
export const databaseName = database.metadata.name;
export const backupScheduleName = scheduledBackup.metadata.name;
export const rawBucketClaimName = rawBucket.metadata.name;
export const backupBucketClaimName = backupBucket.metadata.name;
export const oauthWrappingSecretName = wrappingSecret.metadata.name;
export const applicationSecretName = appSecret.metadata.name;
export const deploymentName = deployment.metadata.name;
export const serviceName = service.metadata.name;
export const routeName = route.metadata.name;
export const oidcIssuer = oidc.issuer;
export const oidcClientId = oidc.clientId;
export const deployedNatsNamespace = natsNamespace;
