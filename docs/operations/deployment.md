# Preview and production deployment

This guide defines approved intended infrastructure, not deployed resources or live verification.
The [Pulumi program](../../infra/pulumi/index.ts) and [policy module](../../infra/pulumi/policy.ts) define the configuration reference below.
The [local runbook](runbook.md) remains the Compose procedure.
No command here grants deployment, IdP mutation, data replacement, or reset authority.

## Resource ownership

Pulumi owns the complete Inbox environment resource graph, under the sibling Faktory pattern.
Each preview or production stack declares its namespace, application workload, service, route, network policy, and runtime secrets.
Each stack also declares CNPG PostgreSQL, its backup schedule, and Ceph ObjectBucketClaims for raw MIME and database backups.
Raw MIME and backup buckets use separate credentials and purposes.
Each environment has its own Authentik application, provider configuration, browser client secret, and OAuth wrapping-key material.
Hosted environments require OIDC; private SMTP does not use browser OIDC.

Production data resources require Pulumi protection.
Protection does not establish Ceph retention, version history, or backup completeness.
Operators must verify actual Ceph retention and deletion policy before production approval.
CNPG backups do not back up raw MIME or shared NATS state.

## Environment configuration

The checked-in stack files select these fixed identities.

| Stack | Namespace | HTTPS hostname | NATS namespace | PostgreSQL size | Backup retention | Data protection |
| --- | --- | --- | --- | --- | --- | --- |
| [preview](../../infra/pulumi/Pulumi.preview.yaml) | `inbox-preview` | `preview-inbox.holdenitdown.net` | `INBOX_PREVIEW` | `10Gi` | `14d` | `false` |
| [prod](../../infra/pulumi/Pulumi.prod.yaml) | `inbox` | `inbox.holdenitdown.net` | `INBOX_PROD` | `20Gi` | `30d` | `true` |

Both stacks target Kubernetes context `pantheon`, PostgreSQL storage class `shared-fs`, and bucket storage class `default-bucket`.
Raw MIME and backup endpoints both use `https://s3.pantheon.holdenitdown.net`.
These declarations do not establish available storage, retention enforcement, or a successful backup.

Operators must supply these application configuration values through approved Pulumi configuration and secret channels.
Names below use the `inbox:` config namespace.

| Configuration key | Required value or rule |
| --- | --- |
| `image` | Immutable image reference with `@sha256:` and 64 lowercase hexadecimal digits; Tekton supplies the artifact digest. |
| `authentikBaseUrl` | Credential-free HTTPS origin for the approved identity provider. |
| `adminGroup` | Nonempty exact verified group name that grants domain administration. |
| `bootstrapDomain` | Fully qualified domain for one-time fresh-instance bootstrap; never a mailbox or mail migration instruction. |
| `natsUser` and `natsPassword` | Secret username/password pair; mutually exclusive with `natsCredentials`. |
| `natsCredentials` | Secret complete NATS credentials-file contents; alternative to the username/password pair. |

The Pulumi program rejects absent NATS credentials, partial username/password pairs, and simultaneous credential methods.
Anonymous shared NATS configuration fails infrastructure validation, even though the local runtime supports anonymous private development.
The stack files also declare resource identities, storage settings, and OAuth wrapping-key versions.
`oauthWrappingKeyVersions` contains unique DNS labels; `oauthActiveWrappingKeyVersion` must select one declared version.
Both initial declarations use version `v1`; key rotation requires its own recovery review and approval.

The [auth configuration reference](../architecture/spec/auth.md#auth-configuration) defines exact runtime auth variables.
Pulumi derives client IDs, issuer URLs, callback registration, database/S3 credentials, and key-file mounts from its resource graph.
The hosted runtime receives these NATS settings:

| Runtime variable | Declared hosted value |
| --- | --- |
| `INBOX_NATS_URL` | `nats://nats.nats.svc.cluster.local:4222` |
| `INBOX_NATS_NAMESPACE` | `INBOX_PREVIEW` or `INBOX_PROD`, according to the selected stack. |
| `INBOX_NATS_REPLICAS` | `3` |
| `INBOX_NATS_USER` and `INBOX_NATS_PASSWORD` | Secret pair, only with username/password authentication. |
| `INBOX_NATS_CREDENTIALS_FILE` | `/var/run/secrets/inbox/nats/nats.creds`, only with file credentials. |

## Identity-provider constraint

Native API token admission relies on Authentik `2025.10.2` and its `IDToken.to_access_token()` behavior.
That path adds `uid`, `azp`, and `scope` to access tokens; provider mappings must reserve all three claims.
Mappings must never inject these claims into ID tokens.
The declared [Authentik mappings](../../infra/pulumi/authentik.ts) add verified groups and the API audience, not these reserved access-token claims.
Managed mappings and effective provider configuration require the same review.
API audience values can also appear in ID tokens; audience alone does not distinguish token use.
The [native token contract](../architecture/spec/auth.md#native-access-token-profile) defines exact admission checks.

Before a provider upgrade or mapping change, verify its access-token claim behavior and ID-token separation.
Treat any change to the reserved claims as an authentication compatibility and security decision.
Source declarations do not prove the deployed provider version or a successful browser/native OAuth flow.

## Release tracks

| Track | Trigger | Artifact and target |
| --- | --- | --- |
| Fixed preview | Main-branch push | Build an immutable multiarchitecture image and update the one shared preview environment. |
| Production | Trusted signed stable `vMAJOR.MINOR.PATCH` tag | Verify signature and main ancestry; promote the prior immutable image without a rebuild. |

Preview is not a per-pull-request environment.
Preview and production use separate namespaces, databases, buckets, secrets, auth applications, and NATS prefixes.
Mutable tags never select a deployed artifact.
Release policy checks do not establish application correctness.

Tekton follows Waltr's pinned reusable service tasks:

1. Clone the exact revision.
2. Scan deployment inputs for private credential material.
3. Build amd64 and arm64 images for preview.
4. Publish the immutable multiarchitecture manifest.
5. Apply the selected Pulumi stack.

Production instead verifies the signed stable tag and promotes the immutable image before the production stack update.
Tekton pipelines contain no unit, integration, browser, infrastructure, or smoke-test tasks.
Local validation remains required under the [quality guide](../quality/testing.md).
Manual OAuth and backup/restore verification remain separate operator gates, not pipeline test tasks.

## Container build boundary

The [Dockerfile](../../Dockerfile) uses the repository root as its sole build context.
The [server manifest](../../crates/inbox-server/Cargo.toml) fetches Waltr from Git at `d7a2527db2b0b85b2b382527f24d779bb522d8e9`.
Neither a sibling checkout, an additional Compose build context, nor a vendored Waltr tree supplies the dependency.
Private Git credentials enter through optional BuildKit secrets, not image layers or tracked configuration.
The [Compose file](../../compose.yml) uses the same root context.

## Shared NATS isolation

Hosted Inbox uses the shared homelab NATS service, not a broker per environment.
The application creates its required JetStream resources at runtime under an explicit environment prefix.
Every stream, subject, object store, durable consumer, cursor generation, and outbox publication target respects that environment boundary.
Preview cannot consume production commands, staged payloads, results, changes, or arrival cursors.
Credentials and broker ACLs must enforce the same boundary.

The [command contract](../architecture/spec/commands.md) retains strict durability, retention, capacity, and no-fallback requirements.
Environment isolation never relaxes pending-payload protection, outbox recovery, or durable result confirmation.
Live broker ACLs, available capacity, replica counts, and failure-domain guarantees require verification.
Local single-replica Compose limits do not establish shared-broker readiness.

## Intake and network boundary

HTTP routes serve the browser, API, and human-authorized MCP over HTTPS.
SMTP remains private trusted intake with explicit network restrictions.
By default, the declared Service has no SMTP port and the NetworkPolicy admits no SMTP ingress.
The application still declares its private SMTP listener inside the pod.
Optional SMTP access requires both `smtpSourceNamespace` and nonempty `smtpSourcePodLabels` in Pulumi configuration.
The ingress peer combines the namespace selector and pod selector; a namespace-only or pod-only match grants no access.
Only this explicit opt-in adds the Service port and the corresponding TCP 2525 ingress rule.
It provides neither a public MX nor authenticated client SMTP submission.
No auth bypass on SMTP grants HTTP or MCP mailbox access.
Store endpoints and credentials remain private; frontend configuration contains only public auth metadata and telemetry settings.

## Operator gates and recovery

Before any apply, verify the exact stack, immutable image, namespace, hostname, secret sources, and approved action.
Cross-repository homelab Pipelines-as-Code registration requires a separate implementation and apply decision.
The central registration for `rfhold/inbox` remains outside these resource declarations; no live registration or apply follows from this guide.
Repository declarations alone do not establish that registration or any IdP resource exists live.

Before production approval:

- Verify browser login, API identity, MCP consent, token refresh, revocation, and two-user mail isolation.
- Verify private SMTP network restrictions and shared NATS ACLs.
- Verify CNPG backup success and Ceph raw-object retention.
- Restore a consistent database, raw MIME, NATS work state, and auth key set into an isolated target.
- Verify pending commands, outbox replay, durable results, raw fetches, and cursor reset behavior.

Auth wrapping keys and compatible key versions form part of the private recovery set.
Loss of those keys can prevent token verification or key decryption.
Restore procedures require exact-target approval and a consistent mail-work boundary.
Neither a database-only rollback nor an unrelated object snapshot establishes a safe restore.
Application rollback selects a prior immutable image only after schema and auth-state compatibility review.
Data rollback requires a separate recovery decision.

Local mail stays local; cloud deployment does not import it.
Preview refresh does not authorize database, bucket, NATS, or browser-draft deletion.
No production reset or local-to-cloud migration follows from this guide.

## Source patterns

These sibling sources supply implementation patterns, not evidence of live Inbox infrastructure:

| Source | Relevant pattern |
| --- | --- |
| [Faktory Pulumi](https://git.holdenitdown.net/rfhold/faktory/src/branch/main/infra/pulumi) | Full resource graph, CNPG, Ceph claims, auth application, secrets, and production data protection. |
| [Waltr Tekton](https://git.holdenitdown.net/rfhold/waltr/src/branch/main/.tekton) | Reusable build, manifest, promotion, and Pulumi tasks without application test tasks. |
| [Walter environment overlays](https://git.holdenitdown.net/rfhold/walter/src/branch/main/manifests/overlays) | Shared NATS environment prefixes. |
| [Walter NATS pipeline](https://git.holdenitdown.net/rfhold/walter/src/branch/main/walterd/internal/pipeline/pipeline_nats.go) | Runtime stream and consumer creation. |

Walter's pipeline retention and retry limits do not define Inbox durability.
Inbox retains its own pending-command, durable-result, payload, and replay requirements.
