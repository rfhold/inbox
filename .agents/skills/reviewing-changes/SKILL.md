---
name: reviewing-changes
description: Use when an Inbox review needs per-user auth, hosted OAuth, environment isolation, NATS durability, MCP Events, offline privacy, or operational safety inputs.
---

# Review Inbox changes

This skill supplies repository-specific inputs to `making-changes`; it does not own the review workflow or grant mutation authority.

## Canonical inputs

- [Architecture and contracts](../../../docs/architecture/README.md).
- [Authentication and per-user authorization](../../../docs/architecture/spec/auth.md).
- [Preview and production deployment](../../../docs/operations/deployment.md).
- [Command and confirmation contract](../../../docs/architecture/spec/commands.md).
- [Email-domain contract](../../../docs/architecture/spec/domains.md).
- [Change stream contract](../../../docs/architecture/spec/changes.md).
- [MCP contract](../../../docs/architecture/spec/mcp.md) and [durable arrival contract](../../../docs/architecture/spec/arrivals.md).
- [MCP client guide](../../../docs/architecture/mcp.md).
- [Test guide](../../../docs/quality/testing.md).
- [Operational safety](../../../docs/operations/runbook.md).
- [Authoritative protobuf schema](../../../proto/inbox/v1/inbox.proto).

Use the original approved requirements and actual diffs, not only an implementation summary.

## Coupled surfaces

Paths below are repository-root-relative.

| Change surface | Inspect together |
| --- | --- |
| RPC and stream contract | `proto/inbox/v1/inbox.proto`, `crates/inbox-server/build.rs`, `web/src/gen/`, `web/src/api.ts`, and frontend stream/cache consumers. |
| Commands, confirmation, and worker | `crates/inbox-server/src/pipeline.rs`, `crates/inbox-server/src/mutation.rs`, `migrations/`, and NATS configuration. |
| Domain identity and policy | Protobuf, domain/mailbox migrations, bootstrap, command consumers, SMTP, MCP schemas, web selectors, and domain cache events. |
| Offline and display | `web/src/`, `web/public/`, and browser storage tests. |
| Runtime and secrets | `compose.yml`, `Dockerfile`, `docker/nats.conf`, `docker/`, `scripts/`, and `.env.example`. |
| Telemetry | Server and browser instrumentation, `/config.json`, Compose endpoints, and local capture evidence. |
| MCP and Events | MCP adapter, Waltr transport, `opencode.jsonc`, resource/tool schemas, `arrivals.rs`, arrival migration, and delivery transaction. |
| Identity and ownership | Auth middleware, browser/API/MCP identity maps, mailbox ownership migrations, every read/mutation, results, cursors, streams, and account storage. |
| Hosted infrastructure | Pulumi stacks, CNPG, Ceph claims, Authentik resources, secret/key sources, shared NATS prefixes, Tekton, and homelab PAC registration. |

## High-risk boundaries

- Check every mail/domain mutation's JetStream route, including bootstrap, policy, mailbox creation, SMTP intake, and delivery.
- Restrict direct PostgreSQL writes outside JetStream to auth protocol state; preserve mail commit and durable-result guarantees.
- Check verified issuer/subject identity across browser, API, and MCP; reject email-based identity and first-login local mailbox assignment.
- Check owner access across lists, raw MIME, replies, From, entries, submissions, request keys, results, events, and page/resume cursors.
- Require two-user negative checks and normal address-based cross-user delivery without mailbox-directory disclosure.
- Check global domain reads, administrator-group-only management, and no administrator bypass for another user's mail.
- Check PKCE, state, nonce, cookies, exact Origin, CSRF, logout, revocation, and bounded stream auth leases.
- Check local hosted-MCP issuer/resource binding, human consent, refresh rotation, durable grants, encrypted keys, and direct-IdP-token rejection.
- Check zero hosted-MCP clock skew: admission fails at `exp`, and active streams/Events share that deadline without grace.
- Distinguish verification-key overlap from token lifetime; retained keys never authorize an expired token.
- Check account-specific caches, drafts, queries, cursors, and uncertain commands; prohibit automatic no-auth draft import.
- Check session verification before workspace/query/storage mount and exact origin/environment/user OIDC namespaces with the prior shared no-auth namespace.
- Check generation cancellation, request/stream aborts, query-memory cleanup, cross-tab locks, and 10-second online session rechecks.
- Check expected-user headers against independently authenticated cookie users before every OIDC gRPC-Web handler; header values never supply authority.
- Check cookie-switch races, canonical single-header validation, 401 client locks, generation aborts, and peer notifications.
- Preserve native bearer/MCP behavior and optional-but-validated expected-user headers for disabled callers.
- Check actual fixed-size logout records before OIDC unlock, independent writes, one-store durability, legacy flags, and fail-closed storage errors.
- Require server success or 401 before barrier clearance; document both-store-plus-server failure without universal reload-persistence guarantees.
- Distinguish verified-open-tab offline access until expiry from OIDC offline reload, which requires online verification.
- Check no-auth offline reload requires the origin-bound verified-local-mode tab marker; never treat stored authenticated identity as authorization.
- Check separate preview/production namespaces, CNPG, Ceph claims, secrets, auth applications, keys, and every NATS resource prefix.
- Check runtime NATS resource creation without durability relaxation; report live ACL, capacity, and replica facts as unverified without evidence.
- Check fixed main preview and trusted signed stable-tag promotion of prior immutable images without a rebuild.
- Reject Tekton unit, integration, browser, infrastructure, and smoke-test tasks; preserve local validation and separate manual operator gates.
- Check production data protection, actual Ceph retention, isolated recovery, and private SMTP without public MX or SMTP submission claims.
- Check fixed environment config, immutable image digests, and exclusive secret NATS credential methods; anonymous hosted configuration must fail.
- Check hosted `INBOX_NATS_NAMESPACE` and three-replica declarations without claims of proven broker ACLs, capacity, or failure domains.
- Check SMTP opt-in requires both namespace and pod selectors; default Service and policy expose no SMTP.
- Check the root-only Docker build context and pinned remote Waltr dependency without a sibling context or vendor tree.
- Distinguish cross-repository homelab PAC declarations from authorized live registration or apply.
- Check S3-before-PostgreSQL-commit acceptance followed by durable NATS confirmation before unary success or SMTP `250`.
- Check that `PubAck` means queued, not complete.
- Check transaction outbox recovery, consumer idempotency, redelivery, and lost confirmations across restart.
- Check chunked payload staging, capacity backpressure, and protection against silent expiry of pending payloads.
- Distinguish unlimited transport redelivery from the eight-attempt application delivery budget.
- Check result/staging capacity growth without automatic garbage collection or implied deletion authority.
- Check one-time persisted bootstrap identity and policy across restart and changed `INBOX_DOMAIN`.
- Check fresh-instance scope; do not infer an upgrade path, prior-job conversion, or instance deletion authority.
- Check immutable normalized domain names, UUID references, and mailbox uniqueness within each domain.
- Check all flag combinations: OIDC mailbox creation requires receive OR send at the consumer; no-auth permits both false.
- Check that later domain disablement preserves owner reads and local draft access.
- Check exact `/auth/session` no-store responses, `/auth/login`, `/auth/callback`, and same-origin `/auth/logout`.
- Require exact Origin and `Sec-Fetch-Site: same-origin` for every cookie gRPC-Web RPC, including reads and streams.
- Check native bearer-only access and hosted MCP local OAuth independently from browser session cookies.
- Check native Authentik `2025.10.2` admission: exact `typ: JWT`, exact configured `azp`, bounded nonempty `uid`, and required scope.
- Reserve `uid`, `azp`, and `scope` from ID-token mappings; audience overlap never establishes access-token identity.
- Check every public page/change/arrival cursor binds namespace, principal, and kind in both modes.
- Check cross-namespace OIDC cursor rejection with cloned database keys and explicit fresh-query/watch recovery for old unwrapped cursors.
- Check consumer-authoritative new acceptance, SMTP RCPT receive policy, and no send-policy restriction on external SMTP From.
- Check accepted identical retries and durable delivery after policy changes; retain access to current mail.
- Check unique entries, recoverable leases, bounded retries, and sender-scoped idempotency under concurrency.
- Check all-or-nothing rejection for unknown, external, or receive-disabled recipients and the absence of external relay.
- Check raw MIME preservation, plain-text display, reply headers, and size limits.
- Check query-bound pagination and explicit read-state updates.
- Check cached domain reads, online-only management, and draft preservation across domain invalidation, cache eviction, and reconnect.
- Check IDs-only gRPC-Web streams, cursor resets, full resynchronization, heartbeats, and bounded slow-consumer behavior.
- Check persistent cache and Solid Query invalidation without draft loss or the unary timeout on streams.
- Check runtime public keys and telemetry allowlists against actual payloads.
- Check loopback host ports, private stores, and paired backup assumptions.
- Check same-origin `/mcp`, generic Waltr `2026-07-28` transport, and the absence of legacy fallback.
- Check resource URI scope, query-bound cursors, canonical next links, bounded output, and reads without read-state effects.
- Check domain directory/identity resources and domain-aware management tools against discovered MCP schemas and API fields.
- Check required MCP domain URIs, both policy booleans, management request keys, committed projections, resource links, and same-key uncertain retries.
- Distinguish required MCP mailbox domain selection from the empty gRPC domain's persisted bootstrap default.
- Check `next`, UTF-8 byte-offset continuations, explicit 256 KiB resource failures, and the smaller 128 KiB MCP send-body cap.
- Check mutation resource references, required idempotency keys, explicit reply recipients, and durable acceptance versus terminal delivery.
- Check that no durable draft tool or implicit raw MIME resource extends the approved adapter surface.
- Check Inbox-only ledger inserts within the delivery transaction; reject Sent/read-state/duplicate arrivals and historical backfill.
- Check commit-ordered mailbox cursors, stable generation/event IDs, restart replay, explicit gaps, bounded tails, and cancellation.
- Check Events client opt-in and IDs/resource-URI-only payloads without telemetry exports.
- Check rejection of every supplied `maxAgeMs`, including zero; distinguish `Active.truncated` resets from internal page backlog.
- Distinguish native OpenCode event wakes from resource refresh hints, configuration, and protocol-only tests.
- Check untrusted mail handling and at-least-once deduplication without claims of a restart-persistent agent queue.

## Evidence expectations

Use the test guide's command matrix and contract scenarios for the affected surfaces.
Report exact evidence and gaps separately from intended behavior.
Report source-versus-contract conflicts instead of silently relaxing requirements.
Check relative links and command names when documentation changes.

No deployment, external send, secret rotation, data deletion, or restore authority follows from this skill.
