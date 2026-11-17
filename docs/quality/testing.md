# Test guide

## Commands

Run commands from the repository root unless a row states otherwise.
Install the pinned Rust toolchain and pnpm dependencies first.
These commands follow workspace manifests; a command entry does not assert test coverage or a successful run.

| Command | Purpose | Prerequisites |
| --- | --- | --- |
| `cargo fmt --all -- --check` | Rust format check. | Rust formatter. |
| `cargo check --workspace --locked` | Rust type and build check. | Dependency access. |
| `cargo test --workspace --locked` | Rust tests. | Isolated PostgreSQL, Garage, and NATS for integration tests, if required. |
| `cargo test -p inbox-server --test mcp --locked` | HTTP adapter discovery, strict inputs, and absent-broker behavior. | Dependency access and an ephemeral loopback test listener. |
| `pnpm generate` | Regenerate TypeScript protobuf bindings. | Buf CLI and installed dependencies. |
| `pnpm --dir web typecheck` | TypeScript check. | Installed dependencies. |
| `pnpm --dir web test` | Vitest tests. | Test files and isolated browser storage fixtures. |
| `pnpm web:build` | TypeScript check and production asset build. | Installed dependencies. |
| `pnpm web:e2e` | Playwright tests. | Test configuration, browser binaries, and an isolated local stack. |
| `pnpm --dir web e2e:offline` | Offline, stream, and visual browser fixtures. | Production web assets and Playwright browser binaries. |
| `pnpm --dir web e2e:integration` | Browser tests against the integration preview. | Production web assets, Playwright browsers, and isolated dependencies as each test requires. |
| `pnpm --dir web exec playwright test --config playwright.ux.config.ts` | Email routes, history, drafts, offline deep links, and dismissible notification fixtures. | Installed dependencies and Playwright browser; starts a local production web preview with mocked mail RPCs. |
| `bun run --cwd infra/pulumi build` | Pulumi TypeScript check without an apply. | Installed infrastructure dependencies. |
| `INBOX_TEST_DISPOSABLE_HA=1 tests/acceptance/.venv/bin/python tests/acceptance/ha_local.py` | Opt-in two-process replica, same-key, stream, and SMTP drain checks. | Docker, built `target/debug/inbox-server`, acceptance Python dependencies; creates and removes only unique task-owned containers, using synthetic data and loopback telemetry. |

Check that test files and Playwright configuration exist before test execution.
Report absent coverage or configuration as a gap, not a pass.
Do not target live telemetry collectors or real mailboxes during tests.
The [acceptance guide](../../tests/acceptance/README.md) owns Python setup, public command/stream checks, and opt-in fault commands.
Fault tests require exclusive stack access and explicit execution authority.
Observable restart tests do not replace deterministic commit-to-publication, pending-admission, bootstrap, or slow-consumer evidence.

## Required contract scenarios

Application HA validation is local and opt-in, never a CI stage.
Use two real app processes against one disposable PostgreSQL/NATS/S3 stack.
Exercise concurrent identical request keys through different replicas and verify one effect,
cross-replica reads/cursors, stream cancellation and resume, and admitted SMTP DATA during SIGTERM.
Check readiness withdrawal, rejected new admission, current worker completion, and forced deadlines.
Run authenticated session/OAuth/revocation cases separately; no-auth tests do not prove them.
Do not use personal mail, production dependencies, or shared streams for fault tests.
Record forced-deadline uncertainty and accepted mixed-version web disruption as limits, not passes.

| Area | Evidence required |
| --- | --- |
| Mutation boundary | Every mail/domain mutation, including bootstrap, enters JetStream; only auth protocol state uses the narrow direct-PostgreSQL exception. |
| Durable acceptance | S3 failure, PostgreSQL failure, or absent durable NATS results prevent unary success and SMTP `250`; `PubAck` alone never completes them. |
| NATS outage | Broker loss and capacity exhaustion fail closed or apply backpressure without direct writes or silent payload loss. |
| Crash windows | S3-only objects survive as orphans; PostgreSQL commit before result publication recovers through the outbox. |
| Lost confirmation | Lost `PubAck`, lost result confirmation, and publisher restart preserve command identity and replay safety. |
| Redelivery | Duplicate commands and results produce no duplicate mailboxes, mailbox entries, or recipient effects. |
| Payload staging | Near-limit MIME uses chunked object-store staging below the broker payload limit; pending payloads survive restart and retention pressure. |
| Domain identity | UUIDs, immutable normalized ASCII DNS names, timestamps, duplicate rejection, and independent receive/send flags. |
| Domain bootstrap | A fresh instance seeds once through NATS; restart or changed `INBOX_DOMAIN` preserves default identity and disabled policy. |
| Mailbox domains | Same local part across domains succeeds; same-domain duplicates fail; empty gRPC domain selects the persisted default. |
| Domain policy | All four flag combinations; consumer-authoritative acceptance; OIDC rejects both-disabled mailbox creation; local no-auth permits it; owner reads and drafts remain accessible. |
| Policy retry boundary | Accepted identical retries and durable delivery complete after policy changes; new acceptance uses current flags. |
| Recipient validation | One unknown, external, or receive-disabled recipient rejects the entire API submission without partial acceptance. |
| Idempotency | Identical and concurrent retries return one submission; changed payloads conflict within the sender scope. |
| Worker recovery | Lease expiry permits recovery; bounded retries terminate; replay creates no duplicate mailbox entries. |
| SMTP | Receive policy at RCPT and consumer acceptance; external From ignores send policy; raw-size and acknowledgement-loss limits remain unchanged. |
| MIME | Raw bytes retain HTML and attachments; UI text never executes HTML or header injection. |
| Reply | First Reply-To fallback, RFC identifiers, and server-generated References. |
| Read state | Fetch has no side effect; explicit repeated state updates remain idempotent. |
| Pagination | Equal timestamps, descending ID ties, page limits, and cross-query token rejection. |
| Offline | Asset shell, admitted cached views, absent-cache errors, stale labels, disabled server mutations, and mode-specific reload limits. |
| Drafts | Cache eviction, version changes, reconnect, and uncertain submission outcomes preserve the correct draft state. |
| Transport | Generated client calls use gRPC-Web against the same-origin Rust server. |
| Stream transport | Real gRPC-Web server streams survive beyond the unary timeout; tests do not substitute SSE or WebSocket transport. |
| Stream recovery | Fresh watches and stale/invalid inner stream positions reset; invalid context wrappers fail admission and require a fresh watch. |
| Slow consumers | Bounded buffers reset/resynchronize slow clients; heartbeat, backoff, cancellation, and reconnect release resources. |
| Live invalidation | Domain IDs-only events invalidate persistent domain records, policy views, selectors, and affected mailbox queries alongside mail invalidation. |
| Offline recovery | Reconnect resumes the stream and resynchronizes before cache trust; invalidation never deletes or overwrites drafts. |
| Stream privacy | Events contain only cursor metadata, kind, and applicable IDs; no content, addresses, or telemetry exports. |
| MCP protocol | Same-origin `/mcp` uses generic Waltr `2026-07-28` transport; legacy initialization never succeeds through a fallback. |
| MCP resources | Email mailbox IDs and canonical URIs, UUID mailbox rejection, owner checks, `next` links, default limit 50/max 100, 32 KiB UTF-8 body chunks, and explicit 256 KiB resource failure. |
| MCP pagination | Opaque cursors bind to the query; invalid, cross-mailbox, and cross-folder cursors fail explicitly. |
| MCP mutations | Exact required fields, unknown-field rejection, byte/address limits, matching text/structured outcomes, resource links, and unchanged JetStream confirmation. |
| MCP domain management | Exact domain projections, required fields, explicit canonical domain URIs, committed projections/resource links, bounds, and same-key uncertain retries. |
| MCP mail semantics | Reads never mark mail read; replies use explicit To/Cc and source-derived RFC headers; external recipients reject the whole submission. |
| Arrival transaction | A new Inbox entry and its arrival record commit or roll back together within the NATS consumer transaction. |
| Arrival identity | Sent entries, read-state updates, duplicate delivery, and historical entries produce no arrival; replay preserves entry UUID event IDs. |
| Arrival order | Concurrent delivery commits preserve mailbox order; restart preserves generation; a fresh watch starts at the current watermark. |
| Arrival replay | Reconnect delivers at least once; malformed, cross-mailbox, future, different-generation, and gap cursors never lose events silently. |
| Arrival bounds | Pages contain at most 100 arrivals; one-second idle tails, 15-second heartbeat checks, owned cancellation, and explicit slow-consumer failure. |
| Events privacy | Event input selects an Inbox URI; payloads contain only Inbox/entry URIs, which include the selected mailbox email, never bodies, subjects, or recipient lists. These private URIs never enter telemetry. |
| Events opt-in | The generic extension defaults off; explicit client opt-in enables `mail.received` without a custom Inbox binding tool. |
| Events reset | `Active.truncated: true` signals a reset, not an internal page backlog; every supplied `maxAgeMs` value fails, including zero. |
| OpenCode wake | A flag-enabled Events-capable runtime receives a native watch notification and actually wakes an agent session. |
| Privacy | Local telemetry capture excludes domain names, addresses, policy inputs, application IDs, mail content, and secrets. |
| Backup | An isolated paired restore includes PostgreSQL, both Garage volumes, NATS, and private configuration; outbox and pending commands recover. |

Use failure injection around S3 PUT, PostgreSQL commit, command/result publication, consumer acknowledgement, and recipient entry commit.
Use synthetic MIME fixtures near the 25 MiB raw limit, with transport overhead above it.
Use a local collector to inspect OTLP and Faro payloads, including errors and automatic browser events.
Domain tests target fresh instances, not an upgrade or prior-identity conversion path.
Test isolation never authorizes browser draft deletion or unrelated stack resets.

## Auth and environment security

These scenarios establish conformance to the [auth contract](../architecture/spec/auth.md) and [deployment contract](../operations/deployment.md).
Use two distinct verified subjects plus an administrator-group account.
A single authenticated happy path does not establish user isolation.

| Area | Required evidence |
| --- | --- |
| Mode boundary | Explicit no-auth workspace remains separate; partial OIDC configuration fails closed; first login assigns no local mail or drafts. |
| Stable identity | Browser, API, and hosted MCP resolve the same issuer/subject; email changes never merge or transfer accounts. |
| Domain authority | Both users read domains; normal users cannot create domains or change flags; administrators cannot read another user's mail. |
| Mailbox ownership | Each user creates and lists only owned mailboxes in enabled domains; duplicate addresses fail across users within one domain. |
| Creation policy modes | OIDC creation requires receive OR send at the consumer; no-auth permits both false; later disablement preserves reads and drafts. |
| Two-user reads | User B cannot read user A's mailbox, entries, body continuations, raw MIME, read state, submissions, or durable results. |
| Two-user mutations | User B cannot use A's From mailbox, reply source, entry, command identity, or request key to bypass ownership. |
| Address delivery | A sends to B's address; B receives its own entry without either user gaining mailbox-directory or submission access. |
| Query isolation | Both modes bind namespace, principal, and kind; cross-user/namespace/query reuse fails, including OIDC databases with cloned cursor keys. |
| Cursor transition | Old unwrapped page/change/arrival cursors fail; clients start fresh queries/watches and reconcile; ordinary restart preserves current-format cursor context. |
| Stream isolation | Changes and arrival Events expose only authorized IDs; logout, expiry, and revocation terminate stream leases. |
| Browser login | PKCE S256, state, nonce, correlation cookie, exact callback, safe return path, atomic consumption, and replay rejection. |
| Browser CSRF | Missing, duplicate, malformed, or foreign Origin values reject cookie mutations and logout. |
| Browser RPC CSRF | Every cookie-authenticated gRPC-Web read, mutation, and stream requires exact Origin and `Sec-Fetch-Site: same-origin`; native RPCs require bearer credentials. |
| Expected account | OIDC cookie RPCs require one canonical expected-user UUID; missing/duplicate/malformed/mismatched headers fail before reads, writes, and stream handlers. |
| Cookie switch race | Session preflight for A followed by cookie B and expected-user A fails with 401; the client locks, aborts its generation, and notifies peers. |
| Expected-account modes | The header never authenticates; OIDC native bearer/MCP remain unaffected; disabled callers can omit it, but supplied values must match the fixed local UUID. |
| Session probe | `/auth/session` returns no-store responses with exact mode/authentication fields; disabled mode uses the fixed local UUID and administrator status. |
| API bearer | Algorithm, type, issuer, audience, expiry, subject, scopes, and ID-token confusion tests fail closed. |
| Authentik native profile | Exact `typ: JWT`, exact configured `azp`, bounded nonempty `uid`, and required `scope`; reject `at+jwt` and ID tokens. |
| Provider mappings | Authentik `2025.10.2` supplies `uid`, `azp`, and `scope` only through access-token conversion; property mappings never inject them into ID tokens. |
| Hosted MCP OAuth | Metadata/challenge, public-client identity, automatic approval only for the exact resource/scope, rejected manual decisions, exact redirect, PKCE, one-shot code, refresh rotation, and reuse rejection. |
| MCP credentials | Direct IdP tokens fail; wrong issuer/resource/client/scope and expired or revoked local credentials fail. |
| MCP expiration boundary | At `exp`, new token admission fails and active response streams/Events end; zero clock skew permits no post-expiry grace. |
| Auth persistence | Only protocol state bypasses NATS; auth-store loss fails access closed; mail acceptance still requires commit and durable result. |
| Key lifecycle | Encrypted signing material, separate wrapping keys, verification overlap, restart, rotation, and isolated restore preserve policy; key overlap never extends token validity. |
| Account storage | Account switch, logout, expiry, cross-tab changes, reconnect, and schema changes never expose another account's cache or drafts. |
| Browser admission | Verify `/auth/session` before workspace/query/storage mount; validate environment before OIDC namespace selection. |
| Automatic login | Initial verified OIDC 401 starts login once; malformed/failed probes, local mode, offline reload, logout, and lost verified sessions never trigger it. |
| Account namespace | OIDC uses origin, encoded environment, and user UUID; disabled mode preserves `inbox:${origin}:shared` without automatic import. |
| Account cancellation | Abort generation/request/stream work and clear query memory on account switch, logout, expiry, or failed session checks. |
| Cross-tab checks | BroadcastChannel/storage locks plus focus, reconnect, and 10-second online checks never restore stale-account views. |
| Logout reservation | Reserve actual fixed-size `0` records in localStorage and IndexedDB before OIDC unlock; read/reservation errors fail closed. |
| Logout durability | Independent writes set `1`; one verified/committed store suffices; either pending record or legacy pending flag blocks unlock until server success or 401. |
| Logout failure boundary | Cover quota and one-store failure; both write failures plus failed server logout keep the tab locked with a warning, without universal reload-persistence claims. |
| Authenticated offline | Verified open tabs permit cached reads/drafts until expiry; offline reload locks and requires online verification, not persisted identity. |
| Local offline reload | Only the explicit origin-bound verified-local-mode tab marker admits no-auth reload; absent marker requires online mode verification. |
| Command storage | Unknown-result retry state stays account-specific; no automatic cross-account replay or no-auth draft import occurs. |
| Infrastructure isolation | Separate namespaces, CNPG databases, raw/backup claims, secrets, auth applications, and keys across preview/production. |
| Infrastructure configuration | Fixed stack identities, immutable digest, explicit auth/admin/bootstrap values, exclusive secret NATS credentials or explicit preview-only anonymous opt-in, and production protection. |
| Shared NATS | Every resource and outbox target uses its environment prefix; runtime creation preserves strict retention and durability. |
| Release policy | Main targets one fixed preview; trusted signed stable tags promote prior immutable multiarchitecture images without a rebuild. |
| Pipeline boundary | Tekton contains clone, secret scan, build/manifest, release-policy, promotion, and deploy tasks, not application or infrastructure tests. |
| Private SMTP | Network restrictions deny public intake; no public MX or authenticated SMTP submission follows from OIDC. |
| SMTP opt-in | Default Service/NetworkPolicy expose no SMTP; opt-in requires both namespace and nonempty pod labels within one ingress peer. |
| Hosted recovery | Isolated restore verifies CNPG, Ceph raw MIME, NATS work/results, outbox, auth keys, pending delivery, and cursor recovery together. |

Run unit, integration, browser, and infrastructure checks locally with isolated fixtures.
Do not add those checks, or smoke tests, to Tekton pipelines.
Manual browser OAuth evidence requires separate authorization for the exact environment and identity provider.
Record login, consent, exchange, refresh, logout, revocation, and two-user results separately.
Mocked protocol tests never prove a real browser or provider flow.
Backup/restore evidence requires exact-target approval and an isolated recovery destination.
Verify actual Ceph retention, shared broker ACLs, capacity, and replica policy before production readiness claims.
Declarations and generated previews never establish live enforcement.

## MCP and Events evidence

Use the [MCP client guide](../architecture/mcp.md) for project configuration and the native watch call.
Verify adapter schemas before a test supplies mutation arguments or body continuation parameters.
Record exact resource bounds, tool input shapes, and any unsupported resource paths in the evidence report.

The [adapter](../../crates/inbox-server/src/mcp.rs) defines unit tests for strict URIs, Unicode chunks, byte limits, schemas, cursors, and event filters.
The [HTTP adapter tests](../../tests/backend/mcp.rs) define discovery and absent-broker checks through an ephemeral loopback listener.
Their `http_resources_mutations_events_replay_and_cancel` case requires explicit opt-in through Rust's ignored-test mechanism.
That case requires an exclusive isolated PostgreSQL, NATS, and S3 stack and retains synthetic fixtures.
It uses `INBOX_TEST_DATABASE_URL`, `INBOX_TEST_NATS_URL`, and the `INBOX_TEST_S3_*` variables named in its source.
Do not execute that case against personal mail or shared dependencies.
These definitions do not establish execution results or an OpenCode agent wake.

Separate these checks:

1. Protocol discovery, resources, tool schemas, and client Events opt-in.
2. Durable entry/arrival transaction, concurrent order, replay, restart, and explicit cursor gaps.
3. A native `mail.received` watch that actually wakes an OpenCode agent after synthetic delivery.

An HTTP stream response alone does not prove an agent wake.
A feature flag or resource update notification does not prove a native Events subscription.
Use a fresh Events-capable runtime for the wake check.
OpenCode v2.2.7 lacks the native Events tool; the [client guide](../architecture/mcp.md) states the minimum version.
Do not upgrade the global installation or restart a shared service without separate authority.
Keep test mail, event URIs, and application IDs out of telemetry.
Never treat host subscription state as a durable agent work queue.

## Test ownership and isolation

- Keep Rust unit tests beside their modules under `crates/inbox-server/src/`.
- Keep integration fixtures independent from personal `.env` and persistent mail volumes.
- Keep browser storage tests under `web/` with disposable IndexedDB state.
- Test offline behavior through browser network controls, not only `navigator.onLine` stubs.
- Control clocks and worker failures for deterministic lease and retry tests.
- Use isolated JetStream state and bounded test consumers for command replay and stream recovery tests.
- Capture actual broker configuration before assertions about stream capacity, retention, or staged-payload lifetime.
- Preserve failure evidence privately with synthetic data only.

## Evidence report

Report exact commands, results, unrun checks, missing fixtures, and unresolved contract conflicts.
Do not place transient pass claims or verification dates in canonical documentation.
Update the [local review skill](../../.agents/skills/reviewing-changes/SKILL.md) if a new high-risk boundary appears.
