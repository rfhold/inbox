# System shape

This document describes intended MVP behavior. Tests and source review establish implementation conformance separately.

## Components

| Component | Owns |
| --- | --- |
| Rust server | RPC service, MCP adapter, private SMTP intake, command consumers, MIME parse and composition, static assets, and public runtime configuration. |
| PostgreSQL | Users, auth protocol state, domains, owned mailboxes, messages, delivery work, arrival ledger, command idempotency, and outbox. |
| S3 storage | Original or server-composed raw MIME; Garage locally and Ceph for hosted environments. |
| NATS JetStream | Durable mutation commands, results, delivery work transport, change events, and chunked payload staging. |
| SolidJS SPA | Owner-scoped mailbox views, plain-text composition, submission status, account-specific local drafts, and offline cache. |
| Compose | A single-node private stack with persistent volumes. |
| Pulumi and Tekton | Intended isolated preview/production resources and immutable-image release tracks. |

The [schema](../../proto/inbox/v1/inbox.proto) owns RPC shapes and field semantics.
The [mail contract](spec/mail.md) owns acceptance and delivery requirements.
The [email-domain contract](spec/domains.md) owns domain identity, bootstrap, and independent receive/send policy.
The [command contract](spec/commands.md) owns mutation routing and durable confirmation.
The [change stream contract](spec/changes.md) owns browser cache invalidation and client resynchronization.
The [client contract](spec/client.md) owns browser state and telemetry boundaries.
The [MCP contract](spec/mcp.md) owns the resource-centric adapter.
The [arrival contract](spec/arrivals.md) owns durable Inbox arrival Events.
The [auth contract](spec/auth.md) owns identity, authorization, and the narrow direct-PostgreSQL auth-state exception.
The [deployment guide](../operations/deployment.md) owns hosted infrastructure and promotion policy.

## Data flow

1. An RPC, MCP, or SMTP adapter publishes a JetStream command, with a staged payload reference when necessary.
2. A command consumer validates the command and performs its mutation.
3. For mail acceptance, the consumer completes S3 PUT before the PostgreSQL acceptance transaction commits.
4. The transaction persists domain state, command idempotency, and outbox records for durable results and follow-up work.
5. An outbox publisher sends those records to JetStream and recovers publication after a crash.
6. The adapter returns unary success or SMTP `250` only after the required commit and durable NATS result.
7. Delivery consumers process JetStream commands with replay-safe PostgreSQL state.

Read RPCs access stores directly. `WatchChanges` exposes an IDs-only gRPC-Web server stream.
MCP resource reads also access stores directly and never change read state.
Delivery transactions record new Inbox entries in a separate durable arrival ledger for `mail.received` Events.
Local drafts remain device-local; they do not enter the server command path.

Unique mailbox entries prevent duplicate delivery through job replay.
An S3 object without a PostgreSQL reference remains an orphan; the MVP permits inventory only, not deletion.

## Trust boundary

Explicit no-auth mode preserves the trusted local shared workspace.
OIDC mode requires per-user mailbox ownership on all read, mutation, result, and event paths.
The global domain directory remains readable; only the configured administrator group manages domains.
Administrator status grants no access to another user's mail.
API origination requires both sender ownership and the sender domain's send flag.
The app and SMTP listener bind to host loopback through Compose. PostgreSQL, Garage, and NATS remain private to its network.
Do not expose this stack to an untrusted network.

Hosted HTTPS access requires OIDC and hosted MCP OAuth under the auth contract.
Private trusted SMTP intake remains separate from browser OIDC, public MX, and authenticated SMTP submission.
External relay, public SMTP bridges, imports, IMAP, POP, JMAP, and search remain outside scope.
Attachment composition, HTML display, and offline send also remain outside scope.
