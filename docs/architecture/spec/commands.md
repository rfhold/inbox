# Command and confirmation contract

This document defines approved server mutation behavior, not evidence of implementation completeness.
The [schema](../../../proto/inbox/v1/inbox.proto) owns the public RPC shapes.

## Mutation boundary

Every mail and domain runtime mutation enters NATS JetStream as a command:

| Mutation | Required route |
| --- | --- |
| Domain bootstrap | Startup adapter to command consumer; one persisted bootstrap identity. |
| Domain creation and policy updates | RPC or MCP adapter to command consumer. |
| `CreateMailbox` | RPC or MCP adapter to command consumer. |
| `SubmitMessage` | RPC or MCP adapter to command consumer, with staged payloads when necessary. |
| SMTP intake | SMTP adapter to command consumer, with staged raw MIME. |
| `SetReadState` | RPC or MCP adapter to command consumer. |
| Recipient delivery jobs | Durable follow-up commands to delivery consumers. |

Read RPCs and MCP resource reads remain direct. Device-local drafts remain outside this boundary.
Internal outbox publication status updates and schema migrations do not constitute public command bypasses.
Email-domain bootstrap is a runtime mutation, not an exception for direct SQL writes.
No RPC, MCP, SMTP, or delivery path writes domain state through a direct fallback when NATS fails.
Only [auth protocol state](auth.md#persistence-exception) uses direct PostgreSQL transactions.
This exception never weakens mail acceptance, ownership checks, or durable result confirmation.
OIDC commands and results bind the authenticated actor and authorized target scope.
Request-key reuse by another principal never reveals the prior principal's durable result.

## Acceptance versus completion

A JetStream `PubAck` confirms only durable command admission, not mutation completion.
Every mutation result requires a durable NATS confirmation.
Unary success and SMTP `250` require both the required PostgreSQL/S3 commit and that durable result.
A timeout or lost confirmation leaves an uncertain outcome, not proof of rejection.
`UNAVAILABLE` or `DEADLINE_EXCEEDED` does not prove that a command failed before commit.
The adapter waits up to 60 seconds for a durable result; an unresolved wait returns `UNKNOWN`, not permanent rejection.
Transport redelivery preserves command identity and payload identity through consumer idempotency.
Caller retries preserve the application idempotency key; separate attempts can have distinct command IDs.
The [mail contract](mail.md#recipients-and-idempotency) also governs sender-scoped submission keys and payload conflicts.

For mail acceptance, the consumer completes immutable S3 PUT before PostgreSQL commits.
The acceptance commit records metadata and durable recipient work; it does not imply that every recipient already received an entry.
Submission and recipient status remain the source for delivery progress.

## Outbox and replay

The consumer commits domain state, its idempotent result, and outbox records in one PostgreSQL transaction.
Outbox records cover the durable result and required follow-up commands or change events.
The publisher waits for JetStream acknowledgement before it records publication progress.
After a commit-to-publication crash, the publisher replays the outbox.
After an acknowledgement-to-bookkeeping crash, duplicate publication remains safe through consumer idempotency.
PostgreSQL command journals and application request keys protect replay beyond the broker's finite duplicate-detection window.

Delivery commands use the same durable result and replay rules.
Unique mailbox entries prevent duplicate recipient entries.
Bounded retries and recoverable work state survive restarts.
The system provides at-least-once transport, not exactly-once execution or SMTP acceptance.

`INBOX_MUTATIONS` is a durable pull consumer with explicit acknowledgements and one unacknowledged command at a time.
Its acknowledgement wait is 120 seconds; the worker sends progress every 30 seconds during command execution.
Transport redelivery has no delivery-count limit.
Application recipient delivery remains bounded at eight attempts with recoverable leases.
Unlimited transport redelivery does not imply unlimited application delivery attempts.

## Replica and shutdown boundary

Replicas share PostgreSQL, S3, and the environment's JetStream resources.
The durable mutation consumer remains globally serialized; extra app replicas do not increase its concurrency.
No sticky routing or process-local authoritative mail state is required.

On termination, readiness fails immediately while liveness stays independent.
The process continues admission during a 10-second endpoint-withdrawal window.
It then closes new HTTP and SMTP admission, cancels change and MCP streams, and
allows admitted HTTP calls and accepted SMTP DATA to finish concurrently.
Idle SMTP sessions receive a temporary failure; SMTP `250` still requires durable confirmation.
The admission deadline is 110 seconds from termination.
Workers then stop new pulls and finish current work with the outbox publisher
available for up to 120 seconds, no later than 230 seconds from termination.
Database and telemetry closure share a ten-second deadline, no later than 240 seconds from termination.
Kubernetes and Compose allow 270 seconds of termination grace.
Deadlines are absolute, not renewed for each connection or task.

Deadline expiry can cancel a response or interrupt processing.
Force cancellation covers response bodies and connection I/O, not only response headers.
Before worker shutdown, the server waits up to five seconds for expired HTTP connection tasks to exit.
This wait consumes the total worker deadline; failure returns a shutdown error instead of advancing the graceful worker phase.
Durable commands, journals, leases, and outbox replay retain their existing recovery semantics.
An interrupted caller must treat its outcome as uncertain and retry with the same application key.
This is bounded graceful drain, not a lossless shutdown or a zero-disconnection guarantee.

## Large payloads and capacity

Large command payloads use chunked JetStream object-store staging.
Commands carry a reference, not a single 25 MiB NATS message.
The consumer reads the staged payload before it archives accepted raw MIME in Garage.
The 25 MiB immutable raw MIME limit remains unchanged.

`docker/nats.conf` defines these local Compose broker limits:

| Limit | Configured value |
| --- | --- |
| Maximum NATS payload | 1 MiB. |
| JetStream file storage | 4 GiB. |
| JetStream memory storage | 64 MiB. |

The [pipeline configuration](../../../crates/inbox-server/src/pipeline.rs) defines these file-backed resources, each with one replica:

| Resource | Subject or backing stream | Capacity | Retention and discard |
| --- | --- | --- | --- |
| `INBOX_COMMANDS` | `INBOX.commands.*` | 64 MiB | `WorkQueue`, no age expiry, `DiscardNew`. |
| `INBOX_RESULTS` | `INBOX.results.*` | 1 GiB | `Limits`, no age expiry, `DiscardNew`. |
| `INBOX_CHANGES` | `INBOX.changes` | 64 MiB | `Limits`, seven-day maximum age, `DiscardOld`. |
| Object store `INBOX_STAGING` | `OBJ_INBOX_STAGING` | 1 GiB | No age expiry, `DiscardNew`. |

Work-queue acknowledgement removes completed commands through normal JetStream retention.
Change events can expire or leave the bounded history; clients recover through the [reset contract](changes.md#cursor-and-recovery).
Command, result, and change streams cap individual messages at 1 MiB.
The staging object store uses 128 KiB chunks.
Hosted resources require distinct environment prefixes across streams, subjects, object stores, consumers, cursors, and outbox targets.
The [deployment guide](../../operations/deployment.md#shared-nats-isolation) owns shared-broker isolation and readiness requirements.
The local resource names and replica counts above do not establish live shared-broker configuration.
Serialized commands and staged confirmations cap at 32 MiB; raw MIME still caps at 25 MiB.
Confirmations above 512 KiB use staged object references instead of inline result content.

The application performs no automatic result or staging garbage collection.
Abandoned payloads, resolved payloads, and duplicate large confirmation objects remain in staging.
No age expiry does not imply unlimited capacity.
Pending payloads must not expire silently.
Capacity exhaustion applies backpressure or fails admission closed; it never authorizes a direct-write fallback.
Result or staging capacity exhaustion requires an operator-approved future maintenance procedure or capacity change.
This contract grants no purge or deletion authority for streams, staging objects, S3 objects, or volumes.

## Fresh-instance scope

The persisted email-domain feature targets fresh instances and does not promise an upgrade or prior-job conversion path.
Within an instance, restart and replay preserve accepted work and prevent duplicate mailbox entries.
The [email-domain contract](domains.md) defines one-time bootstrap and policy behavior across restarts.
