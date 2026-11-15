# Durable mail arrival contract

`mail.received` means that a new Inbox entry exists, not merely that a message view changed.
The [MCP contract](mcp.md#events) owns event input and payload resource URIs.
The [change stream](changes.md) remains the browser's cache invalidation mechanism.

## Transaction boundary

The NATS delivery consumer records an arrival only when it creates a new Inbox entry.
The same PostgreSQL transaction commits that entry, its arrival record, command idempotency, and the transaction outbox.
Rollback leaves neither an entry nor an arrival record.
Sent entries, read-state changes, and duplicate delivery attempts create no arrival.
No migration backfills historical entries into the arrival ledger.
The service performs no automatic arrival deletion.

The entry UUID also serves as the stable event ID.
The ledger retains the mailbox ID, entry ID, commit-ordered position, and occurrence timestamp.
It stores no subject, address, or body.
Concurrent transactions never advance a visible watermark past an uncommitted arrival.
The [command contract](commands.md) still governs delivery and durable confirmation.
An arrival watch never writes domain state or bypasses JetStream.

## Cursor and recovery

An opaque cursor binds a stable ledger generation, mailbox, and ordered position.
Its public wrapper binds the NATS namespace, principal, and cursor kind in both auth modes.
OIDC HMAC rejects cross-namespace reuse even when databases share identical cursor keys.
Old unwrapped cursors and context mismatches fail validation; clients start a fresh watch and reconcile the Inbox.
Every watch, replay, and referenced entry requires mailbox ownership under the [auth contract](auth.md).
Expiry or revocation terminates the auth lease; reconnect requires current authorization.
Process restart preserves the generation and cursor meaning.
A fresh watch starts at the current committed watermark and does not replay historical mail.
A valid reconnect cursor resumes strictly after its position.
Replay preserves event IDs and timestamps; delivery remains at least once.
Clients deduplicate by event ID before any downstream action.

Malformed cursors and cursors for another mailbox fail explicitly.
A different generation, future position, or replay gap produces an explicit reset condition.
A reset establishes the current watermark; it never asserts that absent events imply no arrival.
The MCP adapter reports a reset through `Active.truncated: true`, not a synthetic `mail.received` event.
An internal page backlog only requests another page; it does not set that wire flag.
Clients reconcile the Inbox resource after a reset.
The arrival feed is not a durable agent work queue or an exactly-once action trigger.

## Resource bounds

Each active watch can tail PostgreSQL at approximately one-second intervals with bounded result pages.
The MCP adapter reads at most 100 arrivals per page and emits cursor heartbeats approximately every 15 seconds.
That server-side ledger tail does not authorize healthy browser-view polling.
Cancellation releases watch tasks and buffers.
Slow-consumer or replay failures remain explicit; clients never silently skip a gap.
No unbounded per-client queue or automatic history purge follows from this contract.

The ledger implementation resides in [arrivals.rs](../../../crates/inbox-server/src/arrivals.rs).
The [arrival migration](../../../migrations/0003_mail_arrivals.sql) defines retained generation, watermark, and ledger state.
Tests and source review must establish transaction integration and transport conformance separately.
