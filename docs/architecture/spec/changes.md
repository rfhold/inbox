# Change stream contract

`WatchChanges` returns an actual gRPC-Web server stream, not Server-Sent Events (SSE) or WebSocket messages.
The [schema](../../../proto/inbox/v1/inbox.proto) owns its field names and numeric enum values.
Local no-auth events cover the trusted workspace.
OIDC streams expose only authorized mailbox, entry, and submission IDs, plus the global domain directory's IDs.
The [auth contract](auth.md) governs principal-bound cursors, stream leases, expiry, and revocation.

This stream provides cache invalidation, not a durable mail arrival feed.
Message changes include read state and do not prove a new Inbox entry.
The [arrival contract](arrivals.md) defines the separate ledger for MCP `mail.received` Events.

## Wire shape

`WatchChangesRequest` has one string field: `resume_token`.
`ChangeEvent` has `resume_token`, `kind`, `mailbox_id`, `entry_id`, `submission_id`, and `domain_id`.
Events contain only cursor metadata, the kind, and applicable IDs; they never contain mail content or addresses.
Application IDs in this private stream do not enter telemetry.

| `ChangeKind` | Value | Meaning |
| --- | --- | --- |
| `CHANGE_KIND_UNSPECIFIED` | 0 | No usable change classification. |
| `CHANGE_KIND_RESET` | 1 | Current watermark; discard trust in cached server state and perform a full refresh. |
| `CHANGE_KIND_MAILBOX` | 2 | Mailbox selector data changed. |
| `CHANGE_KIND_MESSAGE` | 3 | Mailbox entry or message view changed, including read state. |
| `CHANGE_KIND_SUBMISSION` | 4 | Submission or recipient delivery state changed. |
| `CHANGE_KIND_HEARTBEAT` | 5 | Stream liveness and cursor progress, without mail content. |
| `CHANGE_KIND_DOMAIN` | 6 | Domain directory or receive/send policy changed; `domain_id` identifies the affected domain. |

## Cursor and recovery

A fresh stream emits `RESET` at the current watermark.
Resume tokens identify a cursor generation and sequence.
Public tokens bind the NATS namespace, principal, and cursor kind in both auth modes.
OIDC HMAC prevents cross-namespace reuse even with identical database cursor keys.
Malformed wrappers, old unwrapped tokens, and context mismatches fail validation before stream admission.
Clients restart those watches with an empty token and reconcile current state.
The generation includes the NATS namespace and JetStream change stream's creation timestamp; the sequence uses its stream sequence number.
A valid resume starts with a heartbeat at its unchanged cursor after the server establishes the subscription.
Within a valid context wrapper, an invalid, stale, or different-generation stream position produces `RESET` and requires a full refresh.
A sequence gap also produces `RESET`; clients never infer that absent events imply unchanged data.
Heartbeats allow clients to detect a stale connection.
The backend emits a heartbeat after each pull batch; idle fetch batches expire after 15 seconds.
This batch boundary does not promise an exact wall-clock heartbeat interval.
The browser aborts a stream after 45 seconds without an event and reconnects with backoff.
Reconnect uses backoff and the last accepted resume token.

The stream has bounded slow-consumer behavior: a client that falls behind resets and resynchronizes instead of silent change loss.
The backend channel holds 16 events; the ephemeral consumer allows 16 unacknowledged messages.
After a 15-second send timeout, the backend ends the stream with a terminal reset after the buffered events.
The ephemeral consumer has a 30-second inactivity threshold; normal cancellation also removes that consumer.
Cancellation releases server and browser subscriptions, buffers, and reconnect work.
The normal 15-second unary RPC timeout does not apply to this long-lived stream.
The browser's stream call explicitly sets `timeoutMs: 0`.
The stream retains no unbounded client queue.

These limits come from [the backend pipeline](../../../crates/inbox-server/src/pipeline.rs), [the stream adapter](../../../web/src/api.ts), and [the browser watcher](../../../web/src/changes.ts).

## Cache and UI effects

Events invalidate both persistent mail-cache records and affected Solid Query keys.

| Event scope | Invalidation and refresh |
| --- | --- |
| Domain change | Domain directory, policy views, domain selectors, and affected mailbox state. |
| Mailbox change | Mailbox selector and mailbox lists. |
| Message or delivery change | Relevant Inbox and Sent lists, entry details, and submission views. |
| Read-state change | The affected entry and its list/detail views. |
| Submission change | Submission state and affected delivery views. |
| Reset or reconnect | Full server resynchronization before cached data regains trusted status. |

The client resumes its stream after reconnect and reconciles server state before it trusts cached domains or mail.
Cached offline views remain visibly stale until that reconciliation completes.
Failed view and submission reads remain dirty and retry with exponential backoff, capped at 30 seconds.
Successful reads do not repeat on that retry; heartbeats do not poll healthy views.
Cache invalidation never deletes or overwrites local drafts.
Reconnect never replaces drafts with server data or sends them automatically.
Offline workspaces permit local draft edits, not server mutations or offline send, within the [client's session limits](client.md#offline-reload-limits).
The service worker never caches the stream.
