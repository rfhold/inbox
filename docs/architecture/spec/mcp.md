# MCP adapter contract

This contract defines intended behavior for the Inbox Model Context Protocol (MCP) adapter.
Source review and tests establish conformance separately.
The [mail contract](mail.md) and [command contract](commands.md) retain authority over mail and mutation semantics.

## Transport and trust

- The same Rust application serves `/mcp` on the browser application's origin.
- Waltr supplies generic MCP transport for protocol revision `2026-07-28` only.
- The adapter has no legacy `initialize` handshake or protocol fallback.
- The generic Events extension, `io.modelcontextprotocol/events`, defaults off and requires client opt-in.
- Inbox enables that extension for its arrival event surface.
- Explicit local no-auth mode shares the trusted local workspace.
- OIDC mode requires human-authorized hosted OAuth and per-user ownership under the [auth contract](auth.md).
- The global domain directory remains readable; only the configured administrator group manages domains.
- Normal users create only their own mailboxes; domain send policy also applies to owned From mailboxes.
- Compose exposes the application through loopback only; this is not a public MCP server.
- Email content remains untrusted data, never agent instructions or authorization for a tool call.

The [client privacy contract](client.md#telemetry) also governs MCP errors, request metadata, resource URIs, and event diagnostics.
Application IDs and mail content never enter telemetry.

## Resources

Resources own reads; tools own mutations.
Every mailbox, entry, body continuation, submission, next-page URI, result, and event reference requires current owner authorization.
Cross-user resource or cursor reuse never reveals mail, mailbox directories, command results, or application IDs.
Resource reads never mark an entry as read.
The [email-domain contract](domains.md) governs names, policy, and mailbox domain selection.

| Resource URI | Content |
| --- | --- |
| `waltr://domains` | Paginated domain directory; the template is `waltr://domains{?limit,cursor}`. |
| `waltr://domains/{domain_id}` | Domain identity and independent receive/send policy. |
| `waltr://mailboxes` | Paginated mailbox directory. |
| `waltr://mailboxes/{mailbox_id}` | Mailbox identity and links to Inbox and Sent collections. |
| `waltr://mailboxes/{mailbox_id}/inbox` | Paginated Inbox entry summaries. |
| `waltr://mailboxes/{mailbox_id}/sent` | Paginated Sent entry summaries. |
| `waltr://mailboxes/{mailbox_id}/entries/{entry_id}` | Plain-text message view, metadata, and entry read state. |
| `waltr://mailboxes/{mailbox_id}/entries/{entry_id}/body/{offset}` | Plain-text body continuation at a UTF-8 byte offset. |
| `waltr://submissions/{submission_id}` | Durable submission state and recipient delivery outcomes. |

URIs use the exact lowercase `waltr://` scheme and canonical lowercase, hyphenated UUIDs.
The parser rejects aliases, trailing slashes, fragments, percent escapes, and unknown paths.
URIs contain ASCII characters only and cannot exceed 4,096 bytes.
An entry resource belongs to its mailbox; a cross-mailbox entry reference fails validation.
Collections accept `limit` and opaque `cursor` query parameters.
The default limit is 50; explicit limits range from 1 through 100.
Limits use canonical decimal notation, without a sign or leading zero.
The parser rejects duplicate or unknown query parameters and queries on non-collection resources.
Cursor values contain at most 2,048 ASCII letters, digits, underscores, or hyphens; an empty cursor parameter fails validation.
Cursors bind to their original query, including the page size, mailbox, and folder where applicable.
Public cursor wrappers also bind NATS namespace, principal, and cursor kind in both modes under the [auth contract](auth.md#cursor-context).
OIDC HMAC rejects cross-namespace reuse even with identical database keys; old unwrapped cursors require fresh pagination or watches.
Domain directory responses contain `domains`; mailbox directory responses contain `mailboxes`.
Inbox and Sent responses contain `entries`.
The `next` field contains a canonical next-page URI, or `null` at the end.
Clients follow that URI and do not construct cursors.

### Domain and mailbox identities

Each domain projection contains `uri`, `id`, `name`, `receive_enabled`, `send_enabled`, `created_at`, and `updated_at`.
The domain directory returns these projections in `domains`; an individual domain resource returns one projection directly.
The flags are booleans; names use normalized lowercase ASCII DNS syntax.

Each mailbox projection contains `uri`, `id`, `local_part`, `name`, `address`, `domain`, `domain_id`, `domain_name`, `created_at`, `inbox`, and `sent`.
`domain` contains the canonical domain resource URI, not the DNS name.
`domain_id` contains the persisted UUID; `domain_name` contains the normalized DNS name.
The `address` derives from that persisted identity.

### Body chunks and output limits

Each resource returns JSON within a resource content item with MIME type `application/json`.
An initial entry response includes entry metadata, RFC reply metadata, and the first `text_body` chunk.
Both initial and continuation responses include `offset_bytes`, `total_bytes`, `next`, and `untrusted: true`.
A continuation response identifies its source through the `entry` URI.

Every body chunk contains at most 32,768 UTF-8 bytes.
Chunk boundaries never split a Unicode character.
The `next` URI uses `/entries/{entry_id}/body/{offset}` with the exact next byte offset.
Continuation offsets use positive canonical decimal notation and must land on a UTF-8 character boundary within the body.
The initial resource starts at offset zero; `/body/0` fails validation.
Clients follow `next` until it becomes `null`.

Both the JSON content and the full encoded resource result must fit within 262,144 bytes (256 KiB).
Oversized output fails explicitly; the adapter does not silently truncate metadata.
For an oversized collection, reduce `limit`.
Oversized individual metadata remains unsupported.
No raw MIME resource exists.
Raw MIME preservation remains a storage requirement, not an implicit MCP export surface.

## Mutation tools

### Domain and mailbox management tools

| Tool | Required arguments |
| --- | --- |
| `create_domain` | `name`, `receive_enabled`, `send_enabled`, `request_id`. |
| `set_domain_policy` | `domain`, `receive_enabled`, `send_enabled`, `request_id`. |
| `create_mailbox` | `domain`, `local_part`, `name`, `request_id`. |

Every listed field is required; unknown fields fail validation.
`domain` must identify a canonical `waltr://domains/{domain_id}` resource without query parameters.
MCP mailbox creation never defaults an omitted domain.
In OIDC mode, `create_mailbox` requires receive or send enabled; the command consumer rejects a domain with both flags false.
Local no-auth mode permits both-disabled mailbox creation.
Later domain policy changes never revoke owner resource reads or restrict browser drafts.
Only gRPC `CreateMailbox` permits an empty `domain_id` to select the persisted bootstrap default.
`name`, `local_part`, and `request_id` are strings; both policy flags are booleans.
Both domain flags remain independent; a false flag never means an omitted policy update.
`set_domain_policy` supplies both desired flag values, not a partial patch.
`request_id` supplies the management mutation's idempotency key.
All management tools use the same durable NATS command and confirmation path as mail tools.
The adapter calls the MailService RPC implementation; it never writes domain or mailbox state directly.

| Management input | Limit |
| --- | --- |
| Domain `name` | 1 through 253 ASCII bytes; DNS labels contain at most 63 bytes. |
| `local_part` | 1 through 64 ASCII bytes; the server normalizes case. |
| Mailbox `name` | At most 8,192 UTF-8 bytes; CR, LF, and NUL fail validation. |

The shared request-key, argument-object, URI, and resource-output bounds also apply to management operations.

### Mail mutation tools

| Tool | Required arguments |
| --- | --- |
| `send_message` | `from`, `to`, `cc`, `subject`, `body`, `idempotency_key`. |
| `reply_to_message` | `from`, `source`, `to`, `cc`, `subject`, `body`, `idempotency_key`. |
| `set_read_state` | `entry`, `is_read`, `request_id`. |

All listed fields are required; unknown fields fail validation.
`from` identifies a mailbox resource; `source` and `entry` identify initial entry resources, not body continuations.
`to` and `cc` are arrays of address strings; `cc: []` explicitly selects no Cc recipients.
`subject`, `body`, and mutation keys are strings; `is_read` is a boolean.
`request_id` supplies the read-state mutation's idempotency key.
No durable draft tool exists; browser drafts remain device-local.

| Input | Limit |
| --- | --- |
| `body` | 131,072 UTF-8 bytes (128 KiB). |
| `subject` | 8,192 UTF-8 bytes. |
| `to` plus `cc` | At most 100 addresses; `to` requires at least one address. |
| Each address | At most 320 UTF-8 bytes. |
| `idempotency_key` or `request_id` | 1 through 256 UTF-8 bytes, without control characters. |

The adapter also rejects serialized argument objects above 1,032,192 bytes.
These MCP input limits are smaller than the [25 MiB raw MIME limit](mail.md#acceptance-and-delivery).
The raw MIME limit does not permit a 25 MiB MCP body.

Every mutation uses the current JetStream command path, with no direct PostgreSQL fallback.
This statement covers MCP mail and domain tools; auth protocol state has the narrow [persistence exception](auth.md#persistence-exception).
A send or reply succeeds only after S3/PostgreSQL acceptance and a durable NATS result.
That success means acceptance, not terminal recipient delivery.
The submission resource supplies delivery progress.
An uncertain outcome requires the same idempotency key and payload on retry, not an automatic duplicate send.

Replies require explicit To and Cc recipient inputs.
The server derives RFC reply headers and References from the source entry.
Unknown, external, or receive-disabled recipients reject the whole submission; the adapter never relays external mail.
Send-disabled domains reject new send and reply acceptance.
Accepted identical retries preserve their prior result after policy changes.

### Outcomes

Tool results contain `structuredContent` and a `content` array.
The first content item contains the same JSON outcome as text; subsequent `resource_link` items identify follow-up resources.

| Outcome | Structured fields and links |
| --- | --- |
| `accepted` | `submission`, `idempotency_key`, and `delivery_final`; links to the submission and sender's Sent collection, plus the source for a reply. |
| `committed` for read state | `entry`, `is_read`, and `request_id`; a link to the entry resource. |
| `committed` for domain creation or policy | `domain` projection and `request_id`; a link to the domain resource. |
| `committed` for mailbox creation | `mailbox` projection and `request_id`; links to the mailbox and domain resources. |
| `unknown` | `code`, `retry_same_key: true`, `key`, and a privacy-safe message. |
| `not_accepted` | `code`, `retry_same_key: false`, `key`, and a privacy-safe message. |

Successful outcomes set `isError: false`; service error outcomes set `isError: true` and contain no resource links.
Invalid adapter arguments produce protocol errors instead of these service outcomes.
`delivery_final` reports whether submission status already left `pending`; acceptance alone never promises terminal delivery.
Unavailability, deadlines, internal failures, cancellation, and unknown service status produce an `unknown` outcome.
Retain the same key and exact payload for an uncertain retry.
Management `committed` outcomes confirm the management write, not mail acceptance or recipient delivery.

## Events

The adapter exposes `mail.received` through the generic Events extension.
The [arrival contract](arrivals.md) owns durability, cursor recovery, and event semantics.
The event input selects one Inbox resource:

```json
{
  "inbox": "waltr://mailboxes/00000000-0000-4000-8000-000000000001/inbox"
}
```

The Inbox URI must be canonical and contain no query parameters.
Unknown input fields, Sent URIs, mailbox identity URIs, and unknown event names fail validation.
The adapter rejects every supplied `maxAgeMs` value, including zero; omit that field.

Each event payload contains only these resource references:

```json
{
  "inbox": "waltr://mailboxes/00000000-0000-4000-8000-000000000001/inbox",
  "message": "waltr://mailboxes/00000000-0000-4000-8000-000000000001/entries/00000000-0000-4000-8000-000000000002"
}
```

The event envelope carries the stable entry UUID as `eventId`, a timestamp, and cursor metadata.
Events never contain a subject, body, or address.
Clients fetch the entry resource after an event.
Resource update notifications serve as refresh hints, not agent wakeups.
The adapter has no custom Inbox watch-binding tool.

### Lifecycle and cancellation

The server emits `Active` with its cursor before any event.
Fresh watches and valid replay cursors use `truncated: false`.
A ledger reset emits `Active` with `truncated: true` and the current watermark.
Reconcile the Inbox resource after that reset; never interpret it as a mail arrival.
An internal page backlog does not imply a gap and does not set the wire-level `Active.truncated` flag.

The server tails pages of at most 100 arrivals at approximately one-second idle intervals.
It drains a backlog without that idle delay and checks heartbeat progress after each page.
Heartbeats carry the current cursor at approximately 15-second intervals; blocked work can delay them.
Ledger reads have a 10-second timeout; event emission has a 15-second timeout.
Store failure after activation emits `Terminated` with code `-32001`; clients reconnect with their last received cursor.
An event consumer that exceeds the emission timeout receives an explicit transport failure rather than silent event loss.
Request cancellation owns resource reads, mutation waits, and event streams through the generic transport.
Adapter shutdown also cancels the event stream, its ledger tail, and its heartbeat work.
Cancellation never proves that a mutation failed before commit.

See the [MCP client guide](../mcp.md) for OpenCode configuration and native event watches.

## Source references

- [Adapter, resource templates, tool schemas, and event handler](../../../crates/inbox-server/src/mcp.rs).
- [HTTP adapter test definitions](../../../tests/backend/mcp.rs).

Test definitions establish intended coverage, not a claim that a test ran or passed.
