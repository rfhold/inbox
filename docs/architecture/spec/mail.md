# Mail contract

The [protobuf schema](../../../proto/inbox/v1/inbox.proto) defines the authoritative RPC contract.
These requirements define the intended durability and mail semantics beyond its message shapes.

## Acceptance and delivery

- The raw MIME limit is 25 MiB: 26,214,400 bytes.
- Transport limits allow protobuf overhead beyond that raw limit.
- A NATS consumer completes S3 PUT before the PostgreSQL acceptance transaction commits.
- The transaction persists the message, submission state, recipient work, command idempotency, and the transaction outbox.
- API success and SMTP `250` require that commit and a durable NATS result, as the [command contract](commands.md) defines.
- Delivery consumers use recoverable work state and bounded retries for each recipient job.
- Unique mailbox entries make worker replay safe.
- Submission status reflects durable recipient outcomes, not only request acceptance.
- SMTP acknowledgement loss can cause duplicate submissions; SMTP intake does not promise exactly-once acceptance.
- No path deletes S3 objects; orphan inspection produces an inventory only.

## Recipients and idempotency

The [email-domain contract](domains.md) defines persisted identity, bootstrap, and independent receive/send policy.
Mailbox local parts are case-insensitive and unique within their persisted domain.
OIDC mailbox creation requires at least one enabled domain flag; local no-auth mode permits both false.
Later policy changes never restrict owner reads or local drafts.
The service accepts local recipients only.
Any unknown, external, or receive-disabled recipient rejects the entire submission.
The server deduplicates recipients across To and Cc. It never relays to an external server.

The idempotency scope consists of the sender mailbox and the caller's key.
OIDC authorization binds that scope and its durable results to the authenticated owner.
Cross-user delivery uses recipient addresses, not access to another user's mailbox directory.
An identical payload returns the original durable submission.
A different payload under the same scope returns a conflict through `ALREADY_EXISTS`.
Concurrent retries preserve this contract.
Accepted identical retries retain their durable result after domain policy changes.
Delivery work for accepted recipients does not reapply later domain policy.
Command-consumer idempotency also protects every other server mutation against JetStream redelivery.

## Message identity and content

A message shares content across mailbox entries. Each entry owns its folder and read state.
A mailbox can hold both Inbox and Sent entries for one message.
Fetch does not change read state; `SetReadState` sets an explicit desired value.

Raw MIME retains attachments and HTML unchanged.
The API exposes decoded plain text separately; HTML-only mail has no plain-text body.
The UI never renders HTML from a message.

Reply defaults use the first Reply-To address, otherwise From.
The server derives RFC reply headers and References from the source entry.
The caller supplies To and Cc explicitly; opaque application IDs never substitute for RFC message identifiers.
The [auth contract](auth.md) requires owner checks for From, reply sources, raw MIME, entries, read state, and submissions.
Shared content never permits a recipient to access another owner's entry or submission.

## Pagination

Page size defaults to 50 and accepts explicit values from 1 through 100.
Lists sort by `created_at` descending, then `id` descending.
Message lists use entry timestamps and entry IDs.
Opaque page tokens bind to the original query, including the mailbox and folder where relevant.
In both modes, public tokens also bind the NATS namespace, principal, and cursor kind.
OIDC mode authenticates this context with HMAC, even across databases with identical cursor keys.
Old unwrapped tokens require fresh pagination under the [cursor context contract](auth.md#cursor-context).
An empty next token marks the end.
