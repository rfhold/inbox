# Email-domain contract

This contract defines intended behavior for a fresh instance, not implementation or upgrade evidence.
The [protobuf schema](../../../proto/inbox/v1/inbox.proto) owns exact RPC shapes.
The [command contract](commands.md) owns all domain and mailbox writes and durable confirmation.

## Domain identity

A domain has a UUID, an immutable normalized lowercase ASCII DNS name, and creation and update timestamps.
Domain names are unique.
Each domain has independent `receive_enabled` and `send_enabled` flags.
The service supports domain creation, domain lists, and explicit policy updates through the web UI, API, and MCP.

The scope excludes domain rename, deletion, wildcard names, DNS verification, and external relay.
Domain registration establishes local identity and policy, not control of public DNS.
Every authenticated user can read the global domain directory.
Only the configured administrator group creates domains or changes policy.
The [auth contract](auth.md) defines these permissions and preserves separate local no-auth behavior.

## Mailbox identity

Each mailbox references a persisted domain UUID.
The normalized, case-insensitive local part is globally unique within that domain across users.
For example, `team@inbox.test` and `team@example.test` identify separate mailboxes.
The mailbox address derives from its local part and persisted domain name.
Environment changes never rewrite mailbox addresses.

Mailbox creation accepts an explicit domain choice.
For gRPC `CreateMailbox`, an empty `domain_id` selects the persisted bootstrap default.
MCP `create_mailbox` requires an explicit canonical domain resource URI; it has no omitted-domain default.
In OIDC mode, mailbox creation requires `receive_enabled OR send_enabled`; both false rejects creation.
The command consumer enforces this rule for browser, API, and MCP requests.
Local no-auth mode still permits mailbox creation when both flags are false.
Policy changes never hide mailboxes or messages and never delete data.
Later flag changes never revoke owner reads or restrict local draft access.
In OIDC mode, mailbox creation assigns the caller as owner.
Normal users create their own mailboxes; administrators gain no access to another user's mail.

## Bootstrap

On a fresh instance, `INBOX_DOMAIN` seeds one default domain through the NATS command path.
Its default value is `inbox.test`; the bootstrap domain initially enables both flags.
The durable bootstrap identity remains the default for empty gRPC mailbox domain choices.
Subsequent starts preserve that identity and its policy.
A later environment change neither adds another bootstrap domain nor re-enables flags.

This feature targets fresh-instance installation.
It does not promise an upgrade path or conversion of prior domain identities.
Instance replacement requires separate operator authority and never authorizes browser draft or unrelated stack deletion.

## Acceptance policy

The command consumer makes the authoritative policy decision for new mail acceptance.
Adapter checks provide early feedback but do not replace consumer validation.

| Flag | Effect |
| --- | --- |
| `receive_enabled` | Controls new local recipient acceptance through API submissions and SMTP intake. |
| `send_enabled` | Controls new API-originated mail from mailboxes in that domain, including MCP send and reply tools. |

SMTP checks receive policy at `RCPT TO` and again at authoritative acceptance.
Send policy does not restrict an external SMTP sender's envelope or `From` address.
No flag enables external delivery.
Any unknown, external, or receive-disabled recipient rejects the whole submission; no partial acceptance occurs.

Policy changes apply to new acceptance, not work that already passed acceptance.
An accepted identical idempotent retry returns its prior result despite later flag changes.
Durable delivery work completes under its accepted recipient decision; a later policy change does not retroactively reject it.
The [mail contract](mail.md) retains authority over payload conflicts, delivery outcomes, and replay safety.

## Client state

The browser exposes administrator-only domain management and an explicit domain selector for owner-assigned mailbox creation.
Cached domain reads follow the same stale-data rules as cached mailbox reads.
Domain and mailbox mutations require an online connection; no offline command queue exists.
Domain changes use a dedicated IDs-only invalidation event through the [change stream](changes.md).
Invalidation refreshes domain selectors, policy views, and affected mailbox state.
Cache reset, reconnect, and domain changes preserve the separate local draft store.

The [MCP contract](mcp.md) owns resource and tool shapes for domain management.
Domain names, addresses, UUIDs, and policy request content remain outside telemetry.
