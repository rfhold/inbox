# Inbox

Inbox is a private email workspace with a Rust server and a SolidJS progressive web app (PWA).
PostgreSQL holds metadata, durable work state, and the transaction outbox. S3 storage holds immutable raw MIME.
NATS JetStream carries mail and domain mutation commands and their durable results.

The MVP supports internal delivery and private SMTP intake.
Explicit local no-auth mode preserves a trusted shared workspace.
OIDC mode gives each user separate mailboxes through browser, API, and human-authorized MCP access.
Only the configured administrator group manages global domains.
These documents define intended behavior, not proof of implementation or deployment.

## Quick start

Install Docker with Compose and Python 3.
From the repository root:

```sh
python3 scripts/setup.py
docker compose up --build
```

The setup script creates local credentials without rotation. Keep `.env` and `.local/` private.
The intended app URL is <http://127.0.0.1:8080>; private SMTP uses `127.0.0.1:2525`.
PostgreSQL, Garage, and NATS have no host ports.

The quick start requires end-to-end verification against the complete implementation.
The current setup entry point is `scripts/setup.py`; the proposed `scripts/setup.sh` entry point does not exist.

## Boundaries

- Persisted email domains have independent receive/send flags; `INBOX_DOMAIN` seeds the default once, initially `inbox.test`.
- Mailbox addresses remain unique per domain across users; only owners access their mailboxes in OIDC mode.
- The server never relays mail to external recipients.
- The UI displays plain text, not HTML.
- Offline access covers admitted cached views and local drafts, not server mutations; authenticated offline reload requires online session verification.
- Live updates use an IDs-only gRPC-Web stream; reconnect requires server resynchronization.
- A same-origin MCP adapter exposes mail resources, explicit mutation tools, and durable Inbox arrival Events.
- Local MCP shares the trusted no-auth workspace; hosted MCP requires human-authorized OAuth and owner checks.
- Imports, IMAP, POP, JMAP, search, and attachment composition remain outside scope.
- Fixed preview follows main; signed stable tags promote immutable images to a separate production environment.
- A public SMTP bridge remains future work.

## Documentation

| Document | Covers |
| --- | --- |
| [Documentation index](docs/README.md) | Architecture, contracts, operations, and quality. |
| [Contribute](CONTRIBUTING.md) | Local commands and change expectations. |
| [Operations](docs/operations/runbook.md) | Configuration, safe shutdown, backup, and restore. |
| [MCP clients](docs/architecture/mcp.md) | Resource-centric mail access and native OpenCode event watches. |
| [Authentication](docs/architecture/spec/auth.md) | Per-user access, global domain administration, and the narrow auth persistence exception. |
| [Deployment](docs/operations/deployment.md) | Pulumi, fixed preview, signed-tag promotion, and private shared infrastructure. |

The [protobuf schema](proto/inbox/v1/inbox.proto) owns the RPC wire contract.
The [MCP contract](docs/architecture/spec/mcp.md) defines the adapter's intended resource and Events behavior.
The documentation describes the approved MVP contract, not a claim of implementation completeness.
The [command contract](docs/architecture/spec/commands.md) distinguishes queued commands from completed mutations.
