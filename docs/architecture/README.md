# Architecture

Inbox uses durable private mail delivery with explicit local no-auth and per-user OIDC modes.

| Document | Covers |
| --- | --- |
| [System shape](overview.md) | Component ownership and data flow. |
| [Authentication and authorization](spec/auth.md) | Per-user ownership, browser/API access, hosted MCP OAuth, and auth persistence. |
| [Email-domain contract](spec/domains.md) | Persisted domains, independent receive/send policy, mailbox identity, bootstrap, and client management. |
| [Mail contract](spec/mail.md) | Acceptance, recipients, retries, replies, and pagination. |
| [Command contract](spec/commands.md) | JetStream mutation commands, durable results, outbox recovery, and capacity. |
| [Change stream contract](spec/changes.md) | gRPC-Web events, resume, reset, and client cache invalidation. |
| [Client and privacy contract](spec/client.md) | Plain text, offline state, transport, and telemetry. |
| [MCP clients](mcp.md) | Resource-centric access, project configuration, and native OpenCode event watches. |
| [MCP contract](spec/mcp.md) | Same-origin resources, mutation tools, Events, protocol, and trust boundaries. |
| [Durable arrival contract](spec/arrivals.md) | Inbox-only arrival ledger, stable event IDs, cursors, replay, and explicit gaps. |
