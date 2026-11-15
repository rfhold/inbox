# Documentation

These documents define the approved MVP and its operational boundaries.
The protobuf schema remains authoritative for the RPC wire contract.
The [MCP contract](architecture/spec/mcp.md) defines the separate resource-centric adapter and Events surface.

| Document | Covers |
| --- | --- |
| [Architecture](architecture/README.md) | Components, mail semantics, client behavior, and privacy. |
| [Operations](operations/README.md) | Local configuration, safety, backup, and restore. |
| [Quality](quality/README.md) | Checks, test scenarios, and evidence requirements. |
| [Authentication](architecture/spec/auth.md) | Explicit no-auth mode, OIDC ownership, browser/API access, and hosted MCP OAuth. |
| [Deployment](operations/deployment.md) | Intended preview/production resources, release policy, and recovery gates. |
