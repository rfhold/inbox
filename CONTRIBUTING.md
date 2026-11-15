# Contribute

Read the [architecture](docs/architecture/overview.md) before a behavior change.
Use the [quality guide](docs/quality/testing.md) for commands and required evidence.

## Local development

The repository pins Rust through `rust-toolchain.toml` and pnpm through `package.json`.
Rust `1.96.0` satisfies the Waltr dependency's toolchain requirement.
Docker provides PostgreSQL, Garage, and NATS JetStream for integration checks.
The [quick start](README.md#quick-start) owns the local stack procedure.

From the repository root:

```sh
pnpm install --frozen-lockfile
pnpm generate
```

`pnpm generate` requires the Buf CLI. Generated TypeScript belongs under `web/src/gen/`.
The Rust build generates its protobuf bindings through `crates/inbox-server/build.rs`.

## Change expectations

- Keep the schema authoritative for RPC fields, enums, and wire semantics.
- Change the relevant contract document when intended behavior changes.
- Report source conflicts with the approved contract instead of silently changing the contract.
- Preserve raw MIME bytes and durable acceptance guarantees.
- Route mail and domain mutations through the [command contract](docs/architecture/spec/commands.md).
- Limit direct auth transactions to the [auth protocol persistence exception](docs/architecture/spec/auth.md#persistence-exception).
- Preserve per-user ownership across browser, API, MCP, command consumers, results, streams, and offline state.
- Preserve the [email-domain contract](docs/architecture/spec/domains.md), including one-time NATS bootstrap and domain-scoped mailbox identity.
- Test receive/send flags independently across API, MCP, SMTP, retries, browser cache, and restart.
- Treat domain installation as fresh-instance scope, not an upgrade compatibility promise.
- Preserve stream resume and cache invalidation semantics in the [change stream contract](docs/architecture/spec/changes.md).
- Preserve the [MCP contract](docs/architecture/spec/mcp.md) and the separate [durable arrival contract](docs/architecture/spec/arrivals.md).
- Use synthetic mail and local dependencies for tests.
- Keep secrets and mail content out of commits, diagnostics, and telemetry.
- Use Conventional Commits for commit messages and pull request titles.
- Do not add `Co-authored-by` trailers.

Repository-local review guidance lives in [.agents/skills/reviewing-changes/SKILL.md](.agents/skills/reviewing-changes/SKILL.md).
That guidance does not authorize deployments, external messages, or destructive actions.

## MCP development

The [MCP client guide](docs/architecture/mcp.md) owns project configuration and native OpenCode watches.
The adapter uses Waltr's generic MCP `2026-07-28` transport and Events extension, not an Inbox-specific protocol implementation.
The Events extension requires client opt-in.
Keep all mail and domain mutations on the current JetStream command path.
Use resource templates and tool schemas to verify exact adapter arguments and resource bounds.
The [MCP contract](docs/architecture/spec/mcp.md) documents exact tool fields, byte limits, continuation resources, and outcomes.
The server manifest pins the remote Waltr Git dependency; the [deployment guide](docs/operations/deployment.md#container-build-boundary) defines the root-only build context.
Do not treat a successful protocol connection as evidence of an agent wake.

## Hosted environment changes

The [deployment guide](docs/operations/deployment.md) defines fixed preview and signed stable-tag production promotion.
Keep application tests in local validation, not Tekton pipelines.
Infrastructure declarations do not prove deployment, broker ACLs, backup retention, or browser OAuth success.
No local command grants cloud apply, IdP mutation, mail migration, or reset authority.
