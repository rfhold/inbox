# Index

| Path | Info |
| --- | --- |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contributor entry point and local commands. |
| [docs/README.md](docs/README.md) | Canonical documentation index. |
| [proto/inbox/v1/inbox.proto](proto/inbox/v1/inbox.proto) | Authoritative RPC contract. |
| [crates/inbox-server/](crates/inbox-server/) | Rust API, SMTP intake, persistence, and delivery worker. |
| [migrations/](migrations/) | PostgreSQL schema, command idempotency, outbox, and durable work state. |
| [web/](web/) | SolidJS SPA, offline cache, local drafts, and browser telemetry. |
| [compose.yml](compose.yml) | Private single-node local stack. |
| [docker/](docker/) | Dependency configuration, including private NATS JetStream. |
| [scripts/](scripts/) | Local setup helpers. |
| [.agents/skills/reviewing-changes/SKILL.md](.agents/skills/reviewing-changes/SKILL.md) | Repository-specific review inputs. |

# Hints

- Read the relevant contract under [docs/architecture/](docs/architecture/README.md) before a behavior change.
- Use `planning-changes` and `making-changes` for behavior changes.
- Use `agentic-documentation` for documentation changes.
- Use [the quality guide](docs/quality/testing.md) for checks and evidence.
- Treat documentation as intended behavior, not proof that the implementation satisfies it.
- Do not infer deployment or destructive authority from a documented procedure.
