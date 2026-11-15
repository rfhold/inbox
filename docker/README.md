# Local Compose

From the repository root:

Keep a local Waltr checkout at `../waltr`, or set `INBOX_WALTR_PATH` to its path.
Use Docker Compose 2.17 or newer with a BuildKit builder that supports additional build contexts.

```sh
python3 scripts/setup.py
docker compose up --build --wait
```

Open `http://localhost:8080`. SMTP accepts mail at `localhost:2525`.
The app has no authentication; Compose binds both ports only to IPv4 loopback.
PostgreSQL, Garage, and NATS expose no host ports.

The setup script creates `.env` and `.local/garage-rpc-secret` with private file permissions.
Repeat runs preserve credentials. Back up both files with PostgreSQL, Garage, and NATS volumes through the [paired backup procedure](../docs/operations/runbook.md#paired-backup).
Do not substitute `.env.example` placeholders for generated credentials.
Never display `docker compose config` output without `--quiet`; it contains secrets.

Optional `.env` values:

| Variable | Default |
| --- | --- |
| `INBOX_HTTP_PORT` | `8080` |
| `INBOX_SMTP_PORT` | `2525` |
| `INBOX_WALTR_PATH` | `../waltr` (build context only) |
| `INBOX_DOMAIN` | `inbox.test` |
| `INBOX_S3_BUCKET` | `inbox` |
| `INBOX_ENVIRONMENT` | `compose` |
| `INBOX_FARO_URL` | `https://faro.holdenitdown.net/collect` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `https://telemetry.holdenitdown.net:4318` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` |

Compose supplies the internal HTTP, SMTP, PostgreSQL, NATS, path-style S3, and static asset addresses.
Garage v2.5.0 creates one default bucket and key through its single-node startup flags.
Repeated starts reuse these resources.
The app waits for healthy PostgreSQL, Garage, and NATS services.
The app healthcheck calls `/ready`; `/health` provides the separate liveness endpoint.

The Dockerfile pins multi-platform image digests, Rust 1.96.0, Node 22.16.0, and pnpm 10.32.1.
It uses `Cargo.lock` and `pnpm-lock.yaml` with locked dependency installs.
The container's explicit Rust version overrides the repository's host toolchain file.
The build context excludes `.env`, `.local`, and all `.npmrc` files.
The final Debian image runs the app as UID 10001 with CA certificates and curl.

## Local Waltr dependency

Compose supplies the named `waltr` build context separately from the Inbox context.
The backend copies only Waltr's workspace `Cargo.toml`, `Cargo.lock`, `crates/mcp`, and `crates/mcp-macros` into `/waltr`.
Inbox's relative crate path `../../../waltr/crates/mcp` resolves there from `/src/crates/inbox-server`.
Cargo inherits Waltr's workspace dependency declarations directly; the Dockerfile does not rewrite them or build Waltr's host applications.
The optional macros crate stays available even when Inbox disables its default feature.
The build does not copy Waltr's repository metadata, host applications, credentials, or target artifacts.

For a standalone image build, supply the named context explicitly:

```sh
docker build --build-context waltr=../waltr -t inbox:local .
```

For an alternate checkout, use its path after `waltr=`.
Compose accepts `INBOX_WALTR_PATH=/path/to/waltr docker compose build app` instead.
Custom Compose build overlays must also supply `additional_contexts.waltr` when they replace the app build configuration.
Both build paths use Inbox's `Cargo.lock` through `cargo build --locked`.
Waltr's lockfile does not replace Inbox's dependency lockfile.
For repeatable results, preserve both checkout revisions and all local changes alongside Inbox's lockfile.
Local Waltr edits intentionally change the test surface; no published MCP package or vendored copy substitutes for them.

## Checks

```sh
docker compose config --quiet
sh tests/compose/dependencies.sh
python3 tests/compose/smtp_probe.py YOUR_INBOX@inbox.test
```

The dependency test starts only PostgreSQL and Garage.
It checks exact S3 bytes, restarts both services, then checks PostgreSQL and S3 durability.
It retains a database probe row and `_compose-test/roundtrip.bin` for subsequent checks.
It never removes volumes.
Run this test only when a brief dependency restart is acceptable.

The SMTP probe saves expected raw MIME under `.local/` and submits synthetic mail with dot-prefixed lines and an attachment.
Its optional `--raw-url` compares the download bytes after SMTP acceptance.
App-level API, failure, and telemetry checks require separate integration validation.

To stop this project's services without data loss:

```sh
docker compose stop
```

Do not use `docker compose down --volumes` unless you intend to delete all mailbox data.
This single-node Garage configuration provides persistence, not redundancy or backups.

## NATS JetStream

Compose supplies `INBOX_NATS_URL=nats://nats:4222` directly to the app.
The private, unauthenticated broker uses NATS 2.15.3 with a pinned Alpine image digest.
Client port 4222 and monitor port 8222 remain inside the Compose network.
This single-node broker provides no high availability.

`docker/nats.conf` sets server name `inbox-nats`, a 1 MiB payload limit, 4 GiB file storage, and 64 MiB memory storage.
The backend stages large payloads as 128 KiB Object Store chunks; the broker does not accept 25 MiB messages.
The named `nats-data` volume mounts at `/var/tmp`; NATS stores JetStream files under `/var/tmp/jetstream`.
The Alpine directory permissions permit UID/GID 65534 to create this directory without a root init service.
The broker uses a read-only root filesystem, dropped capabilities, and a temporary `/tmp` filesystem.
Its passive healthcheck uses BusyBox wget against `/healthz?js-enabled-only=true`.

After explicit stack startup, check NATS without a host NATS CLI:

```sh
sh tests/compose/nats.sh
```

This default check reads health and resource limits only; it does not start or restart any service.
The optional commands below retain a synthetic file-backed stream and message, then verify their bytes:

```sh
sh tests/compose/nats.sh --write
sh tests/compose/nats.sh --verify
```

The probe never purges streams, removes messages, or resets volumes.
To test restart persistence, the coordinator explicitly restarts NATS between these commands.
After a restart, run `docker compose up -d --wait nats` before verification.
Broker probes do not establish application commit ordering, acknowledgement behavior, Object Store integration, or WatchChanges conformance.
