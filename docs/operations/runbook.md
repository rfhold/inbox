# Local stack runbook

## Safety

Explicit local no-auth mode has no caller isolation. Keep HTTP and SMTP on host loopback.
The [deployment guide](deployment.md) defines separate OIDC preview and production environments.
Do not publish PostgreSQL, Garage, NATS broker, or NATS monitor ports.
Compose provides neither high availability, replication, nor automatic backups.
Named volumes are not backups.

Require explicit operator approval before a restore, credential rotation, or data replacement.
Stop if the target stack, backup set, or credential provenance is uncertain.
Never use volume removal as a repair step.
Never delete S3 objects as orphan cleanup.

## Start and inspect

Use the [README quick start](../../README.md#quick-start) for setup and start commands.
From the repository root, inspect service state:

```sh
docker compose ps
```

Inspect logs only within a private boundary. Do not paste mail content or credentials into reports.
The intended health path is `GET /ready` on port 8080.
Health alone does not prove durable mail delivery or backup consistency.

For a normal stop that preserves volumes:

```sh
docker compose stop
```

## Browser session and offline access

The [client contract](../architecture/spec/client.md#offline-reload-limits) defines mode-specific offline access.
Authenticated cached mail and drafts remain available only within an already-verified open tab until session expiry.
An authenticated offline reload locks the workspace; reconnect for online session verification.
Draft retention does not grant offline authorization.
Persistent authenticated offline unlock is not supported.

Local no-auth offline reload requires that tab's prior online verification of local mode.
Account switches, logout, and expiry clear protected views and cancel prior-account work.
Failed or offline logout attempts identity-free barriers in two stores; either persisted pending record blocks unlock until server confirmation or 401.
If both writes and server logout fail, the current tab warns and stays locked; reload persistence lacks a guarantee.
Keep that tab open and reconnect to complete server logout before leaving.
Do not clear browser storage to bypass that barrier.
Do not delete drafts or mail caches as a session repair procedure.

## Runtime configuration

Compose owns service addresses; `.env` supplies local secrets and optional overrides.
Do not copy placeholder credentials from `.env.example`.

| Runtime variable | Purpose or Compose value |
| --- | --- |
| `INBOX_HTTP_ADDR` | App listener: `0.0.0.0:8080` inside its container. |
| `INBOX_SMTP_ADDR` | Private SMTP listener: `0.0.0.0:2525` inside its container. |
| `DATABASE_URL` | Private PostgreSQL connection string; includes a secret. |
| `INBOX_DOMAIN` | One-time fresh-instance bootstrap domain; defaults to `inbox.test`. |
| `INBOX_S3_ENDPOINT` | Private Garage S3 endpoint: `http://garage:3900`. |
| `INBOX_S3_REGION` | Garage region: `garage`. |
| `INBOX_S3_BUCKET` | Raw MIME bucket; defaults to `inbox`. |
| `INBOX_NATS_URL` | Private NATS endpoint: `nats://nats:4222`. |
| `AWS_ACCESS_KEY_ID` | Private S3 credential. |
| `AWS_SECRET_ACCESS_KEY` | Private S3 secret. |
| `INBOX_WEB_DIR` | Production asset directory: `/app/web`. |
| `INBOX_ENVIRONMENT` | Public environment label; Compose defaults to `compose`. |
| `INBOX_FARO_URL` | Public Faro collector URL. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Server OTLP collector URL. |

Compose uses `POSTGRES_PASSWORD` for its database secret.
`INBOX_HTTP_PORT` and `INBOX_SMTP_PORT` change host ports, not runtime listener names.
`OTEL_EXPORTER_OTLP_PROTOCOL` selects the exporter protocol; Compose defaults to `http/protobuf`.
The [client contract](../architecture/spec/client.md#telemetry) defines endpoint defaults and privacy requirements.
The [auth configuration reference](../architecture/spec/auth.md#auth-configuration) owns OIDC mode variables and required HTTPS settings.
The [auth route reference](../architecture/spec/auth.md#auth-routes) owns session, login, callback, and logout behavior.
After bootstrap, persisted domain identity and flags control mail behavior.
An environment change never rewrites addresses, resets flags, or adds another bootstrap domain.
Use domain management for later domain creation and policy changes.
The [email-domain contract](../architecture/spec/domains.md) defines fresh-instance scope; it does not promise an upgrade path.

## NATS availability and capacity

NATS JetStream persists state in `nats-data`. The broker and monitor have no host port publications.
This single-node broker has no high availability guarantee.
`docker/nats.conf` limits file storage to 4 GiB, memory storage to 64 MiB, and individual payloads to 1 MiB.
The [command contract](../architecture/spec/commands.md) defines chunked payload staging and the distinction between `PubAck` and mutation completion.

If NATS is unavailable or full, preserve PostgreSQL, Garage, and broker data.
Do not bypass NATS with direct writes or treat queued commands as complete.
The [auth protocol persistence exception](../architecture/spec/auth.md#persistence-exception) never permits direct mail or domain writes.
Pending staged payloads must not expire silently.
Before capacity changes, inspect actual stream limits, retention, consumer lag, outbox backlog, and staged-payload references privately.
Do not delete streams, staged payloads, S3 objects, or volumes as a capacity repair.

The [resource policy table](../architecture/spec/commands.md#large-payloads-and-capacity) defines stream capacities, subjects, retention, and discard policies.
Results and staged payloads have no age expiry and no automatic garbage collection.
Abandoned or resolved payloads and duplicate large confirmations consume capacity until a future operator-approved maintenance or capacity change.
Capacity exhaustion fails closed; it does not grant purge or deletion authority.
Broker-wide limits alone do not establish safe backlog capacity.

## Paired backup

Use this procedure before an upgrade or data migration.
It causes local downtime. The operator owns the backup destination and access policy.

1. Block new HTTP submissions and SMTP intake at the trusted boundary.
2. Quiesce every app consumer, command publisher, outbox publisher, API adapter, and SMTP intake process.
3. Stop all app processes before stores stop.
4. Stop PostgreSQL, Garage, and NATS cleanly through `docker compose stop`.
5. Capture `postgres-data`, `garage-metadata`, `garage-data`, and `nats-data` from that same stopped interval.
6. Save `.env`, `.local/garage-rpc-secret`, dependency configuration, and the exact application and image revisions privately.
7. Record one backup-set identity for all artifacts.
8. Encrypt the backup outside the stack's storage failure boundary.
9. Verify artifact completeness before service restart.

The PostgreSQL snapshot, both Garage volumes, NATS volume, and private configuration form one restore unit.
Never combine artifacts from unrelated backup sets.
The operator must select platform-specific volume snapshot or archive tools; this repository does not automate that step.

## Restore order

Require explicit operator approval for the exact restore target and backup set.
Use an isolated target first. Keep all app consumers and publishers stopped until every store passes verification.

1. Confirm the approved target and complete backup set.
2. Restore private configuration and credentials for that set.
3. Restore Garage metadata and object data together.
4. Start Garage privately with the backup's compatible image and configuration.
5. Verify bucket access and object availability with the restored credentials.
6. Restore PostgreSQL with the backup's compatible PostgreSQL version.
7. Start PostgreSQL privately.
8. Verify database integrity and referenced MIME object availability.
9. Restore `nats-data` from the same set with the backup's compatible NATS version and configuration.
10. Start NATS privately.
11. Verify JetStream state, staged payload availability, command/results streams, and consumer state against that backup set.
12. Start the same application revision after every store passes those checks.
13. Verify outbox replay, command redelivery safety, pending recipient work, and durable result confirmation with synthetic mail.
14. Verify mailbox entries, raw MIME fetch, submission status, and stream resume/reset behavior.
15. Reopen the trusted intake boundary only after verification.

If a reference lacks its object, stop the app and preserve the artifacts for diagnosis.
Do not repair a mismatch through deletion or an unrelated snapshot.
Schema upgrades require a separate migration and recovery decision after the baseline restore succeeds.
Restart within the same instance preserves accepted commands and pending delivery work.
This runbook grants no authority to replace an instance, clear browser drafts, or delete unrelated stack data.
