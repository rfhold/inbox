# Client and privacy contract

## Navigation and notifications

Solid Router owns mailbox selection at `/mail/EMAIL/inbox`, `/mail/EMAIL/sent`, and `/mail/EMAIL/drafts`.
An optional final entry or draft UUID selects the detail pane.
Reloads and browser history preserve the URL's mailbox and folder; UUID mailbox aliases do not exist.
Mailbox addresses resolve only against the authenticated account's directory.
Unknown addresses never select another mailbox as a fallback.
The root URL selects the first available mailbox.
Domain management has its own `/domains` page.
The server and service worker serve the application shell for these navigation routes.
The shell contains no mail; private offline records remain account-scoped.

Delivery jobs continue to reconcile independently of their presentation.
New delivery states produce dismissible toasts and appear in a notification center, not a Delivery sidebar section.
Notification dismissal persists on the device for that account and never removes a job or draft.
A changed delivery state produces a new notification even after dismissal of its earlier state.
Failure toasts remain until dismissal; other delivery toasts expire after eight seconds.
The center retains at most the latest 100 job notifications.
Draft navigation waits for local persistence; failed persistence must not discard an unsaved draft.
Mailbox email paths and entry IDs must not enter telemetry or error context.

## Browser transport and display

Rust serves production assets and RPC endpoints from one origin.
Rolling deployments can briefly serve different bundled asset versions.
An HTML, asset, or service-worker request can reach a different release and fail;
this uncommon web disruption is accepted, without cross-release asset retention.
Reconnect or reload after rollout can recover; device-local drafts remain separate.
Active streams can disconnect during pod termination and use their existing resume/reset behavior.
The SPA uses generated TypeScript protobuf bindings with `createGrpcWebTransport`.
The [change stream contract](changes.md) defines live updates, cursor recovery, and persistent cache invalidation.
Plain-text views never interpret message content as HTML.
The MVP composes plain text only; attachment composition remains outside scope.
The interface follows the system light or dark theme and respects reduced-motion preferences.

The local runtime configuration uses these public keys:

| Key | Purpose |
| --- | --- |
| `faro_url` | Browser telemetry collector URL. |
| `environment` | Public environment label. |
| `version` | Public application version. |

Server secrets never enter runtime configuration or frontend build variables.
The [auth session endpoint](auth.md#auth-routes) reports the browser's mode, user UUID, administrator status, and session expiry.
Auth configuration names follow the [backend reference](auth.md#auth-configuration).
Before OIDC account admission, the browser reads `/config.json` with `no-store` and validates its nonempty `environment` string.
Public auth metadata never contains client secrets, tokens, cookie values, or private keys.

## Offline state

The service worker caches the asset shell, not RPC responses, raw MIME, or the change stream.
IndexedDB holds a bounded, versioned, server-specific mail cache.
Domain directory and policy reads use the same cache rules as mailbox reads.
Local drafts occupy separate storage from that cache.
Cache eviction and schema changes preserve drafts.

Offline views identify stale cached data. An absent cached view reports that a connection is necessary.
Server mutations, including send and read-state writes, remain disabled offline; the client has no offline command queue.
Domain creation, policy updates, and mailbox creation also require an online connection.
The global domain directory remains visible; only administrators see domain creation and policy controls.
OIDC mailbox creation requires at least one enabled flag; the server consumer enforces the rule for the selected domain.
Local no-auth mode also permits domains with both flags false.
Later flag changes never hide owned mail, restrict draft access, or delete drafts.
Within an admitted workspace, local draft edits remain available offline under the mode-specific access limits below.
Reconnect resumes the stream and performs full server resynchronization before the client trusts cached mail.
That process preserves local drafts.
Submission retries retain the same idempotency key for the same payload.
An unknown submission outcome does not justify a new key or an automatic duplicate send.

### Offline reload limits

An OIDC workspace permits cached reads and draft edits only in an already-verified open tab, until its session expiry.
Offline reload does not restore that authorization.
The browser locks the authenticated workspace until an online session check succeeds.
Stored mail, drafts, partition names, and account identifiers never prove identity.
The browser persists no authenticated session snapshot or bearer token as an offline unlock credential.
Persistent authenticated offline unlock remains a separate security and user-experience decision, not a supported capability.

Local no-auth mode retains offline reload within a tab that previously verified that mode online.
The origin-bound `sessionStorage` marker `inbox:local-mode` records that explicit local-mode choice.
It contains no authenticated identity and never unlocks OIDC storage.
Without that marker or available tab storage, offline reload also requires an online mode check.

## Account partition

Browser cache, drafts, cursors, and uncertain command state use the admitted account namespace:

| Mode | Namespace |
| --- | --- |
| OIDC | `inbox:${origin}:${encodedEnvironment}:oidc:${userUUID}` |
| Local no-auth | `inbox:${origin}:shared` |

`encodedEnvironment` uses `encodeURIComponent(environment)`; the OIDC user UUID comes from the verified session response.
Local no-auth preserves its prior shared namespace; it does not add an environment or authenticated user suffix.
The principal derives from verified identity, never an email address.
First login never imports no-auth drafts, mail, or command state.
Account switches never reuse another user's cached views or replay another user's uncertain commands.
Session verification precedes workspace mount, query initialization, and account storage access.
Each mounted workspace owns a fresh query client.
Generation checks guard asynchronous results; account changes abort request and stream signals and clear query memory.

Logout cancels streams and reconnect work, clears protected in-memory views, and makes prior-account storage inaccessible to the next account.
Session expiry applies the same access boundary.
Retained drafts stay in their original account partition; cache maintenance never silently reassigns them.
The service worker caches no authenticated responses, auth endpoints, raw MIME, or tokens.
BroadcastChannel messages and storage events coordinate account locks across tabs.
Focus, visibility, page restoration, and reconnect trigger session checks.
Online open tabs also check the session every 10 seconds.
Network dispatch verifies the account and rejects stale-generation responses.
Each account request sends `x-inbox-expected-user` with the account's canonical user UUID.
The server compares it with the independently authenticated cookie user before every read, write, or stream handler.
HTTP 401 or an unauthenticated RPC/stream error locks the client, aborts the account generation, and notifies other tabs.

Before OIDC unlock, the browser reserves the actual fixed-size logout records with the one-character value `0`.
Logout changes those records to `1` before the server request:

| Store | Identity-free record |
| --- | --- |
| localStorage | Key `inbox:logout-barrier:v1`, value `0` or `1`. |
| IndexedDB | Database `inbox:${origin}:logout-barrier`, object store `barrier`, key `logout`, value `0` or `1`. |

Logout attempts both writes independently; either a verified localStorage write or committed IndexedDB transaction preserves the pending barrier.
A pending record in either store blocks unlock.
The browser also honors the legacy `inbox:logout-pending` flag.
Storage-read or reservation errors fail closed; record reservation must succeed before OIDC unlock.
Only server-confirmed logout or HTTP 401 permits barrier clearance.
Clearance resets the fixed-size records to `0` and removes the legacy flag.
The records contain neither account identity nor credentials.

If both persistent writes and server logout fail, the current tab remains locked and displays the unsafe-logout warning.
In that case, persistence across reload lacks a guarantee if the stores later recover without a pending record.
Keep the tab open and reconnect to complete server logout before leaving.
This residual storage-failure limit grants no persistent authenticated offline unlock.

Implementation references: [session lifecycle](../../../web/src/session.ts), [workspace gate](../../../web/src/Auth.tsx), [account transport](../../../web/src/api.ts), [logout barriers](../../../web/src/logout-barrier.ts), and [storage](../../../web/src/storage.ts).

## Telemetry

The Rust service emits OpenTelemetry Protocol (OTLP) logs and traces with service name `inbox-server`.
The SPA emits allowlisted Faro telemetry with application name `inbox-web`.
Compose supplies the endpoints at runtime:

- OTLP: `https://telemetry.holdenitdown.net:4318`.
- Faro: `https://faro.holdenitdown.net/collect`.

The telemetry allowlist excludes mail bodies, subjects, addresses, drafts, secrets, application IDs, and query content.
URLs, errors, request fields, and browser auto-instrumentation do not bypass that rule.
Use synthetic fixtures and a local capture collector to verify the boundary.

## MCP clients

The [MCP contract](mcp.md) applies the same privacy boundary to resource reads, tools, and Events.
Email content remains untrusted data, not agent instructions.
Arrival Events contain only resource references and event metadata, never mail content or addresses.
Event and resource identifiers never enter telemetry.
The [MCP client guide](../mcp.md) distinguishes native agent wakes from resource refresh hints.
