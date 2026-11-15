# Authentication and per-user authorization

This contract defines approved intended behavior, not implementation or deployment evidence.
The protobuf schema owns RPC fields.
The route and configuration reference below follows [auth.rs](../../../crates/inbox-server/src/auth.rs) and [oauth.rs](../../../crates/inbox-server/src/oauth.rs).
Source inspection does not establish runtime or provider verification.
The [deployment guide](../../operations/deployment.md) owns environment isolation and release policy.

## Modes and identity

Inbox supports explicit local no-auth mode and OIDC mode.
Partial or invalid OIDC configuration fails startup; OIDC never falls back to no-auth.
No-auth mode retains one trusted local workspace and loopback-only access.
Its synthetic principal remains separate from every authenticated user.
First login never assigns local mailboxes, mail, drafts, or command history to that user.
There is no automatic local-to-cloud mail migration.

OIDC identity uses the exact verified issuer and nonempty subject, never an email address or display name.
Browser sessions, API access tokens, and hosted MCP grants resolve to the same Inbox user.
Identity admission does not grant shared mailbox access.
Each mailbox has one user owner; mailbox creation assigns the authenticated caller as owner.
OIDC mailbox creation requires a domain with receive or send enabled; the command consumer rejects both false.
Local no-auth mode permits both-disabled mailbox creation.
Later domain policy changes never revoke owner reads or restrict local drafts.
The server never accepts a caller-supplied owner as authority.

## Authorization

The service enforces ownership before every protected read or mutation.
Command consumers enforce the authenticated actor and target scope; adapters cannot substitute arbitrary actor identities.

| Surface | Required boundary |
| --- | --- |
| Mailbox lists and identity resources | Return only the caller's mailboxes. |
| Inbox, Sent, entry, body continuation, raw MIME, and read state | Require ownership of the referenced mailbox and entry. |
| Send and reply | Require ownership of From and any source entry; domain send policy also applies. |
| Submission reads and results | Require the caller's sender ownership; shared message content never grants access. |
| Request keys, command journals, and durable results | Bind actor identity and operation scope; a different user cannot recover another user's result. |
| Pagination and resume cursors | Bind NATS namespace, principal, cursor kind, and original query; cross-user and cross-namespace reuse fail in both modes. |
| Change streams and arrival Events | Emit only authorized mailbox, entry, and submission IDs; authorize subscriptions and cursor replay. |

Mailbox addresses remain globally unique within each domain across all users.
Different users exchange mail through ordinary recipient addresses.
Recipient resolution does not expose another user's mailbox directory.
Cross-user delivery creates separate entries under their respective owners.
Shared raw content storage never bypasses entry authorization.

Domains form a global readable directory.
Only members of the configured administrator group create domains or change domain policy.
Normal users create their own mailboxes in those domains, subject to the address uniqueness rule.
Administrator status grants no access to another user's mail.
Domain flags retain their [acceptance semantics](domains.md#acceptance-policy).

## Cursor context

Every public pagination, change-stream, and arrival cursor wraps its position with the NATS namespace, principal, and cursor kind.
This rule also applies to disabled mode's fixed local principal.
OIDC mode authenticates that context with HMAC.
A production cursor fails in preview even when both databases contain identical cursor keys.
Disabled mode checks the context but provides no authenticated security boundary.
Query, mailbox, folder, and ledger-generation checks still apply within that context.
Old unwrapped cursors fail validation; clients restart pagination or watches with an empty cursor and reconcile current state.
An ordinary process restart preserves valid current-format cursors within the same namespace and identity.

## Browser and API access

Browser login uses Authorization Code with PKCE S256, independent random state, nonce, and browser correlation.
Durable one-shot attempts enforce expiry, atomic callback consumption, and replay rejection.
Callbacks validate signature, algorithm, exact issuer, audience, time claims, subject, nonce, and the exact redirect URI.
Return destinations permit only safe local paths.
Back-channel requests use bounded, redirect-free HTTPS.

The server stores opaque browser-session digests, not plaintext session tokens.
Cookies use HttpOnly, SameSite, and Secure attributes in hosted environments.
Browser JavaScript receives neither provider tokens nor client secrets.
Every cookie-authenticated gRPC-Web RPC requires CSRF protection, even a read or stream RPC.
The check requires exactly one Origin equal to `INBOX_PUBLIC_URL` and `Sec-Fetch-Site: same-origin`.
Missing, duplicate, malformed, or foreign Origin values fail closed.
Every OIDC cookie-authenticated gRPC-Web RPC also requires exactly one `x-inbox-expected-user` header containing a canonical lowercase, hyphenated user UUID.
The server independently authenticates the cookie, then compares that user with the header before handler dispatch.
The header asserts the browser's expected account; it never supplies authentication or grants authority.
Missing, duplicate, malformed, or mismatched values return 401.
A cookie switch between session preflight and RPC dispatch therefore cannot cross the browser's account boundary.
Native bearer and hosted MCP authentication do not use this OIDC cookie-RPC header requirement.
Disabled callers can omit the header; any supplied value must match the fixed local user canonically.
Logout requires an authenticated same-origin request and durably revokes the session before cookie removal.
The browser locks locally before logout and attempts independent identity-free barriers in localStorage and IndexedDB.
Either persisted pending marker blocks unlock; storage errors fail closed.
If both barrier writes and server logout fail, the current tab stays locked and warns that reload persistence lacks a guarantee.
The [client contract](client.md#account-partition) defines that barrier, session checks, generation cancellation, and exact storage namespaces.
OIDC offline reload requires online session verification; a prior cache never establishes identity.
An already-verified open tab retains offline reads and drafts only until its current session expiry.

Native gRPC accepts bearer credentials only, never browser cookies.
API bearer validation independently checks allowed algorithms, token type, issuer, API audience, expiry, subject, and required scopes.
Native admission follows the [pinned Authentik access-token profile](#native-access-token-profile).
ID tokens do not authorize API requests.
API and MCP auth failures return protocol errors, not HTML login redirects.
Active streams hold bounded auth leases; expiry or revocation terminates access without a process restart.
Reconnect reauthenticates and reauthorizes the current principal.
Implementation configuration must state lease and revocation-check bounds.

## Auth routes

`GET /auth/session` returns `Cache-Control: no-store` for successful and unauthenticated responses.

| Mode and state | Status | JSON response |
| --- | --- | --- |
| Local no-auth | 200 | `{"mode":"disabled","authenticated":true,"user_id":"00000000-0000-4000-8000-000000000001","administrator":true,"expires_at":null}` |
| OIDC authenticated | 200 | `mode: "oidc"`, `authenticated: true`, user UUID `user_id`, boolean `administrator`, RFC3339 `expires_at`. |
| OIDC unauthenticated | 401 | `{"mode":"oidc","authenticated":false}` |

| Route | Contract |
| --- | --- |
| `GET /auth/login?return_to=<relative-path>` | Starts browser login; the optional return path defaults to `/`. |
| `GET /auth/callback` | Consumes the browser-bound OIDC attempt and establishes the session. |
| `POST /auth/logout` | Requires the authenticated session, exact Origin, and `Sec-Fetch-Site: same-origin` in OIDC mode; success returns 204. |

Return paths start with one `/`; absolute URLs, protocol-relative paths, backslashes, and control characters fail.
Browser cookies use `__Host-inbox-session`; login correlation uses `__Host-inbox-login`.
Hosted MCP uses its separate local OAuth issuer at `<public-origin>/oauth` for resource `<public-origin>/mcp`.
Its upstream resource-owner callback is `<public-origin>/oauth/oidc/callback`, not the browser session callback.
The identity provider must register both callback URLs.

## Auth configuration

`auth::Config` reads these environment variables.
`INBOX_AUTH_MODE` accepts exactly `disabled` or `oidc`; omission selects `disabled`.
Disabled mode rejects `INBOX_OIDC_*`, `INBOX_OAUTH_*`, and other `INBOX_AUTH_*` settings.
OIDC mode requires every required row below; invalid or partial values fail startup.

| Variable | Requirement and purpose |
| --- | --- |
| `INBOX_PUBLIC_URL` | Required HTTPS origin without a trailing slash, userinfo, query, or fragment. |
| `INBOX_OIDC_ISSUER` | Required exact HTTPS identity-provider issuer. |
| `INBOX_OIDC_CLIENT_ID` | Required confidential browser and hosted resource-owner client identifier. |
| `INBOX_OIDC_CLIENT_SECRET` | Required confidential-client secret; provider discovery must support `client_secret_basic`. |
| `INBOX_OIDC_API_AUDIENCE` | Required native API access-token audience. |
| `INBOX_OIDC_API_CLIENT_ID` | Required native API client identifier. |
| `INBOX_AUTH_ADMIN_GROUP` | Required exact administrator-group value from verified `groups` claims. |
| `INBOX_OIDC_API_SCOPES` | Optional whitespace-separated API scopes; defaults to `inbox:api`; an empty set fails startup. |
| `INBOX_OAUTH_WRAPPING_KEYS_FILE` | Required private versioned OAuth wrapping-key JSON file. |

Hosted MCP requires scope `inbox:mcp`, distinct from the native API scope.
No loopback HTTP OIDC override exists in this configuration.
Local HTTP development uses explicit no-auth mode.

## Native access-token profile

Native API admission targets Authentik `2025.10.2`, not a provider-independent JWT token profile.
Its `IDToken.to_access_token()` path adds `uid`, `azp`, and `scope` to access tokens.
Provider property mappings must reserve all three claims and never inject them into ID tokens.
The [deployment guide](../../operations/deployment.md#identity-provider-constraint) owns the operator verification requirement.

The native verifier requires:

- Exact JOSE `typ: JWT`; absent, differently cased, or different types fail.
- Exact `azp` equal to `INBOX_OIDC_API_CLIENT_ID`.
- A nonempty string `uid` of at most 256 bytes without control characters.
- Every configured API scope within the whitespace-separated `scope` string.
- The allowed asymmetric signature, exact issuer, exact API audience, valid time claims, and nonempty subject.

If a token contains `client_id`, it must equal the configured API client ID.
`at+jwt` is not an alternative native token type.
This profile uses local JWT verification, not token introspection or another environment setting.
Audience overlap alone never establishes access-token identity.
`uid` serves token admission, not account identity; verified issuer plus subject still identifies the Inbox user.
Browser ID tokens retain their separate nonce/client validation and never authorize native API requests.
Hosted MCP retains its separate locally issued OAuth tokens and exact MCP resource audience.

## Human-authorized hosted MCP

Inbox follows the sibling hosted OAuth pattern: the identity provider authenticates the human; Inbox hosts the MCP authorization server.
Protected-resource metadata identifies the exact MCP resource and the local authorization issuer.
Direct identity-provider tokens never authorize hosted `/mcp`.
The generic Waltr MCP implementation owns protocol mechanics; Inbox maps verified identity to its per-user authorization domain.

Public clients complete Authorization Code with PKCE S256 and explicit consent.
Authorization binds client, exact redirect, resource, scope, human owner, and code challenge.
Client registration and metadata discovery retain the generic implementation's hardened destination and redirect checks.
Tokens bind the local issuer, exact resource audience, client, subject, lifetime, and required scopes.
Each request checks durable grant, token, refresh-family, and client authorization state.
Hosted MCP sets clock skew to zero: access tokens fail admission at `exp`, without post-expiry grace.
Active MCP response streams and Events use the same expiration deadline.
Refresh rotates tokens atomically; reuse and revocation fail closed.
Signing keys persist encrypted under a separately mounted versioned wrapping key.
Production composition sets the access-token lifetime to 600 seconds and verification-key overlap to 660 seconds.
Verification-key overlap preserves key availability; it never extends an expired token's validity.
MCP transport identity never expands mailbox authority.
Local no-auth MCP remains separate and uses no hosted OAuth challenge.

## Persistence exception

Only auth protocol state can use direct PostgreSQL transactions outside JetStream:

- Login attempts and browser sessions.
- OAuth clients, consent grants, codes, tokens, refresh families, and protected signing keys.

This exception does not cover mailbox or domain creation, ownership changes, mail acceptance, read state, delivery, or user mail migration.
The [command contract](commands.md) still requires NATS commands, committed effects, and durable results for those mutations.
Auth-store failure fails protected access closed; it never authorizes a mail-path SQL fallback.

## Client privacy and verification

The [client contract](client.md#account-partition) owns account-specific caches, drafts, command state, and logout behavior.
The [quality guide](../../quality/testing.md#auth-and-environment-security) defines two-user checks and manual OAuth evidence.
Sibling references inform the design, not proof of Inbox conformance:

- [Kuri browser and API auth](https://git.holdenitdown.net/rfhold/kuri/src/branch/main/docs/architecture/access-authentication.md).
- [homelab-mcp hosted OAuth](https://git.holdenitdown.net/rfhold/homelab-mcp/src/branch/main/docs/architecture/access-authentication.md).
