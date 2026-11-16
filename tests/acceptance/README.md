# Black-box acceptance tests

The standard and stream tests use public gRPC-Web and SMTP interfaces without importing server code or inspecting database tables.
The separately enabled legacy fault suite uses scoped SQL fixtures as described below.
The protobuf contract in `proto/inbox/v1/inbox.proto` defines the RPC messages.

## Setup

Run these commands from the repository root:

```sh
python3 -m venv tests/acceptance/.venv
tests/acceptance/.venv/bin/python -m pip install -r tests/acceptance/requirements.txt
```

The only direct dependency is `grpcio-tools`; it supplies compatible protobuf runtime and compiler dependencies.
The client generates standard Python protobuf bindings in a temporary directory at import time.
The host `protoc` executable is not required. No generated bindings enter the repository.
Python 3.11 or later is required, including built-in exception groups for cleanup diagnostics.

## Checks without the app

```sh
tests/acceptance/.venv/bin/python -m compileall -q tests/acceptance -x '/\.venv/'
tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p 'test*protocol.py' -v
```

Protocol checks cover binary and text responses, error trailers, header-only errors, malformed frames, the HTTP request format, and the MIME fixture.
They also check metadata identifier normalization and SQL UUID validation.
Stream checks cover every frame split, partial headers and payloads, multiple frames, terminal trailers,
non-OK statuses, truncated EOF, frame size bounds, protobuf field numbers, incremental HTTP delivery, bounded waits, and cancellation.
The HTTP protocol check uses a local fixture server. It does not replace acceptance tests against the actual app.

## Tests against the actual app

Start the app separately before this command:

```sh
tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_acceptance.py -v
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `INBOX_TEST_HTTP_URL` | `http://localhost:8080` | gRPC-Web base URL; optional path prefix. |
| `INBOX_TEST_SMTP_HOST` | `localhost` | SMTP hostname. |
| `INBOX_TEST_SMTP_PORT` | `2525` | SMTP port. |
| `INBOX_TEST_TIMEOUT` | `30` | Delivery poll deadline, in seconds. |
| `INBOX_TEST_LARGE` | unset | `1` enables the additional exact-limit and oversize tests. |

Example with the raw-size tests:

```sh
INBOX_TEST_LARGE=1 tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_acceptance.py -v
```

Every scenario creates unique synthetic mailboxes under `inbox.test`.
The suite leaves those mailboxes and messages intact. It never removes objects or volumes.
No test sends telemetry directly or contacts a real collector. Normal app telemetry depends on the app configuration.
Use a local telemetry collector if the app emits telemetry during these tests.

## Coverage

- Two-mailbox internal submission and bounded `GetSubmission` polls until terminal success.
- Exactly one recipient Inbox entry and one sender Sent entry for each accepted message.
- Recipient deduplication across To and Cc.
- Identical retries preserve submission and message IDs; changed payloads return `ALREADY_EXISTS`.
- Independent sender scopes for the same idempotency key.
- Four concurrent identical requests produce one submission and one delivery.
- A discarded response body followed by a retry produces one durable message.
- Unknown or external recipients in mixed To/Cc requests reject the whole request with `INVALID_ARGUMENT`.
- A later successful submission provides a worker-progress barrier before the no-delivery assertion.
- Repeated explicit read-state updates; `GetMessage` does not mark messages read.
- Cross-mailbox `GetMessage`, `GetRawMessage`, `SetReadState`, and reply source requests reject invalid scope.
- Cursor traversal excludes new mail, contains no duplicates, retains older entries, and follows descending timestamp/ID order.
- Cross-mailbox and cross-folder cursor reuse fails.
- Composed MIME includes sender, To, Cc, Subject, Date, and Message-ID headers.
- API identifier comparisons allow absent angle wrappers; raw reply headers still require correct RFC angle wrappers.
- SMTP multipart MIME preserves UTF-8 plaintext, HTML, attachment bytes, and raw CRLF bytes after dot transparency.
- Plaintext extraction matches the fixture exactly and does not substitute HTML or attachment text.
- The first Reply-To address wins; absent Reply-To falls back to From.
- Replies derive In-Reply-To and References from the source message; each reply receives a distinct Message-ID.
- SMTP refuses unknown local recipients and external relay attempts at RCPT TO.
- CRLF/LF header injection, absent recipients, absent idempotency keys, and incomplete reply fields fail validation.
- Optional large tests accept and download exactly 25 MiB of SMTP raw MIME, with protobuf overhead above the raw limit.
- Optional large requests exceed the 25 MiB raw limit and fail before acceptance.

## WatchChanges and command request IDs

Run after rebuilding the app with the NATS implementation and starting the complete stack:

```sh
tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_changes.py -v
```

These four scenarios cover:

- A fresh binary gRPC-Web stream starts with RESET and a nonempty watermark.
- Mailbox creation produces MAILBOX; submission and delivery produce SUBMISSION and MESSAGE events.
- IDs resolve through read RPCs when observed, and event protobufs contain no tested subjects, bodies, or addresses.
- Concurrent streams receive changes; closing one does not interrupt another.
- Explicit read-state changes produce MESSAGE events and updated read results.
- CreateMailbox and SetReadState request IDs replay the original result; changed payloads conflict.
- A saved cursor recovers offline changes, either through replay or an explicit RESET followed by full refresh.
- Invalid cursors cause RESET. Read RPCs do not emit mutations for the test's own resources.
- Heartbeats have no entity IDs and do not cause RESET or change read state.

`INBOX_TEST_HEARTBEAT_TIMEOUT` defaults to 35 seconds; set it above the configured heartbeat interval.
Each stream uses a bounded frame buffer, a bounded event queue, socket deadlines, per-event wait deadlines, and explicit close.
No test uses SSE, WebSockets, or whole-body polling for the stream.
An unexpected transport EOF without gRPC status is a protocol failure, not a successful empty stream.

## Exclusive NATS fault tests

The coordinator runs these separately from every other service-dependent test:

```sh
INBOX_TEST_NATS_FAULTS=1 tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_nats_faults.py -v
```

Without `INBOX_TEST_NATS_FAULTS=1`, all scenarios skip before setup or service commands.
Two scenarios run. The process-crash case additionally requires `INBOX_TEST_NATS_CRASH=1`.

The default scenarios stop NATS and verify that List/Get RPCs still work while CreateMailbox,
SetReadState, SubmitMessage, and SMTP cannot return application success.
SMTP may return an error at RCPT or DATA. Transport failures and gRPC `CANCELLED` (1),
`DEADLINE_EXCEEDED` (4), and `UNAVAILABLE` (14) leave command fate unknown.
They prove neither admission nor permanent rejection: a queued command can commit after confirmation is lost.
Contract application errors such as `INVALID_ARGUMENT` (3), `NOT_FOUND` (5), and `ALREADY_EXISTS` (6)
are distinguished in diagnostics. Status alone does not establish the origin of a worker confirmation.
The NATS outage scenario therefore makes no no-eventual-commit assertion from an error status.
Recovery retries API commands with their original request IDs or scoped idempotency keys and verifies stable IDs and one Inbox/Sent pair.
SMTP `451` can follow a worker commit plus lost confirmation. The SMTP contract is at-least-once on retry,
not exactly-once: these tests neither retry the outage send nor require its absence after recovery.
Successful DATA `250` still requires durable application confirmation, not just broker PubAck.
Permanent unknown-recipient and no-relay validation assertions remain in the standard suite.
App shutdown plus a same-volume NATS restart preserves a confirmed submission and its delivery,
allows saved-cursor replay or RESET, and permits a new RESET stream.
The original submission is polled before any retry, so recovery cannot rely on resubmission.

A short client timeout or gRPC availability error cannot prove that JetStream admitted a command.
The separately enabled crash fixture below instead observes the consumer inside PostgreSQL before proceeding.

These tests use the private Compose `nats` service; no host broker port or NATS client dependency is required.
They restore PostgreSQL, Garage, NATS, and app in `finally` and unittest cleanup, attempting every service even if one start fails.
They never purge streams, delete data, invoke `down`, or remove volumes.
Only the additional crash fixture opens a session advisory lock and reads its own synthetic SQL state;
it does not change tables, install triggers, or remove records.
`INBOX_TEST_FAULT_TIMEOUT` also controls NATS fault requests and recovery polls.

### Process crash after commit, before confirmation

Run only with exclusive access after all browser and other app tests have finished:

```sh
INBOX_TEST_NATS_FAULTS=1 INBOX_TEST_NATS_CRASH=1 tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_nats_faults.py -k process_crash -v
```

`INBOX_TEST_NATS_CRASH_TIMEOUT` defaults to 300 seconds; accepted values are 240 through 600.
This permits recovery past the consumer's 120-second AckWait. No fixed sleep establishes a crash barrier.

The fixture holds a session advisory lock for its validated sender UUID and unique idempotency key.
An owned `docker compose exec -T postgres psql` process emits a readiness marker after taking that lock.
The test starts one background HTTP Submit and observes a consumer waiting on that exact PostgreSQL lock,
matching the hash's high/low words, database, owner PID, and unique session application name.
That consumer-side barrier proves admission; merely sending HTTP or timing out does not.

With the consumer blocked, the test pauses only the local `nats` service, then releases the advisory lock.
Scoped read-only SQL must observe the new submission, matching message, journal result, and unpublished
confirmation outbox together. The journal is matched through both synthetic submission and message UUIDs.
The committed Sent entry's raw object must also be readable through GetRawMessage while NATS is paused.
The owned HTTP request must not have returned application success.

The test then issues only `docker compose kill -s SIGKILL app`, with NATS still paused.
It verifies the same journal confirmation remains unpublished before any restart.
Cleanup unpauses NATS before readiness waits, releases or terminates only the owned lock session,
restores the app and dependencies, and closes the owned HTTP thread.
If lock release fails, termination requires the exact owned backend PID and session name;
before PID readiness, the unique session name guards fallback termination.
No SQL values, mail content, or idempotency keys are printed.
Cleanup failures identify each operation; Compose failures retain only operation, service, and exit status
(or timeout/launch failure). Service-start errors stay visible even if a later cleanup restores health.
An exception group reports all failed cleanup operations without discarding an original test failure.
Restoration uses plain `docker compose start <service>`, without `--wait`, then polls only that service
with `docker compose ps --all --format '{{.State}}|{{.Health}}' <service>` until it reports exactly
`running|healthy`. Existing `unhealthy` status after unpause is a wait condition, never success.
Each service's health deadline is bounded by the fault timeout, with each `ps` command capped at five seconds.
App start follows observed healthy PostgreSQL, Garage, and NATS. A failed dependency blocks app start;
independent service restoration and all outer cleanup operations are still attempted.
No restart, recreation, or reduced health assertion is used to recover from an unhealthy transition.

After restart, GetSubmission polling by the original SQL-proven ID must complete delivery without a Submit retry.
The original confirmation outbox must become published. Only then does the fixture retry the immutable Submit
and require the original submission/message IDs and exactly one recipient Inbox entry and sender Sent entry.
This covers one process-crash boundary, not every crash window or loss of the broker volume.

Run the unchanged exact-25-MiB and oversize cases against NATS to exercise chunk staging above the 1-MiB broker payload bound:

```sh
INBOX_TEST_LARGE=1 tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_acceptance.py -v
```

The wire tests do not inspect JetStream configuration, subject layout, chunks, command rows, or outbox internals.
They therefore establish observable byte fidelity, limits, confirmation behavior, replay, and recovery, not the internal implementation mechanism.

### Legacy fault semantics after the NATS migration

The existing `test_faults.py` assertions remain unchanged pending coordinator-owned runtime evidence.
They require actual API/SMTP errors during dependency outages and currently forbid later delivery of the SMTP input.
That absence assumption can conflict with lost confirmation after a queued command commits;
neither gRPC availability errors nor SMTP `451` alone prove permanent rejection under the new architecture.
The deferred-commit fixture also expects rollback followed by an explicit same-key retry.
Only an actual confirmed permanent rejection supports a no-eventual-commit assertion.
Report a runtime staging/semantic mismatch before adapting these legacy fixtures.
The legacy delivery-lease SQL fixture assumes the original durable delivery schema and must be rechecked before running it against a changed schema.

## Exclusive fault tests

These six tests require exclusive access to the repository's local Compose stack.
Five tests stop and start services; one test installs a temporary PostgreSQL constraint trigger.
They require Docker Compose with `start --wait` support.
The coordinator owns their execution. Do not run them alongside browser, standard acceptance, or other service-dependent tests.
They reject non-local HTTP and SMTP endpoints.

```sh
INBOX_TEST_FAULTS=1 tests/acceptance/.venv/bin/python -m unittest discover -s tests/acceptance -p test_faults.py -v
```

Without `INBOX_TEST_FAULTS=1`, every fault test skips before setup or service commands.
`INBOX_TEST_FAULT_TIMEOUT` defaults to 90 seconds for dependency errors and recovery polls.
`INBOX_TEST_LEASE_SECONDS` defaults to 3 seconds; valid values span 3 through 30.
If app startup exceeds the lease window, increase `INBOX_TEST_LEASE_SECONDS` before the next run.

Fault coverage:

- Garage outage: API non-success and SMTP DATA rejection, followed by same-key durable recovery and exactly one delivery.
- PostgreSQL outage: API non-success and SMTP RCPT or DATA rejection, followed by same-key durable recovery and exactly one delivery.
- Deferred PostgreSQL commit failure after S3 PUT: API `UNAVAILABLE`, SMTP RCPT acceptance followed by DATA `451`, rollback, and same-key recovery.
- Expired lease replay: a prior delivery job returns to pending; the worker reuses the original unique Inbox entry.
- Future lease: a responsive app leaves the job unclaimed before expiry, then recovers after expiry.
- Retry exhaustion: a pending job at eight attempts becomes terminal failure without another claim or duplicate entry.

The outage assertions require actual API and SMTP error responses. A timeout alone does not pass a no-acceptance assertion.
The recovery path retries uncertain API calls with the same idempotency key.
Outage tests print only the dependency name, protocol, and status codes.

The three worker tests first create a successful synthetic submission through the API.
Each test stops the app before a narrowly scoped SQL update to its own delivery row.
Every SQL identifier passes UUID validation. The SQL predicate also requires the test's sender, recipient, submission, and message IDs.
No test updates unrelated records or removes rows, objects, or volumes.
Each fault test restores PostgreSQL, Garage, and the app through `finally` and unittest cleanup.
Cleanup also restores staged synthetic delivery status to its original successful entry.
The suite never invokes `down`, volume commands, or data deletion commands.

### Deferred commit fixture

The commit-failure test creates one uniquely named function and one `DEFERRABLE INITIALLY DEFERRED` constraint trigger on `public.messages`.
Both names contain validated UUID hex. Creation uses a transaction and never uses `OR REPLACE`.
The trigger applies only when the inserted message subject equals this test's unique synthetic subject.
The function also checks that subject before it raises an exception at commit.
All other traffic retains normal behavior.

The API and SMTP attempts share that subject. Their bodies and sender addresses differ.
PostgreSQL logs must show two distinct synthetic message IDs at the deferred trigger.
The test then checks that no subject-specific messages, entries, submissions, or deliveries remain in PostgreSQL.
It removes the fixture before the same-key API retry.
Exactly one recipient Inbox entry and one sender Sent entry must follow that retry.
The recipient entry must contain the API message, not the refused SMTP input.

The `finally` block removes only the exact fixture trigger and function.
Cleanup requires matching ownership comments before each `DROP`; it never uses `CASCADE` or removes user metadata.
It also handles uncertain fixture-creation outcomes without replacing or removing unrelated database objects.
The test reads bounded PostgreSQL logs and emits only the synthetic trigger-hit count.

The source order in `MailService::persist` completes S3 PUT before the message insert and PostgreSQL commit.
Runtime trigger evidence therefore establishes that each rejected request reaches commit after its successful PUT.
The test does not list S3 objects or directly observe orphan inventory.
It leaves any orphan objects intact by design.

## Limits

The standard and stream suites do not stop, restart, or deploy services. Only explicitly enabled fault suites stop and start services.
Dependency outage tests prove observable rejection and recovery, not the exact instruction order around S3 PUT and PostgreSQL commit.
The deferred commit test covers that boundary through its trigger and the production source order.
Lease replay approximates a crash boundary through synthetic SQL state; it does not kill a process inside a transaction.
Retry exhaustion starts at eight attempts; it does not induce eight real transient failures or prove every backoff interval.
The suite does not prove S3-only orphan survival or every crash window.
The discarded-body scenario closes the connection after HTTP headers, before protobuf response decode.
It does not deterministically crash the client between server commit and network acknowledgement.
The suite cannot recover the first response IDs directly; retries and entry counts verify the observable result.
SMTP acknowledgement loss has no idempotency guarantee, so the suite does not retry uncertain SMTP sends.
Pagination uses server-assigned timestamps. It does not force timestamp ties.
The large tests do not construct an API submission whose generated MIME size equals the limit exactly.
No test proves browser, offline, draft, telemetry-content, backup, or restore behavior.
Stream tests permit an explicit replay RESET; they do not force retention expiry, a missing sequence, or a generation change.
Event content checks cover the supplied sensitive fixture strings, not arbitrary encoded or encrypted content leaks.
The separately enabled process-crash scenario targets PostgreSQL commit before NATS confirmation;
other restart scenarios do not establish that boundary or every consumer-acknowledgement window.
They do not deterministically strand an old PostgreSQL delivery job before restarting the app.
Integration results require an actual app run; local protocol checks alone do not establish MVP acceptance.

## Isolated OpenCode MCP event wake check

`opencode_wake.mjs` prepares a separate OpenCode v2.3.0 runtime for one synthetic Inbox event.
Only the root coordinator executes this opt-in test.
Node.js 20 or later supplies the test dependencies.
These checks use no runtime, provider, SMTP connection, or app:

```sh
node --check tests/acceptance/opencode_wake.mjs
node --test tests/acceptance/opencode_wake.test.mjs
```

The live harness verifies the extracted executable SHA256 before launch:
`9f3147cae4ca323827d3ca39451b0d3431b2906e79a08e9b09d1eb34bb703f52`.
The official ZIP archive has a different SHA256:
`7907df2b12720a840fbb49c5b978b9b82925b6bf75479a5fd216afa8932b6702`.
The executable check rejects the archive digest.
Its API contract follows OpenCode commit `a6b15d33daf6ab7d35569f6d711fdb630b9f04e7`:

- [OpenAPI contract](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/protocol/openapi.json).
- [Native watch implementation](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/core/src/tool/plugin/mcp-events.ts).
- [Message response and cursor handler](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/server/src/handlers/message.ts).
- [Synthetic input admission](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/core/src/session/session.ts).
- [Inbox-delivery message projection](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/core/src/session/projector.ts).
- [Execution lifecycle](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/core/src/session/execution.ts).
- [Execution-before-drain order](https://github.com/anomalyco/opencode/blob/a6b15d33daf6ab7d35569f6d711fdb630b9f04e7/packages/core/src/session/run-coordinator.ts).

### Prerequisites and limits

The coordinator supplies an approved temporary directory and a fresh synthetic mailbox UUID.
The coordinator starts the Inbox adapter separately at the repository-configured `/mcp` endpoint.
The project configuration must already enable `experimental.mcp_events` and the `inbox` remote MCP server.
The harness never modifies configuration, grants permissions, changes models, or invokes shared-service commands.
Provider or permission failures block the test.

The private process uses a fresh `OPENCODE_DB`, a loopback ephemeral port, and a random memory-only HTTP password.
The harness verifies `/api/info` version and PID against its owned child before session creation.
The runtime inherits global plugins, MCP configuration, and provider environment.
This isolation is not a full sandbox or a financial spending cap.
The initial prompt restricts agent actions, but it does not install enforcement rules.
The private database can retain session data after exit.
Treat that directory as private evidence; the harness never copies credentials or reads the shared database.

### Registration readiness and delivery proof

**Native watch completion does not prove server-side stream admission.**
The pinned watch implementation forks the event stream before it returns `{watching: "inbox mail.received"}`.
Its public API exposes no Active-admission acknowledgement for that native watch.
The harness reports registration readiness only before fixture delivery.
It requires no Active-admission attestation or `active.json` file.
The final correlated session wake proves native event delivery, not independent Active observation before the fixture.
If the stream misses the fixture, the test fails or times out.
Do not resend the fixture, add a replay cursor, or retry the prompt.
The harness adds no proxy, configuration override, product instrumentation, or callback command.

### Root execution and phase files

After the coordinator authorizes execution, run from the repository root:

```sh
INBOX_WAKE_RUN=1 \
INBOX_WAKE_BINARY=/absolute/path/to/verified/opencode-2.3.0 \
INBOX_WAKE_TEMP_ROOT=/absolute/path/to/approved/temp-directory \
INBOX_WAKE_MAILBOX_ADDRESS=synthetic@inbox.test \
INBOX_WAKE_TIMEOUT=120 \
node tests/acceptance/opencode_wake.mjs
```

Replace the sample UUID with the coordinator's actual synthetic mailbox UUID.
The timeout accepts 90 through 150 seconds and covers startup, the agent turn, fixture coordination, and wake observation.
Cleanup has separate bounded HTTP and child-exit waits.
The coordinator must prepare the fixture before launch.

The harness emits JSON phase records:

1. `PRIVATE_RUNTIME_VERIFIED` confirms the owned PID and exact runtime version.
2. `WATCH_REGISTERED` confirms a completed tool result, the unique assistant marker, and initial execution success.
   It prints `run`, `sessionID`, `mailboxID`, and `eventFile`.
   This phase explicitly reports `admissionProven:false`.
3. Immediately afterward, `READY_FOR_FIXTURE` permits the coordinator to send exactly one synthetic SMTP message externally.
   It reports `readiness:"registration-only"`, `admissionProven:false`, `run`, `sessionID`, and `eventFile`.
   The harness sends no SMTP message and executes no fixture callback.
4. After direct API evidence identifies that message's recipient Inbox entry UUID, write the printed `eventFile`:

   ```json
   {"run":"value from WATCH_REGISTERED","eventID":"actual recipient Inbox entry UUID"}
   ```

   The entry UUID must come from the fixture's independent delivery evidence, not the observed wake alone.
   Publish the complete JSON file atomically to avoid partial reads.
   Keep message content and credentials out of the file.
5. `PASS` requires the exact event ID in one projected `mcp.event` synthetic message for `inbox` / `mail.received`.
   A native `session.inbox.delivered` event must reference that exact synthetic message ID.
   The owned session's initial execution success must precede a new execution start.
   That new execution must start before Inbox delivery and succeed afterward.
   All lifecycle events must share the owned session ID and its durable aggregate.
   Durable sequence values establish lifecycle order; the message list establishes the synthetic-before-assistant order.
   The subsequent assistant must complete with the exact `WAKE_OK` marker.
   Any assistant tool call after that synthetic message prevents a pass.
   Exactly one user message must match the initial prompt's admitted ID.
   It reports `deliveryProof:"matched-native-session-wake"` and `admissionObservedBeforeFixture:false`.
6. `CLEANUP` reports private-directory retention and owned-child exit.
   A complete result requires `PASS`, successful `CLEANUP`, and a zero process exit status.
   `FAILURE` preserves a safe failure reason before cleanup.
   `BLOCKED` returns a nonzero exit status without raw provider, startup, HTTP, prompt, or mail diagnostics.

The harness connects `/api/event` before the only prompt and awaits `server.connected`.
It uses bounded message observation, not another prompt, to check progress.
The pinned message response contains `{data: Session.Message.Info[], cursor: {previous?, next?}}`.
Assistant messages contain `content` parts; completed tool parts contain `state.content` text or file items.
This contract has no `MessageWithParts` wrapper.
The pinned handler returns boundary cursors for every nonempty page, even at the end.
The harness requests 101 rows and rejects more than 100; a `cursor.next` value alone does not indicate overflow.
Separate safe diagnostics identify response-schema errors, item-schema errors, and row-bound overflow without raw response output.
The SSE `data` field contains the native V2 event object directly.
Session-event correlation uses `data.sessionID`, `data.inboxID`, and `durable.aggregateID` / `durable.seq`.
The synthetic message's metadata contains `source`, `server`, `event`, and `eventID`.
The pinned native watch admits synthetic input through the session inbox, not a `session.synthetic` publication.
The execution coordinator publishes `session.execution.started` before its drain promotes that input.
`session.inbox.delivered` then projects the synthetic message with its inbox ID as the message ID.
The harness therefore correlates an execution interval around delivery, not an execution start after synthetic projection.
The agent discovers the actual tool through Code Mode and lists events only for `inbox`.
It registers one native watch with the exact Inbox URI, replies with `ARMED`, and ends its first turn.
The later MCP event must resume the session without another user prompt.

Cleanup interrupts only the owned session, then sends SIGTERM only to the owned child.
If that child exceeds its exit bound, cleanup escalates to SIGKILL on that same child object.
The harness never upgrades OpenCode, restarts the shared service, deletes evidence, or prints its HTTP password.
A watch registration or mock-test pass alone proves no actual agent wake.
