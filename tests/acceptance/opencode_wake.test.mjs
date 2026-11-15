import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeNativeEvent, EXECUTABLE_SHA256, fixtureReadyFields, onePrompt, parseMessagePage,
  registrationReady, run, safeBlocker, SSEDecoder, stopOwnedChild,
  verifyExecutableDigest, verifyIdentity, waitFor,
  wakeEvidence, watchingResult } from './opencode_wake.mjs';

const userMessage = { id: 'msg_user', type: 'user', time: { created: 1 }, text: 'Synthetic test prompt' };
const assistantMessage = {
  id: 'msg_assistant', type: 'assistant', agent: 'build',
  model: { providerID: 'test', id: 'native-default' }, time: { created: 2 }, content: [],
};
const toolPart = {
  type: 'tool', id: 'call_test', name: 'execute', time: { created: 2, ran: 2, completed: 3 },
  state: { status: 'completed', input: { code: 'synthetic mock code' }, content: [
    { type: 'text', text: '{"watching":"inbox mail.received"}' },
  ] },
};

test('pinned message pages accept empty and pending projections with boundary cursors', () => {
  assert.deepEqual(parseMessagePage({ data: [], cursor: {} }), []);
  assert.deepEqual(parseMessagePage({ data: [], cursor: { previous: null, next: null } }), []);
  const data = [userMessage, assistantMessage, { ...assistantMessage, id: 'msg_running', content: [{
    ...toolPart, state: { status: 'running', input: { code: 'synthetic mock code' }, metadata: {} },
  }] }];
  assert.deepEqual(parseMessagePage({ data, cursor: { previous: 'opaque-start', next: 'opaque-end' } }), data);
  assert.equal(registrationReady(data, [], 'ARMED_unique'), false);
});

test('pinned completed assistant.content tool result and text satisfy registration gate', () => {
  const data = [userMessage,
    { ...assistantMessage, content: [toolPart], time: { created: 2, completed: 3 } },
    { ...assistantMessage, id: 'msg_armed', time: { created: 4, completed: 5 },
      content: [{ type: 'text', text: 'ARMED_unique' }], finish: 'stop' },
  ];
  const page = parseMessagePage({ data, cursor: { previous: 'opaque-start', next: 'opaque-end' } });
  assert.equal(registrationReady(page, [{ type: 'session.execution.succeeded' }], 'ARMED_unique'), true);
});

test('message bounds use row sentinel rather than the always-present next cursor', () => {
  const data = Array.from({ length: 100 }, (_, i) => ({ ...userMessage, id: `msg_${i}` }));
  assert.equal(parseMessagePage({ data, cursor: { next: 'opaque-end' } }).length, 100);
  assert.throws(() => parseMessagePage({ data: [...data, userMessage], cursor: { next: 'opaque-end' } }),
    /Message observation bound exceeded/);
});

test('schema diagnostics distinguish nonarray response and invalid parts from actual overflow', () => {
  for (const page of [[], { data: { private: 'secret' }, cursor: {} }, { data: [], cursor: [] },
    { data: [], cursor: { next: 42 } }]) {
    assert.throws(() => parseMessagePage(page), error =>
      safeBlocker(error) === 'Message response schema mismatch' && !safeBlocker(error).includes('secret'));
  }
  assert.throws(() => parseMessagePage({ data: [{ info: assistantMessage, parts: [] }], cursor: {} }),
    /Message item schema mismatch/);
  assert.throws(() => parseMessagePage({ data: [{ ...assistantMessage, content: [{ type: 'tool', state: {
    status: 'completed', content: 'secret',
  } }] }], cursor: {} }), /Message item schema mismatch/);
});

test('native SSE data contains the V2 envelope directly and preserves session correlation', () => {
  const envelopes = [
    { id: 'evt_connected', type: 'server.connected', data: {} },
    { id: 'evt_started', created: 10, type: 'session.execution.started',
      durable: { aggregateID: 'ses_owned', seq: 41, version: 1 }, data: { sessionID: 'ses_owned' } },
    { id: 'evt_delivered', created: 11, type: 'session.inbox.delivered',
      durable: { aggregateID: 'ses_owned', seq: 42, version: 1 },
      data: { sessionID: 'ses_owned', inboxID: 'msg_mcp_synthetic' } },
  ];
  const decoded = [];
  const parser = new SSEDecoder(frame => decoded.push(decodeNativeEvent(frame)));
  parser.push(Buffer.from(envelopes.map(event => `data: ${JSON.stringify(event)}\n\n`).join('')));
  parser.push(new Uint8Array(), true);
  assert.deepEqual(decoded, envelopes);
  for (const data of ['not JSON', JSON.stringify({ event: envelopes[1] }), JSON.stringify(JSON.stringify(envelopes[1])),
    JSON.stringify({ ...envelopes[1], durable: { aggregateID: 'ses_owned', seq: '4' } }),
    JSON.stringify({ ...envelopes[2], data: { sessionID: 'ses_owned' } })]) {
    assert.throws(() => decodeNativeEvent({ data }), /Event envelope schema mismatch/);
  }
});

test('executable integrity accepts extracted binary digest and rejects archive digest', () => {
  const executable = '9f3147cae4ca323827d3ca39451b0d3431b2906e79a08e9b09d1eb34bb703f52';
  const archive = '7907df2b12720a840fbb49c5b978b9b82925b6bf75479a5fd216afa8932b6702';
  assert.equal(EXECUTABLE_SHA256, executable);
  verifyExecutableDigest(executable);
  assert.throws(() => verifyExecutableDigest(archive), /Binary SHA256 mismatch/);
  assert.throws(() => verifyExecutableDigest('0'.repeat(64)), /Binary SHA256 mismatch/);
});

test('SSE supports every byte split, CRLF, comments, multiline data and UTF-8', () => {
  const bytes = Buffer.from(': comment\r\nevent: message\r\ndata: {"text":\r\ndata: "héllo"}\r\n\r\ndata: {}\n\n');
  for (let split = 0; split <= bytes.length; split++) {
    const frames = [];
    const decoder = new SSEDecoder(frame => frames.push(frame));
    decoder.push(bytes.subarray(0, split));
    decoder.push(bytes.subarray(split));
    decoder.push(new Uint8Array(), true);
    assert.deepEqual(frames, [
      { event: 'message', data: '{"text":\n"héllo"}' }, { event: '', data: '{}' },
    ]);
  }
});

test('SSE rejects truncated and oversized frames', () => {
  const partial = new SSEDecoder(() => {});
  partial.push(Buffer.from('data: {}\n'));
  assert.throws(() => partial.push(new Uint8Array(), true), /truncated/);
  assert.throws(() => new SSEDecoder(() => {}, 10).push(Buffer.from('data: 12345\n\n')), /bound/);
  assert.throws(() => new SSEDecoder(() => {}, 10).push(Buffer.from('x'.repeat(11))), /bound/);
});

test('bounded wait expires and responds to cancellation', async () => {
  await assert.rejects(waitFor(() => false, Date.now() + 15, undefined, 1), /expired/);
  const controller = new AbortController();
  controller.abort(new Error('cancel test'));
  await assert.rejects(waitFor(() => false, Date.now() + 100, controller.signal), /cancel test/);
});

test('runtime identity requires exact version and owned live PID', () => {
  const child = { pid: 123, exitCode: null, signalCode: null };
  verifyIdentity({ version: '2.3.0', pid: 123 }, child);
  for (const info of [{ version: '2.2.7', pid: 123 }, { version: '2.3.0', pid: 124 }]) {
    assert.throws(() => verifyIdentity(info, child), /mismatch/);
  }
  assert.throws(() => verifyIdentity({ version: '2.3.0', pid: 123 }, { ...child, exitCode: 0 }), /mismatch/);
});

test('cleanup signals only the owned child, including bounded escalation', async () => {
  const signals = [];
  const child = { pid: 123, exitCode: null, signalCode: null,
    kill(signal) { signals.push(signal); if (signal === 'SIGKILL') this.signalCode = signal; } };
  await assert.rejects(stopOwnedChild(child, 124, 1), /guard/);
  assert.deepEqual(signals, []);
  await stopOwnedChild(child, 123, 1);
  assert.deepEqual(signals, ['SIGTERM', 'SIGKILL']);
  await stopOwnedChild(child, 123, 1);
  assert.equal(signals.length, 2);
});

test('prompt cannot be retried even after uncertain HTTP failure', async () => {
  let count = 0;
  const send = onePrompt(async () => { count++; throw new Error('transport'); });
  await assert.rejects(send('/prompt', {}), /transport/);
  await assert.rejects(send('/prompt', {}), /Second prompt/);
  assert.equal(count, 1);
});

test('watch registration requires completed structured tool output, not assistant assertion', () => {
  assert.equal(watchingResult({ type: 'text', text: 'Watching inbox mail.received' }), false);
  const tool = { type: 'tool', state: { status: 'completed', content: [
    { type: 'text', text: JSON.stringify({ watching: 'inbox mail.received' }) },
  ] } };
  assert.equal(watchingResult(tool), true);
  assert.equal(watchingResult({ ...tool, state: { ...tool.state, status: 'running' } }), false);
});

test('fixture readiness reports registration only without any Active attestation file', () => {
  assert.deepEqual(fixtureReadyFields('ses_owned', '/private/test/event.json'), {
    sessionID: 'ses_owned', eventFile: '/private/test/event.json',
    readiness: 'registration-only', admissionProven: false,
  });
});

test('registration readiness requires completed watch, ARMED marker and execution success', () => {
  const tool = { type: 'assistant', content: [{ type: 'tool', state: {
    status: 'completed', content: [{ type: 'text', text: '{"watching":"inbox mail.received"}' }],
  } }] };
  const armed = { type: 'assistant', time: { completed: 2 }, content: [{ type: 'text', text: 'ARMED_unique' }] };
  const success = [{ type: 'session.execution.succeeded' }];
  assert.equal(registrationReady([tool, armed], success, 'ARMED_unique'), true);
  assert.equal(registrationReady([tool], success, 'ARMED_unique'), false);
  assert.equal(registrationReady([armed], success, 'ARMED_unique'), false);
  assert.equal(registrationReady([tool, armed], [], 'ARMED_unique'), false);
  assert.equal(registrationReady([tool, armed], success, 'ARMED_other'), false);
});

test('registration, bus notification, or assistant reply alone never proves a wake', () => {
  const assistant = { type: 'assistant', time: { completed: 2 },
    content: [{ type: 'text', text: 'WAKE_OK_unique' }] };
  const registration = { type: 'assistant', content: [{ type: 'tool', state: {
    status: 'completed', content: [{ type: 'text', text: '{"watching":"inbox mail.received"}' }],
  } }] };
  assert.equal(watchingResult(registration.content[0]), true);
  for (const messages of [[registration], [assistant], [registration, assistant]]) {
    assert.equal(wakeEvidence(messages, [], 'entry-id', 'WAKE_OK_unique'), false);
    assert.equal(wakeEvidence(messages, [{ type: 'experimental.mcp.event' }], 'entry-id', 'WAKE_OK_unique'), false);
  }
});

function nativeWakeFixture() {
  const context = { sessionID: 'ses_owned', promptID: userMessage.id, armedMarker: 'ARMED_unique' };
  const eventID = 'd369168e-27d0-4d09-8025-a5c98db665de';
  const marker = 'WAKE_OK_unique';
  const messages = [userMessage,
    { ...assistantMessage, content: [toolPart], time: { created: 2, completed: 3 } },
    { ...assistantMessage, id: 'msg_armed', time: { created: 4, completed: 5 },
      content: [{ type: 'text', text: context.armedMarker }] },
    { id: 'msg_idle_initial', type: 'idle', time: { created: 6 }, outcome: 'succeeded' },
    { id: 'msg_mcp_synthetic', type: 'synthetic', time: { created: 8 }, text: 'Untrusted mock event data',
      metadata: { source: 'mcp.event', server: 'inbox', event: 'mail.received', eventID } },
    { ...assistantMessage, id: 'msg_wake', time: { created: 9, completed: 10 },
      content: [{ type: 'text', text: marker }] },
    { id: 'msg_idle_wake', type: 'idle', time: { created: 11 }, outcome: 'succeeded' },
  ];
  const event = (type, seq, extra = {}) => ({ id: `evt_${seq}`, type, created: seq,
    durable: { aggregateID: context.sessionID, seq, version: 1 }, data: { sessionID: context.sessionID, ...extra } });
  const events = [event('session.execution.started', 3), event('session.inbox.delivered', 4, { inboxID: userMessage.id }),
    event('session.execution.succeeded', 39), event('session.execution.started', 41),
    event('session.inbox.delivered', 42, { inboxID: 'msg_mcp_synthetic' }), event('session.execution.succeeded', 49)];
  return { context, messages, events, marker, eventID };
}

test('actual native lifecycle starts before InboxDelivered projection and needs no session.synthetic event', () => {
  const { messages, events, eventID, marker, context } = nativeWakeFixture();
  const projected = parseMessagePage({ data: messages, cursor: { previous: 'start', next: 'end' } });
  const decoded = events.map(event => decodeNativeEvent({ data: JSON.stringify(event) }));
  assert.equal(decoded.some(e => e.type === 'session.synthetic'), false);
  assert.equal(wakeEvidence(projected, decoded, eventID, marker, context), true);
  assert.equal(wakeEvidence(projected, decoded.toReversed(), eventID, marker, context), true);
});

test('wake proof rejects missing or unrelated delivery and execution lifecycle evidence', () => {
  const { messages, events, eventID, marker, context } = nativeWakeFixture();
  const check = value => wakeEvidence(messages, value, eventID, marker, context);
  for (const seq of [39, 41, 42, 49]) assert.equal(check(events.filter(e => e.durable.seq !== seq)), false);
  assert.equal(check(events.filter(e => e.type !== 'session.inbox.delivered')), false);
  assert.equal(check(events.map(e => e.durable.seq === 42 ? { ...e, data: { ...e.data, inboxID: 'msg_other' } } : e)), false);
  assert.equal(check(events.map(e => e.durable.seq === 42 ? { ...e, data: { ...e.data, sessionID: 'ses_other' } } : e)), false);
  assert.equal(check(events.map(e => e.durable.seq === 41 ? { ...e, durable: { ...e.durable, aggregateID: 'ses_other' } } : e)), false);
  assert.equal(check(events.map(e => e.durable.seq === 41 ? { ...e, durable: { ...e.durable, seq: 43 } } : e)), false);
  assert.equal(check(events.map(e => e.durable.seq === 49 ? { ...e, type: 'session.execution.failed' } : e)), false);
  assert.equal(check([{ type: 'experimental.mcp.event' }]), false);
});

test('wake proof rejects second user input, wrong prompt, wrong synthetic and tools after native input', () => {
  const { messages, events, eventID, marker, context } = nativeWakeFixture();
  const check = value => wakeEvidence(value, events, eventID, marker, context);
  assert.equal(wakeEvidence(messages, events, eventID, marker, { ...context, promptID: 'msg_other' }), false);
  assert.equal(wakeEvidence(messages, events, 'other-entry', marker, context), false);
  assert.equal(wakeEvidence(messages, events, eventID, marker, {}), false);
  assert.equal(check([...messages, { ...userMessage, id: 'msg_second_prompt' }]), false);
  assert.equal(check(messages.filter(m => m.type !== 'synthetic')), false);
  assert.equal(check(messages.map(m => m.type === 'synthetic' ? { ...m, metadata: { ...m.metadata, source: 'user' } } : m)), false);
  assert.equal(check([...messages, { ...messages[4], id: 'msg_duplicate_synthetic' }]), false);
  assert.equal(check(messages.map(m => m.id === 'msg_wake' ? { ...m, content: [...m.content, toolPart] } : m)), false);
  assert.equal(check([...messages, { ...assistantMessage, id: 'msg_extra_tool', content: [toolPart] }]), false);
  assert.equal(check(messages.map(m => m.id === 'msg_wake' ? { ...m, time: { created: 9 } } : m)), false);
  const reordered = [...messages.slice(0, 4), messages[5], messages[4], messages[6]];
  assert.equal(check(reordered), false);
});

test('diagnostics redact arbitrary secret-bearing errors; live run requires explicit opt-in', async () => {
  assert.equal(safeBlocker(new Error('Authorization: secret; private mail')), 'Wake harness failed; no raw diagnostics emitted');
  assert.equal(safeBlocker(new Error('Permission required; no grant attempted')), 'Permission required; no grant attempted');
  await assert.rejects(run({}), /explicitly authorized/);
});
