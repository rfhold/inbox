import { expect, it } from 'vitest';
import { Code, ConnectError } from '@connectrpc/connect';
import { commandIntent, definitiveRejection, sendRejected } from './intents';

it('reuses exact mailbox intent keys and includes the selected domain in equality', () => {
  const payload = {localPart: 'team', name: 'Team', domainId: 'one'};
  const first = commandIntent(undefined, payload);
  expect(commandIntent(first, {...payload})).toBe(first);
  expect(Object.isFrozen(first)).toBe(true);
  expect(commandIntent(first, {...payload, domainId: 'two'}).requestId).not.toBe(first.requestId);
});
it('keys independent domain policies by every explicit flag and domain identity', () => {
  const policy = {domainId: 'one', receiveEnabled: false, sendEnabled: true};
  const first = commandIntent(undefined, policy);
  expect(commandIntent(first, policy)).toBe(first);
  for (const change of [{domainId: 'two'}, {receiveEnabled: true}, {sendEnabled: false}])
    expect(commandIntent(first, {...policy, ...change}).requestId).not.toBe(first.requestId);
});
it('permits correction only after definitive send rejection, never transport uncertainty', () => {
  for (const code of [Code.InvalidArgument, Code.FailedPrecondition]) expect(sendRejected(new ConnectError('synthetic', code))).toBe(true);
  for (const code of [Code.Unavailable, Code.DeadlineExceeded, Code.Unknown, Code.Canceled, Code.Internal]) {
    const error = new ConnectError('synthetic', code);
    expect(sendRejected(error)).toBe(false);
    expect(definitiveRejection(error)).toBe(false);
  }
  expect(sendRejected(new Error('lost confirmation'))).toBe(false);
});
