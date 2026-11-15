import { Code, ConnectError } from '@connectrpc/connect';

export type Intent<T> = Readonly<T & {requestId: string}>;
export function commandIntent<T extends object>(previous: Intent<T> | undefined, payload: T): Intent<T> {
  if (previous && Object.entries(payload).every(([key, value]) => previous[key as keyof T] === value)) return previous;
  return Object.freeze({...payload, requestId: crypto.randomUUID()});
}
export const definitiveRejection = (error: unknown) => error instanceof ConnectError
  && [Code.InvalidArgument, Code.FailedPrecondition, Code.AlreadyExists, Code.NotFound].includes(error.code);
export const sendRejected = (error: unknown) => error instanceof ConnectError
  && [Code.InvalidArgument, Code.FailedPrecondition].includes(error.code);
