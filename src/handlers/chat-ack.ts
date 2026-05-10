// Tiny helpers for the Socket.IO ack callback used by `chat:send`.
//
// Mobile clients pass `(payload, ack)` and expect ack to be invoked with
// `(ok: boolean, err?: string)` so their `emitChat()` Promise resolves.
// Some emitters fire-and-forget (no ack); skipping the call when the arg
// isn't a function keeps that path safe.

export type ChatAck = (ok: boolean, err?: string) => void;

export function ackOk(ack: unknown): void {
  if (typeof ack === 'function') (ack as ChatAck)(true);
}

export function ackFail(ack: unknown, err: string): void {
  if (typeof ack === 'function') (ack as ChatAck)(false, err);
}
