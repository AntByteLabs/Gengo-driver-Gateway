// Tiny helpers for the Socket.IO ack callback used by `chat:send`.
//
// Mobile clients pass `(payload, ack)` and expect ack to be invoked with
// `(ok: boolean, err?: string)` so their `emitChat()` Promise resolves.
// Some emitters fire-and-forget (no ack); skipping the call when the arg
// isn't a function keeps that path safe.
export function ackOk(ack) {
    if (typeof ack === 'function')
        ack(true);
}
export function ackFail(ack, err) {
    if (typeof ack === 'function')
        ack(false, err);
}
//# sourceMappingURL=chat-ack.js.map