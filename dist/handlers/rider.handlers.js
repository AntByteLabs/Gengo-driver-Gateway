import { z } from 'zod';
import { ackFail, ackOk } from './chat-ack.js';
import { logger } from '../logger.js';
// ─── Zod schemas for incoming payloads ───────────────────────────────────────
const chatSendSchema = z.object({
    tripId: z.string().min(1),
    text: z.string().min(1).max(2000),
});
// ─── Handler registration ─────────────────────────────────────────────────────
export function registerRiderHandlers(socket, driverNsp) {
    const riderId = socket.data.riderId;
    // ── chat:send ────────────────────────────────────────────────────────────
    // Wire format negotiated with the rider app: incoming `chat:send`,
    // outgoing `trip:update` with `eventType: 'CHAT'` so it rides the same
    // envelope as every other trip-room broadcast on the wire.
    socket.on('chat:send', async (raw, ack) => {
        const result = chatSendSchema.safeParse(raw);
        if (!result.success) {
            logger.warn({ riderId, errors: result.error.flatten() }, 'chat:send validation failed');
            ackFail(ack, 'invalid_payload');
            return;
        }
        const { tripId, text } = result.data;
        // The tripId comes from the client payload, but the only trip a rider
        // is allowed to chat in is the one we joined them to from Redis at
        // connect time. The trip-events consumer wipes activeTripId the moment
        // the trip transitions to COMPLETED/CANCELLED — so chat dies with the
        // ride.
        const activeTripId = socket.data.activeTripId;
        if (!activeTripId || tripId !== activeTripId) {
            logger.warn({ riderId, payloadTripId: tripId, activeTripId }, 'chat:send dropped — rider is not paired with that trip');
            ackFail(ack, 'not_paired');
            return;
        }
        const ts = Date.now();
        const id = `m-${ts}-${riderId.slice(-6)}`;
        const envelope = {
            tripId,
            eventType: 'CHAT',
            payload: { id, from: 'rider', text },
            ts,
        };
        // Forward only to the OTHER side. The rider's own UI shows the message
        // optimistically the moment they type it; echoing back here would
        // either duplicate (different id) or be a no-op (same id) — neither
        // is worth the extra write.
        driverNsp.to(`trip:${tripId}`).emit('trip:update', envelope);
        // Other rider devices in the same trip room (e.g. tablet + phone)
        // still need the new message — emit there too, excluding the sender.
        socket.to(`trip:${tripId}`).emit('trip:update', envelope);
        ackOk(ack);
    });
    // ── disconnect ────────────────────────────────────────────────────────────
    socket.on('disconnect', (reason) => {
        logger.info({ riderId, reason }, 'Rider disconnected');
    });
}
//# sourceMappingURL=rider.handlers.js.map