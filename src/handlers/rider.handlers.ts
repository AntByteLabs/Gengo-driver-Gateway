import { z } from 'zod';
import type { Namespace, Socket } from 'socket.io';
import { logger } from '../logger.js';

// ─── Extended socket.data type for the rider namespace ───────────────────────

interface RiderSocketData {
  riderId: string;
}

// ─── Zod schemas for incoming payloads ───────────────────────────────────────

const chatMessageSchema = z.object({
  tripId: z.string().min(1),
  text: z.string().min(1).max(2000),
});

// ─── Handler registration ─────────────────────────────────────────────────────

export function registerRiderHandlers(
  socket: Socket<
    Record<string, never>,
    Record<string, never>,
    Record<string, never>,
    RiderSocketData
  >,
  driverNsp: Namespace,
  _riderNsp: Namespace,
): void {
  const riderId = socket.data.riderId;

  // ── chat:message ──────────────────────────────────────────────────────────

  socket.on('chat:message', async (raw: unknown) => {
    const result = chatMessageSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ riderId, errors: result.error.flatten() }, 'chat:message validation failed');
      return;
    }
    const { tripId, text } = result.data;

    // Forward to every driver socket in the trip room (real-time relay — never persist)
    driverNsp.to(`trip:${tripId}`).emit('chat:message', {
      tripId,
      from: 'rider',
      text,
      ts: Date.now(),
    });
  });

  // ── disconnect ────────────────────────────────────────────────────────────

  socket.on('disconnect', (reason) => {
    logger.info({ riderId, reason }, 'Rider disconnected');
  });
}
