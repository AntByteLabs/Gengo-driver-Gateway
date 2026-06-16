import type { Server, Namespace, Socket } from 'socket.io';
import { createSocketAuthMiddleware } from '../middleware/socket-auth.js';
import { registerRiderHandlers } from '../handlers/rider.handlers.js';
import { resolveRiderActiveTrip } from '../services/session.service.js';
import { logger } from '../logger.js';

/**
 * Configures the /rider Socket.io namespace.
 * The sibling /driver namespace is resolved lazily from `io` at connection time.
 */
export function setupRiderNamespace(io: Server): Namespace {
  const riderNsp: Namespace = io.of('/rider');

  // ── Auth middleware ───────────────────────────────────────────────────────

  riderNsp.use(createSocketAuthMiddleware('rider'));

  // ── Connection handler ────────────────────────────────────────────────────

  riderNsp.on('connection', async (socket: Socket) => {
    const riderId = socket.data.riderId as string;

    logger.info({ riderId, socketId: socket.id }, 'Rider connected');

    // Lazy-resolve driver namespace
    const driverNsp = io.of('/driver');

    // Register handlers FIRST so any event the client emits right on connect
    // isn't dropped while we await the room joins below (same race the driver
    // namespace had). Handlers read socket.data.activeTripId lazily.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    registerRiderHandlers(socket as any, driverNsp);

    // 1. Join personal room
    await socket.join(`rider:${riderId}`);

    // 2. Join trip room. Redis is the source of truth — the handshake's
    // tripId is treated as a hint and only honoured when it matches what
    // Redis has recorded for this rider. Without this check a client could
    // pass any tripId at connect time and listen to (or send chat into)
    // somebody else's trip room.
    const handshakeTripId = (socket.handshake.auth as { tripId?: string })?.tripId;
    let tripId: string | null = null;
    try {
      tripId = await resolveRiderActiveTrip(riderId);
    } catch (err) {
      logger.warn({ err, riderId }, 'Failed to resolve rider active trip');
    }
    if (handshakeTripId && handshakeTripId !== tripId) {
      logger.warn(
        { riderId, handshakeTripId, activeTripId: tripId },
        'Ignoring handshake tripId — does not match rider’s active trip in Redis',
      );
    }
    if (tripId) {
      await socket.join(`trip:${tripId}`);
      socket.data.activeTripId = tripId;
      logger.debug({ riderId, tripId }, 'Rider joined trip room');
    }
  });

  return riderNsp;
}
