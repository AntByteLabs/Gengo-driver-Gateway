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

    // 1. Join personal room
    await socket.join(`rider:${riderId}`);

    // 2. Rejoin active trip room if one exists (reconnect scenario)
    try {
      const activeTripId = await resolveRiderActiveTrip(riderId);
      if (activeTripId) {
        await socket.join(`trip:${activeTripId}`);
        logger.debug({ riderId, activeTripId }, 'Rider rejoined active trip room');
      }
    } catch (err) {
      logger.warn({ err, riderId }, 'Failed to rejoin rider active trip room');
    }

    // 3. Register all event handlers
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    registerRiderHandlers(socket as any, driverNsp, riderNsp);
  });

  return riderNsp;
}
