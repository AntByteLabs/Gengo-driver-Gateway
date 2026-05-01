import type { Server, Namespace, Socket } from 'socket.io';
import { createSocketAuthMiddleware } from '../middleware/socket-auth.js';
import { registerDriverHandlers } from '../handlers/driver.handlers.js';
import { resolveDriverActiveTrip } from '../services/session.service.js';
import { logger } from '../logger.js';

/**
 * Configures the /driver Socket.io namespace.
 * The sibling /rider namespace is resolved lazily from `io` at connection time
 * to avoid forward-reference issues during startup.
 */
export function setupDriverNamespace(io: Server): Namespace {
  const driverNsp: Namespace = io.of('/driver');

  // ── Auth middleware ───────────────────────────────────────────────────────

  driverNsp.use(createSocketAuthMiddleware('driver'));

  // ── Connection handler ────────────────────────────────────────────────────

  driverNsp.on('connection', async (socket: Socket) => {
    const driverId = socket.data.driverId as string;

    logger.info({ driverId, socketId: socket.id }, 'Driver connected');

    // Lazy-resolve rider namespace — both namespaces are set up independently
    const riderNsp = io.of('/rider');

    // 1. Join personal room
    await socket.join(`driver:${driverId}`);

    // 2. Rejoin active trip room if one exists (reconnect scenario)
    try {
      const activeTripId = await resolveDriverActiveTrip(driverId);
      if (activeTripId) {
        await socket.join(`trip:${activeTripId}`);
        // Store on socket.data so the location handler can fan out to the rider room
        socket.data.activeTripId = activeTripId;
        logger.debug({ driverId, activeTripId }, 'Driver rejoined active trip room');
      }
    } catch (err) {
      logger.warn({ err, driverId }, 'Failed to rejoin driver active trip room');
    }

    // 3. Register all event handlers
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    registerDriverHandlers(socket as any, driverNsp, riderNsp);
  });

  return driverNsp;
}
