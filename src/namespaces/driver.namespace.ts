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

    // 2. Rejoin active trip room. Redis is the source of truth — the
    // handshake-supplied tripId is treated as a *hint* and only honoured when
    // it matches what Redis has recorded for this driver. Without this check,
    // a client could pass an arbitrary tripId on connect and read every
    // event emitted into that trip room (status, location, chat, bargain
    // offers).
    const handshakeTripId = (socket.handshake.auth as { tripId?: string })?.tripId;
    let activeTripId: string | null = null;
    try {
      activeTripId = await resolveDriverActiveTrip(driverId);
    } catch (err) {
      logger.warn({ err, driverId }, 'Failed to resolve driver active trip');
    }
    if (handshakeTripId && handshakeTripId !== activeTripId) {
      logger.warn(
        { driverId, handshakeTripId, activeTripId },
        'Ignoring handshake tripId — does not match driver’s active trip in Redis',
      );
    }
    if (activeTripId) {
      await socket.join(`trip:${activeTripId}`);
      socket.data.activeTripId = activeTripId;
      logger.debug({ driverId, activeTripId }, 'Driver joined trip room');
    }

    // 3. Register all event handlers
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    registerDriverHandlers(socket as any, driverNsp, riderNsp);
  });

  return driverNsp;
}
