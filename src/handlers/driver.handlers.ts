import { z } from 'zod';
import type { Namespace, Socket } from 'socket.io';
import { config } from '../config.js';
import { logger } from '../logger.js';
import {
  geoAddDriver,
  geoRemoveDriver,
  setDriverOnline,
  setDriverOffline,
} from '../services/geo.service.js';
import { sendKafkaMessage } from '../infrastructure/kafka.js';
import { getDriverApprovalStatus } from '../services/approval.service.js';
import { ackFail, ackOk } from './chat-ack.js';

// ─── Extended socket.data type for the driver namespace ──────────────────────

interface DriverSocketData {
  driverId: string;
  activeTripId?: string;
  /** Set when the driver emits `driver:online` so subsequent heartbeats can
   *  forward vehicleType to the Kafka location topic — without it,
   *  location-svc's UpsertAvailable wipes the meta hash on every tick. */
  vehicleType?: string;
}

// ─── Zod schemas for incoming payloads ───────────────────────────────────────

const driverOnlineSchema = z.object({
  vehicleType: z.string().min(1),
  // Optional current GPS fix. When the client sends it, we register the driver
  // at their REAL location immediately instead of the (0,0) placeholder — so
  // the matcher can find them before their first location heartbeat lands.
  lat: z.number().finite().optional(),
  lng: z.number().finite().optional(),
  // Optional preferred-route corridor. When present, trip-svc only offers the
  // driver rides whose pickup is within the admin-configured distance of the
  // line between origin and destination.
  preferredRoute: z
    .object({
      originLat: z.number().finite(),
      originLng: z.number().finite(),
      destLat: z.number().finite(),
      destLng: z.number().finite(),
    })
    .optional(),
});

const driverLocationSchema = z.object({
  lat: z.number().finite(),
  lng: z.number().finite(),
  heading: z.number().finite().optional(),
});

const chatSendSchema = z.object({
  tripId: z.string().min(1),
  text: z.string().min(1).max(2000),
});

// ─── Handler registration ─────────────────────────────────────────────────────

export function registerDriverHandlers(
  socket: Socket<
    Record<string, never>,
    Record<string, never>,
    Record<string, never>,
    DriverSocketData
  >,
  driverNsp: Namespace,
  riderNsp: Namespace,
): void {
  const driverId = socket.data.driverId;

  // ── driver:online ─────────────────────────────────────────────────────────

  socket.on('driver:online', async (raw: unknown) => {
    const result = driverOnlineSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ driverId, errors: result.error.flatten() }, 'driver:online validation failed');
      return;
    }
    const { vehicleType, preferredRoute, lat, lng } = result.data;
    // Use the client's real fix when present and non-zero; otherwise fall back
    // to the (0,0) placeholder that the first location heartbeat will correct.
    const hasFix = lat !== undefined && lng !== undefined && (lat !== 0 || lng !== 0);
    const onlineLat = hasFix ? lat : 0;
    const onlineLng = hasFix ? lng : 0;
    const routeMeta = preferredRoute
      ? `${preferredRoute.originLat},${preferredRoute.originLng};${preferredRoute.destLat},${preferredRoute.destLng}`
      : '';

    // ── Approval gate ────────────────────────────────────────────────────
    // Only KYC-approved drivers may enter the available pool. driver-svc is
    // the source of truth ("is this user allowed to drive?" is a domain
    // question, not a JWT question — CLAUDE.md §5.4); the answer is cached
    // in Redis for 5 minutes. Fail closed: if approval can't be verified,
    // the driver does not go online.
    const rawToken: unknown = socket.handshake.auth?.token;
    const approval = await getDriverApprovalStatus(
      driverId,
      typeof rawToken === 'string' ? rawToken : '',
    );
    if (approval !== 'APPROVED') {
      logger.warn({ driverId, approval }, 'driver:online rejected — driver not approved');
      driverNsp.to(socket.id).emit('driver:error', {
        code: 'DRIVER_NOT_APPROVED',
        message:
          approval === null
            ? 'Could not verify driver approval. Please try again shortly.'
            : 'Your driver account is not approved yet. Complete KYC and wait for approval.',
      });
      return;
    }

    // Remember on this socket so every heartbeat can include it in the
    // Kafka payload that location-svc consumes.
    socket.data.vehicleType = vehicleType;

    try {
      // Register at the driver's real fix when the client sent one; otherwise a
      // (0,0) placeholder that the first location heartbeat will correct.
      await geoAddDriver(driverId, onlineLat, onlineLng);
      await setDriverOnline(driverId, vehicleType, socket.id, routeMeta);

      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        vehicleType,
        lat: onlineLat,
        lng: onlineLng,
        status: 'available',
        ts: Date.now(),
      });

      logger.info({ driverId, vehicleType, hasFix }, 'Driver came online');
    } catch (err) {
      logger.error({ err, driverId }, 'Error handling driver:online');
    }
  });

  // ── driver:location / driver:location_update ──────────────────────────────
  // The mobile clients emit `driver:location_update` per FRONTEND_INTEGRATION
  // §D1; an older variant sent `driver:location`. Subscribe to both so a
  // version mismatch doesn't silently strand drivers at (0,0).
  //
  // The payload may include a `status` field per the spec; the gateway
  // doesn't currently change behaviour based on it, but we pass it through
  // to Kafka so location-svc can demote on_trip drivers correctly later.
  const locationUpdateSchema = driverLocationSchema.extend({
    status: z.enum(['AVAILABLE', 'ON_TRIP', 'OFFLINE', 'available', 'on_trip', 'offline']).optional(),
    timestamp: z.number().optional(),
  });

  const handleLocation = async (raw: unknown, eventName: string) => {
    const result = locationUpdateSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ driverId, eventName, errors: result.error.flatten() }, 'driver location validation failed');
      return;
    }
    const { lat, lng, heading, status: rawStatus } = result.data;
    // Normalise to lowercase for the rest of the pipeline; location-svc and
    // the matcher both expect 'available' (lowercase).
    let status = rawStatus ? rawStatus.toLowerCase() : 'available';
    // A location heartbeat is proof the driver's socket is live, so they are
    // online by definition. The online/offline lifecycle is owned by
    // `driver:online` and the disconnect handler — never by a per-tick status
    // field. Some clients stamp every heartbeat with "offline", which made
    // location-svc ZREM the driver from `drivers:available:geo`, so the matcher
    // found "no nearby drivers" and no offer ever reached the driver side.
    // Treat a heartbeat "offline" as available (or on_trip when the gateway
    // knows the driver is mid-trip) instead of evicting them.
    if (status === 'offline') {
      status = socket.data.activeTripId ? 'on_trip' : 'available';
    }

    try {
      // No direct lastSeen HSET here: location-svc's consumer writes
      // `lastSeen` (from the event's `ts`) into the same `drivers:meta:` hash
      // on every beat, so the gateway-side write was a redundant serial
      // Redis round-trip on the hottest path in the service.

      // Publish location update to Kafka. CRITICAL: include vehicleType so
      // location-svc's UpsertAvailable doesn't overwrite the meta hash with
      // an empty string — that wipe is what causes the matcher to skip the
      // driver regardless of how close they are to the rider.
      // Fire-and-forget: location beats are lossy-tolerant, and awaiting the
      // produce inline made each socket handler block on broker RTT.
      const vehicleType = socket.data.vehicleType;
      sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        lat,
        lng,
        ...(heading !== undefined && { heading }),
        ...(vehicleType ? { vehicleType } : {}),
        status,
        ts: Date.now(),
      }).catch((err) => {
        logger.error({ err, driverId, eventName }, 'Failed to publish driver location to Kafka');
      });

      // If driver has an active trip, forward location to the trip room in /rider namespace
      const activeTripId = socket.data.activeTripId;
      if (activeTripId) {
        riderNsp.to(`trip:${activeTripId}`).emit('driver:location', {
          tripId: activeTripId,
          lat,
          lng,
          ...(heading !== undefined && { heading }),
          ts: Date.now(),
        });
      }
    } catch (err) {
      logger.error({ err, driverId, eventName }, 'Error handling driver location');
    }
  };

  socket.on('driver:location', (raw: unknown) => { void handleLocation(raw, 'driver:location'); });
  socket.on('driver:location_update', (raw: unknown) => { void handleLocation(raw, 'driver:location_update'); });

  // ── chat:send ─────────────────────────────────────────────────────────────
  // Mirror of rider.handlers.ts: incoming `chat:send`, outgoing `trip:update`
  // with `eventType: 'CHAT'` so both sides ride the same trip-room envelope.
  // (The old `chat:message` handler was removed: no client ever emitted it,
  // it trusted the client-supplied tripId with no membership check, and it
  // relayed an event name riders don't listen to.)

  socket.on('chat:send', async (raw: unknown, ack?: unknown) => {
    const result = chatSendSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ driverId, errors: result.error.flatten() }, 'chat:send validation failed');
      ackFail(ack, 'invalid_payload');
      return;
    }
    const { tripId, text } = result.data;

    // The tripId comes from the client payload, but the only trip a driver
    // is allowed to chat in is the one Redis paired them with at connect
    // time (or via the trip-events consumer). The consumer wipes
    // activeTripId the moment the trip ends — so chat dies with the ride.
    const activeTripId = socket.data.activeTripId;
    if (!activeTripId || tripId !== activeTripId) {
      logger.warn(
        { driverId, payloadTripId: tripId, activeTripId },
        'chat:send dropped — driver is not paired with that trip',
      );
      ackFail(ack, 'not_paired');
      return;
    }

    const ts = Date.now();
    const id = `m-${ts}-${driverId.slice(-6)}`;
    const envelope = {
      tripId,
      eventType: 'CHAT' as const,
      payload: { id, from: 'driver' as const, text },
      ts,
    };

    // Relay to the rider side of the trip room, plus any other driver-side
    // sockets in the same room (excluding the sender, whose UI already
    // shows the message optimistically).
    riderNsp.to(`trip:${tripId}`).emit('trip:update', envelope);
    socket.to(`trip:${tripId}`).emit('trip:update', envelope);

    ackOk(ack);
  });

  // Remove this driver from the available pool: drop from the Redis geo-set,
  // mark meta offline, and emit an offline location event so location-svc and
  // the matcher converge immediately. Shared by the explicit driver:offline
  // handler and the socket disconnect handler (the only two ways a driver
  // leaves the pool) so the sequence has a single source of truth.
  const removeFromPool = async (context: string): Promise<void> => {
    try {
      await geoRemoveDriver(driverId);
      await setDriverOffline(driverId);
      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        lat: 0,
        lng: 0,
        status: 'offline',
        ts: Date.now(),
      });
    } catch (err) {
      logger.error({ err, driverId }, `Error handling ${context}`);
    }
  };

  // ── driver:offline ──────────────────────────────────────────────────────────
  // Explicit "go offline" — the driver tapped the shift toggle but keeps the
  // socket open (so they can still browse the open-rides feed while off duty).
  // This is the ONLY driver-initiated way to leave the available pool short of
  // disconnecting: a location heartbeat stamped "offline" is deliberately
  // coerced back to available/on_trip above (buggy clients stamp every beat
  // that way), so we must NOT overload location for the lifecycle signal.
  socket.on('driver:offline', async () => {
    logger.info({ driverId }, 'Driver went offline (explicit)');
    await removeFromPool('driver:offline');
  });

  // ── disconnect ────────────────────────────────────────────────────────────

  socket.on('disconnect', async (reason) => {
    logger.info({ driverId, reason }, 'Driver disconnected');
    await removeFromPool('driver disconnect');
  });
}
