import { z } from 'zod';
import type { Namespace, Socket } from 'socket.io';
import { config } from '../config.js';
import { logger } from '../logger.js';
import {
  geoAddDriver,
  geoRemoveDriver,
  setDriverOnline,
  setDriverOffline,
  updateDriverLastSeen,
} from '../services/geo.service.js';
import { sendKafkaMessage } from '../infrastructure/kafka.js';

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
});

const driverLocationSchema = z.object({
  lat: z.number().finite(),
  lng: z.number().finite(),
  heading: z.number().finite().optional(),
});

const chatMessageSchema = z.object({
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
    const { vehicleType } = result.data;
    // Remember on this socket so every heartbeat can include it in the
    // Kafka payload that location-svc consumes.
    socket.data.vehicleType = vehicleType;

    try {
      // GEOADD with 0,0 placeholder — first real location update will correct it
      await geoAddDriver(driverId, 0, 0);
      await setDriverOnline(driverId, vehicleType, socket.id);

      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        vehicleType,
        lat: 0,
        lng: 0,
        status: 'available',
        ts: Date.now(),
      });

      logger.info({ driverId, vehicleType }, 'Driver came online');
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
      await updateDriverLastSeen(driverId);

      // Publish location update to Kafka. CRITICAL: include vehicleType so
      // location-svc's UpsertAvailable doesn't overwrite the meta hash with
      // an empty string — that wipe is what causes the matcher to skip the
      // driver regardless of how close they are to the rider.
      const vehicleType = socket.data.vehicleType;
      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        lat,
        lng,
        ...(heading !== undefined && { heading }),
        ...(vehicleType ? { vehicleType } : {}),
        status,
        ts: Date.now(),
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

  // ── chat:message ──────────────────────────────────────────────────────────

  socket.on('chat:message', async (raw: unknown) => {
    const result = chatMessageSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ driverId, errors: result.error.flatten() }, 'chat:message validation failed');
      return;
    }
    const { tripId, text } = result.data;

    // Real-time relay only — never persist
    riderNsp.to(`trip:${tripId}`).emit('chat:message', {
      tripId,
      from: 'driver',
      text,
      ts: Date.now(),
    });
  });

  // ── disconnect ────────────────────────────────────────────────────────────

  socket.on('disconnect', async (reason) => {
    logger.info({ driverId, reason }, 'Driver disconnected');

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
      logger.error({ err, driverId }, 'Error handling driver disconnect');
    }
  });
}
