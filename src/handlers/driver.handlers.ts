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

    try {
      // GEOADD with 0,0 placeholder — first real location update will correct it
      await geoAddDriver(driverId, 0, 0);
      await setDriverOnline(driverId, vehicleType, socket.id);

      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
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

  // ── driver:location ───────────────────────────────────────────────────────

  socket.on('driver:location', async (raw: unknown) => {
    const result = driverLocationSchema.safeParse(raw);
    if (!result.success) {
      logger.warn({ driverId, errors: result.error.flatten() }, 'driver:location validation failed');
      return;
    }
    const { lat, lng, heading } = result.data;

    try {
      await updateDriverLastSeen(driverId);

      // Publish location update to Kafka
      await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
        driverId,
        lat,
        lng,
        ...(heading !== undefined && { heading }),
        status: 'available',
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
      logger.error({ err, driverId }, 'Error handling driver:location');
    }
  });

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
