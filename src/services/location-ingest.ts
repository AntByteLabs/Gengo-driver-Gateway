import type { Namespace } from 'socket.io';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { sendKafkaMessage } from '../infrastructure/kafka.js';

export interface DriverLocationIngest {
  driverId: string;
  /** The driver's active trip, if any. When set, the fix is forwarded to that
   *  trip's rider room so the rider's map keeps moving. */
  activeTripId: string | null;
  lat: number;
  lng: number;
  heading?: number;
  vehicleType?: string;
  /** 'available' | 'on_trip' — location-svc branches on this to keep the driver
   *  in or out of the dispatch geo-set. */
  status: string;
}

/**
 * The single path a driver's position takes into the platform, shared by the
 * socket heartbeat (driver.handlers) and the background HTTP ping (screen-locked
 * mid-trip, location-ping.route):
 *   1. publish to Kafka (location-svc writes the geo-set + history), and
 *   2. when the driver is on a trip, forward it to that trip's rider room so the
 *      rider's live map keeps moving.
 * Kafka is fire-and-forget: location beats are lossy-tolerant and awaiting the
 * broker RTT would block the hottest path in the service.
 */
export function ingestDriverLocation(riderNsp: Namespace, ev: DriverLocationIngest): void {
  const ts = Date.now();

  sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, ev.driverId, {
    driverId: ev.driverId,
    lat: ev.lat,
    lng: ev.lng,
    ...(ev.heading !== undefined && { heading: ev.heading }),
    ...(ev.vehicleType ? { vehicleType: ev.vehicleType } : {}),
    status: ev.status,
    ts,
  }).catch((err) => {
    logger.error({ err, driverId: ev.driverId }, 'Failed to publish driver location to Kafka');
  });

  if (ev.activeTripId) {
    riderNsp.to(`trip:${ev.activeTripId}`).emit('driver:location', {
      tripId: ev.activeTripId,
      lat: ev.lat,
      lng: ev.lng,
      ...(ev.heading !== undefined && { heading: ev.heading }),
      ts,
    });
  }
}
