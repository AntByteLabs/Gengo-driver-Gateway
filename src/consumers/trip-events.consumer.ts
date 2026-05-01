import { z } from 'zod';
import type { Namespace } from 'socket.io';
import { getKafkaConsumer } from '../infrastructure/kafka.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import type { KafkaTripEvent, KafkaNotificationOutboxEvent } from '../domain/types.js';

// ─── Zod schemas ──────────────────────────────────────────────────────────────

const tripEventSchema = z.object({
  type: z.string(),
  payload: z.object({
    tripId: z.string(),
    riderId: z.string(),
    driverId: z.string().optional(),
    driverName: z.string().optional(),
    status: z.string(),
    ts: z.number().optional(),
  }).passthrough(),
});

const notificationOutboxSchema = z.object({
  type: z.string(),
  payload: z.object({
    version: z.number().optional(),
  }).passthrough(),
});

// ─── Consumer bootstrap ───────────────────────────────────────────────────────

export async function startTripEventsConsumer(
  driverNsp: Namespace,
  riderNsp: Namespace,
): Promise<void> {
  const consumer = await getKafkaConsumer();

  await consumer.subscribe({
    topics: [
      config.KAFKA_TOPIC_TRIP_EVENTS,
      config.KAFKA_TOPIC_NOTIFICATION_OUTBOX,
    ],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;

      let parsed: unknown;
      try {
        parsed = JSON.parse(message.value.toString());
      } catch {
        logger.warn({ topic }, 'Failed to parse Kafka message as JSON');
        return;
      }

      if (topic === config.KAFKA_TOPIC_TRIP_EVENTS) {
        handleTripEvent(parsed, driverNsp, riderNsp);
      } else if (topic === config.KAFKA_TOPIC_NOTIFICATION_OUTBOX) {
        handleNotificationOutbox(parsed, driverNsp, riderNsp);
      }
    },
  });

  logger.info(
    { topics: [config.KAFKA_TOPIC_TRIP_EVENTS, config.KAFKA_TOPIC_NOTIFICATION_OUTBOX] },
    'Kafka trip-events consumer running',
  );
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

function handleTripEvent(
  raw: unknown,
  driverNsp: Namespace,
  riderNsp: Namespace,
): void {
  const result = tripEventSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.events message validation failed');
    return;
  }

  const event = result.data as KafkaTripEvent;
  const { tripId, riderId, driverId, driverName, status } = event.payload;
  const ts = event.payload.ts ?? Date.now();

  // Emit trip:status to all riders in the trip room
  riderNsp.to(`trip:${tripId}`).emit('trip:status', {
    tripId,
    status,
    driverId,
    driverName,
    ts,
  });

  // If it's a cancellation also notify driver with generic event
  if (event.type === 'TRIP_CANCELLED' && driverId) {
    driverNsp.to(`driver:${driverId}`).emit('event', {
      type: 'TRIP_CANCELLED',
      payload: event.payload,
    });
  }

  logger.debug({ tripId, riderId, status, type: event.type }, 'Processed trip event');
}

function handleNotificationOutbox(
  raw: unknown,
  driverNsp: Namespace,
  riderNsp: Namespace,
): void {
  const result = notificationOutboxSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'notification.outbox message validation failed');
    return;
  }

  const event = result.data as KafkaNotificationOutboxEvent;

  if (event.type === 'app:config:bumped') {
    const version = event.payload.version;
    driverNsp.emit('app:config:bumped', { version });
    riderNsp.emit('app:config:bumped', { version });
    logger.info({ version }, 'Broadcasted app:config:bumped');
  }
}
