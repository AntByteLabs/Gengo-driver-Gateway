import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import type { Namespace } from 'socket.io';
import { getKafkaConsumer } from '../infrastructure/kafka.js';
import { getRedisDataClient } from '../infrastructure/redis.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import {
  clearDriverActiveTrip,
  clearRiderActiveTrip,
} from '../services/geo.service.js';
import type { KafkaTripEvent, KafkaNotificationOutboxEvent } from '../domain/types.js';

// ─── Idempotency for app:config:bumped ───────────────────────────────────────
// The notification.outbox topic can replay (consumer rebalance, manual
// resets) and would re-broadcast `app:config:bumped` to every connected
// client, kicking apps to re-fetch config unnecessarily. We dedupe on the
// version number using SETNX with a short TTL: the first pod to see a
// version wins; all others (including replayed deliveries) drop it.
const APP_CONFIG_DEDUPE_TTL_SEC = 600;
function appConfigDedupeKey(version: number): string {
  return `gw:dedupe:appconfig:v${version}`;
}

async function shouldBroadcastAppConfig(version: number): Promise<boolean> {
  try {
    const redis = getRedisDataClient();
    // ioredis NX+EX form returns 'OK' on first write, null on duplicate.
    const res = await redis.set(appConfigDedupeKey(version), '1', 'EX', APP_CONFIG_DEDUPE_TTL_SEC, 'NX');
    return res === 'OK';
  } catch (err) {
    // Fail-open: if Redis is unavailable we'd rather double-broadcast
    // than miss a real config bump. Operationally a duplicate just means
    // clients re-fetch the same config object.
    logger.warn({ err, version }, 'appConfig dedupe check failed; broadcasting anyway');
    return true;
  }
}

/** Trip statuses that mean the ride is over — chat + the trip room must
 *  be torn down once we see one of these. */
const TERMINAL_STATUSES = new Set(['COMPLETED', 'CANCELLED']);

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

// trip-svc emits this when a driver responds to a dispatch with accept-at-fare
// or a counter-bid. The rider's SearchingView consumes the forwarded event to
// populate `driverResponses[]`.
const bargainOfferSchema = z.object({
  type: z.literal('trip.bargain_offer'),
  payload: z.object({
    tripId: z.string(),
    offerId: z.string(),
    driverId: z.string(),
    driverName: z.string(),
    driverRating: z.number(),
    vehicle: z.string(),
    plate: z.string(),
    distanceKm: z.number(),
    offerFareNPR: z.number(),
    expiresAt: z.number(),
    issuedAt: z.number(),
  }),
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
        await handleNotificationOutbox(parsed, driverNsp, riderNsp);
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
  // Bargain offers ride the same topic but have a different payload shape;
  // route on the discriminator before validating against the lifecycle schema.
  if (typeof raw === 'object' && raw !== null && (raw as { type?: string }).type === 'trip.bargain_offer') {
    handleBargainOffer(raw, riderNsp);
    return;
  }

  const result = tripEventSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.events message validation failed');
    return;
  }

  const event = result.data as KafkaTripEvent;
  const { tripId, riderId, driverId, driverName, status } = event.payload;
  const ts = event.payload.ts ?? Date.now();
  // Carry the upstream eventId if trip-svc supplied one; otherwise mint a
  // local one so every emit has a traceable id for debugging.
  const upstreamEventId =
    typeof (event.payload as { eventId?: unknown }).eventId === 'string'
      ? ((event.payload as unknown as { eventId: string }).eventId)
      : undefined;
  const eventId = upstreamEventId ?? randomUUID();

  // Emit trip:status to all riders in the trip room
  riderNsp.to(`trip:${tripId}`).emit('trip:status', {
    tripId,
    status,
    driverId,
    driverName,
    ts,
    meta: { eventId },
  });

  // If it's a cancellation also notify driver with generic event
  if (event.type === 'TRIP_CANCELLED' && driverId) {
    driverNsp.to(`driver:${driverId}`).emit('event', {
      type: 'TRIP_CANCELLED',
      payload: event.payload,
      meta: { eventId },
    });
  }

  // Bargain → match: rider picked this driver. The driver's offer card
  // should turn into an active trip.
  if (event.type === 'trip.matched' && driverId) {
    driverNsp.to(`driver:${driverId}`).emit('driver:trip:assigned', {
      trip: { tripId, riderId, status, driverId, ts },
      meta: { eventId },
    });
  }

  // End-of-trip teardown — once the ride is over, chat must stop working.
  // Three things have to happen:
  //   1. Every socket sitting in `trip:{tripId}` is forced to leave that
  //      room — otherwise the trip-update broadcast still reaches them.
  //   2. Their `socket.data.activeTripId` is cleared — otherwise the
  //      chat:send handler's pairing check would still pass.
  //   3. Redis `activeTrip:rider/driver:` keys are cleared as a backstop,
  //      in case trip-svc didn't (the schema doesn't yet say who owns this
  //      side-effect, so we do it ourselves).
  if (TERMINAL_STATUSES.has(status)) {
    void teardownTripRoom(tripId, riderId, driverId, driverNsp, riderNsp);
  }

  logger.debug({ tripId, riderId, status, type: event.type }, 'Processed trip event');
}

function handleBargainOffer(raw: unknown, riderNsp: Namespace): void {
  const result = bargainOfferSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.bargain_offer validation failed');
    return;
  }
  const { tripId, offerId } = result.data.payload;
  // offerId is a stable per-offer identifier from trip-svc — reuse it as
  // the eventId so a Kafka redelivery surfaces with the same id and
  // clients can dedupe by it.
  riderNsp.to(`trip:${tripId}`).emit('trip:bargain_offer', {
    ...result.data.payload,
    meta: { eventId: offerId },
  });
  logger.debug({ tripId, offerId }, 'Forwarded bargain offer to rider');
}

async function teardownTripRoom(
  tripId: string,
  riderId: string,
  driverId: string | undefined,
  driverNsp: Namespace,
  riderNsp: Namespace,
): Promise<void> {
  const room = `trip:${tripId}`;
  try {
    // socketsLeave + iterating fetchSockets to clear data are independent —
    // run them in parallel for speed.
    const [riderSockets, driverSockets] = await Promise.all([
      riderNsp.in(room).fetchSockets(),
      driverNsp.in(room).fetchSockets(),
    ]);
    for (const s of riderSockets) {
      if ((s.data as { activeTripId?: string }).activeTripId === tripId) {
        (s.data as { activeTripId?: string }).activeTripId = undefined;
      }
    }
    for (const s of driverSockets) {
      if ((s.data as { activeTripId?: string }).activeTripId === tripId) {
        (s.data as { activeTripId?: string }).activeTripId = undefined;
      }
    }
    await Promise.all([
      riderNsp.in(room).socketsLeave(room),
      driverNsp.in(room).socketsLeave(room),
    ]);
    await Promise.all([
      clearRiderActiveTrip(riderId).catch(() => {}),
      driverId ? clearDriverActiveTrip(driverId).catch(() => {}) : Promise.resolve(),
    ]);
    logger.info({ tripId, riderId, driverId }, 'Tore down trip room after terminal status');
  } catch (err) {
    logger.warn({ err, tripId }, 'Trip-room teardown failed');
  }
}

async function handleNotificationOutbox(
  raw: unknown,
  driverNsp: Namespace,
  riderNsp: Namespace,
): Promise<void> {
  const result = notificationOutboxSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'notification.outbox message validation failed');
    return;
  }

  const event = result.data as KafkaNotificationOutboxEvent;

  if (event.type === 'app:config:bumped') {
    const version = event.payload.version;
    if (typeof version !== 'number') {
      logger.warn('app:config:bumped received without numeric version; skipping');
      return;
    }

    // Dedupe across pods + Kafka redeliveries on the version number. If
    // we've already broadcast this version in the last 10 minutes, skip
    // the fan-out — every connected client refetches config the moment
    // they see the event, so a duplicate just hammers app-config-svc.
    const fresh = await shouldBroadcastAppConfig(version);
    if (!fresh) {
      logger.debug({ version }, 'app:config:bumped duplicate suppressed');
      return;
    }

    const eventId = randomUUID();
    driverNsp.emit('app:config:bumped', { version, meta: { eventId } });
    riderNsp.emit('app:config:bumped', { version, meta: { eventId } });
    logger.info({ version, eventId }, 'Broadcasted app:config:bumped');
  }
}
