import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import type { Namespace } from 'socket.io';
import { getKafkaConsumer } from '../infrastructure/kafka.js';
import { getRedisDataClient } from '../infrastructure/redis.js';
import { closeChatForTrip, clearChatWindow } from '../services/chat-window.service.js';
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
    // Cancellation cause (e.g. 'no_drivers_available') forwarded to the rider.
    reason: z.string().optional(),
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

// Rider counter-back — same payload shape as bargain_offer, forwarded to
// the *driver* socket so the driver can respond again. Re-using the wire
// shape keeps the driver UI's bargain logic symmetric.
const counterBackSchema = z.object({
  type: z.literal('trip.counter_back'),
  payload: z.object({
    tripId: z.string(),
    offerId: z.string(),
    driverId: z.string(),
    driverName: z.string().optional().default(''),
    driverRating: z.number().optional().default(5),
    vehicle: z.string().optional().default(''),
    plate: z.string().optional().default(''),
    distanceKm: z.number().optional().default(0),
    offerFareNPR: z.number(),
    expiresAt: z.number(),
    issuedAt: z.number(),
  }),
});

// Rider explicitly rejected a driver's proposed counter — drop the offer
// card on the driver's UI without waiting for natural expiry.
const offerRejectedSchema = z.object({
  type: z.literal('trip.offer_rejected'),
  payload: z.object({
    tripId: z.string(),
    offerId: z.string(),
    driverId: z.string(),
    riderId: z.string(),
    ts: z.number(),
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
  if (typeof raw === 'object' && raw !== null) {
    const type = (raw as { type?: string }).type;
    if (type === 'trip.bargain_offer') {
      handleBargainOffer(raw, riderNsp);
      return;
    }
    if (type === 'trip.counter_back') {
      handleCounterBack(raw, driverNsp);
      return;
    }
    if (type === 'trip.offer_rejected') {
      handleOfferRejected(raw, driverNsp);
      return;
    }
  }

  const result = tripEventSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.events message validation failed');
    return;
  }

  const event = result.data as KafkaTripEvent;
  const { tripId, riderId, driverId, driverName, status, reason } = event.payload;
  const ts = event.payload.ts ?? Date.now();
  // Carry the upstream eventId if trip-svc supplied one; otherwise mint a
  // local one so every emit has a traceable id for debugging.
  const upstreamEventId =
    typeof (event.payload as { eventId?: unknown }).eventId === 'string'
      ? ((event.payload as unknown as { eventId: string }).eventId)
      : undefined;
  const eventId = upstreamEventId ?? randomUUID();

  // Emit trip:status to all riders in the trip room. `reason` rides along on
  // cancellations so the rider app can show a specific message (e.g. no drivers).
  riderNsp.to(`trip:${tripId}`).emit('trip:status', {
    tripId,
    status,
    driverId,
    driverName,
    ...(reason ? { reason } : {}),
    ts,
    meta: { eventId },
  });

  // Rider↔driver contact closes the moment the ride starts. Mark it server-side
  // so chat:send is rejected from here on, even across a client reconnect.
  if (status === 'IN_PROGRESS') {
    void closeChatForTrip(tripId);
  }

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

  // ── Status-transition + notification fan-out ─────────────────────────────
  // The lifecycle status update above already broadcast `trip:status` to
  // every socket in the trip room. We additionally emit a per-side explicit
  // event for actions the UI binds to directly — easier than every screen
  // having to parse the generic status enum. Targeted to the side that
  // *receives* the notification (driver-arrived → rider's UI, rider-
  // approaching → driver's UI, etc.).
  if (event.type === 'trip.driver_arrived') {
    riderNsp.to(`trip:${tripId}`).emit('trip:driver_arrived', {
      tripId, driverId, ts, meta: { eventId },
    });
  } else if (event.type === 'trip.rider_approaching' && driverId) {
    driverNsp.to(`driver:${driverId}`).emit('trip:rider_approaching', {
      tripId, riderId, ts, meta: { eventId },
    });
    driverNsp.to(`trip:${tripId}`).emit('trip:rider_approaching', {
      tripId, riderId, ts, meta: { eventId },
    });
  } else if (event.type === 'trip.started') {
    riderNsp.to(`trip:${tripId}`).emit('trip:started', { tripId, driverId, ts, meta: { eventId } });
    if (driverId) driverNsp.to(`driver:${driverId}`).emit('trip:started', { tripId, driverId, ts, meta: { eventId } });
  } else if (event.type === 'trip.completed') {
    riderNsp.to(`trip:${tripId}`).emit('trip:completed', { tripId, driverId, ts, meta: { eventId } });
    if (driverId) driverNsp.to(`driver:${driverId}`).emit('trip:completed', { tripId, driverId, ts, meta: { eventId } });
  } else if (event.type === 'trip.fare_bumped') {
    // Rider already gets the HTTP response synchronously; the broadcast is
    // mostly for visibility into the trip room (admin debug panels, future
    // co-rider devices).
    riderNsp.to(`trip:${tripId}`).emit('trip:fare_bumped', { tripId, ts, meta: { eventId } });
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

// Rider's counter-back arrives at the driver as a fresh `trip:counter_back`
// event carrying a new offerId. The driver's UI shows it and the driver
// responds via the existing /offers/:id/respond endpoint.
function handleCounterBack(raw: unknown, driverNsp: Namespace): void {
  const result = counterBackSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.counter_back validation failed');
    return;
  }
  const { driverId, offerId, tripId } = result.data.payload;
  driverNsp.to(`driver:${driverId}`).emit('trip:counter_back', {
    ...result.data.payload,
    meta: { eventId: offerId },
  });
  logger.debug({ tripId, offerId, driverId }, 'Forwarded counter_back to driver');
}

// Rider explicitly rejected the driver's counter. Tell the driver so the
// offer card can be removed.
function handleOfferRejected(raw: unknown, driverNsp: Namespace): void {
  const result = offerRejectedSchema.safeParse(raw);
  if (!result.success) {
    logger.warn({ errors: result.error.flatten() }, 'trip.offer_rejected validation failed');
    return;
  }
  const { driverId, offerId, tripId } = result.data.payload;
  driverNsp.to(`driver:${driverId}`).emit('trip:offer_rejected', {
    ...result.data.payload,
    meta: { eventId: offerId },
  });
  logger.debug({ tripId, offerId, driverId }, 'Forwarded offer_rejected to driver');
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
      clearChatWindow(tripId),
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
