import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import { ackFail, ackOk } from './chat-ack.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { geoAddDriver, geoRemoveDriver, getDriverSocketId, setDriverOnline, setDriverOffline, updateDriverLastSeen, } from '../services/geo.service.js';
import { sendKafkaMessage } from '../infrastructure/kafka.js';
// ─── Per-socket location-update throttle ─────────────────────────────────────
// Token-bucket: 2 updates/sec sustained, burst of 4. A buggy or malicious
// client streaming `driver:location` at 100Hz used to fan out to Kafka,
// Redis GEOADD, and a rider-room broadcast on every event — this caps the
// pressure regardless of client behaviour. Per-socket state is fine because
// throttling is a "this physical connection is misbehaving" concern, not a
// global authorisation one.
const LOCATION_BUCKET_RATE_PER_SEC = 2;
const LOCATION_BUCKET_BURST = 4;
function takeToken(bucket) {
    const now = Date.now();
    const elapsedSec = (now - bucket.lastRefillMs) / 1000;
    if (elapsedSec > 0) {
        bucket.tokens = Math.min(LOCATION_BUCKET_BURST, bucket.tokens + elapsedSec * LOCATION_BUCKET_RATE_PER_SEC);
        bucket.lastRefillMs = now;
    }
    if (bucket.tokens >= 1) {
        bucket.tokens -= 1;
        return true;
    }
    return false;
}
// ─── Zod schemas for incoming payloads ───────────────────────────────────────
const driverOnlineSchema = z.object({
    vehicleType: z.string().min(1),
    // lat/lng are required: we used to GEOADD a (0,0) placeholder at online
    // time, which left a stale entry off the coast of Africa until the first
    // heartbeat arrived. Now `driver:online` is the first real position.
    lat: z.number().finite().min(-90).max(90),
    lng: z.number().finite().min(-180).max(180),
});
const driverLocationSchema = z.object({
    lat: z.number().finite().min(-90).max(90),
    lng: z.number().finite().min(-180).max(180),
    heading: z.number().finite().optional(),
});
const chatSendSchema = z.object({
    tripId: z.string().min(1),
    text: z.string().min(1).max(2000),
});
// ─── Handler registration ─────────────────────────────────────────────────────
export function registerDriverHandlers(socket, driverNsp, riderNsp) {
    const driverId = socket.data.driverId;
    // Per-socket throttle state. Lives on the closure (not `socket.data`) so
    // it isn't visible to handlers that should not be able to refill it.
    const locationBucket = {
        tokens: LOCATION_BUCKET_BURST,
        lastRefillMs: Date.now(),
    };
    // ── driver:online ─────────────────────────────────────────────────────────
    socket.on('driver:online', async (raw) => {
        const result = driverOnlineSchema.safeParse(raw);
        if (!result.success) {
            logger.warn({ driverId, errors: result.error.flatten() }, 'driver:online validation failed');
            return;
        }
        const { vehicleType, lat, lng } = result.data;
        // Cache vehicleType immediately so any concurrent driver:location
        // event picks it up. The Redis/Kafka writes below are idempotent; the
        // failure modes are independent.
        socket.data.vehicleType = vehicleType;
        // Side-effects: each gets its own try/catch so one failure does not
        // poison the others (e.g. Kafka being briefly slow must not prevent
        // the geo entry being created).
        try {
            await geoAddDriver(driverId, lat, lng);
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:online geoAddDriver failed');
        }
        try {
            await setDriverOnline(driverId, vehicleType, socket.id);
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:online setDriverOnline failed');
        }
        try {
            await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
                driverId,
                vehicleType,
                lat,
                lng,
                status: 'available',
                ts: Date.now(),
                meta: { eventId: randomUUID() },
            });
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:online Kafka publish failed');
        }
        logger.info({ driverId, vehicleType }, 'Driver came online');
    });
    // ── driver:location ───────────────────────────────────────────────────────
    socket.on('driver:location', async (raw) => {
        // Per-socket token-bucket throttle: drop excess updates BEFORE doing
        // any work. Logged at debug only to avoid flooding the log when a
        // misbehaving client is the very thing we're protecting against.
        if (!takeToken(locationBucket)) {
            logger.debug({ driverId, socketId: socket.id }, 'driver:location throttled');
            return;
        }
        const result = driverLocationSchema.safeParse(raw);
        if (!result.success) {
            logger.warn({ driverId, errors: result.error.flatten() }, 'driver:location validation failed');
            return;
        }
        const { lat, lng, heading } = result.data;
        const ts = Date.now();
        const eventId = randomUUID();
        // Broadcast to the rider's trip room FIRST. Riders should see fresh
        // driver position even if Kafka or Redis is briefly slow — the broadcast
        // is the only thing the rider's UI is waiting on. Fan-out to Kafka and
        // Redis is best-effort downstream-state propagation, not on the
        // user-visible critical path.
        const activeTripId = socket.data.activeTripId;
        if (activeTripId) {
            riderNsp.to(`trip:${activeTripId}`).emit('driver:location', {
                tripId: activeTripId,
                lat,
                lng,
                ...(heading !== undefined && { heading }),
                ts,
                meta: { eventId },
            });
        }
        // Each side-effect runs in its own try/catch so one failure cannot
        // skip another. `await` them in series to keep ordering predictable
        // for the same driverId; the throttle above caps total throughput.
        try {
            await geoAddDriver(driverId, lat, lng);
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:location geoAddDriver failed');
        }
        try {
            await updateDriverLastSeen(driverId);
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:location updateDriverLastSeen failed');
        }
        try {
            await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
                driverId,
                ...(socket.data.vehicleType ? { vehicleType: socket.data.vehicleType } : {}),
                lat,
                lng,
                ...(heading !== undefined && { heading }),
                status: 'available',
                ts,
                meta: { eventId },
            });
        }
        catch (err) {
            logger.error({ err, driverId }, 'driver:location Kafka publish failed');
        }
    });
    // ── chat:send ────────────────────────────────────────────────────────────
    // Wire format negotiated with the driver app: incoming `chat:send`,
    // outgoing `trip:update` with `eventType: 'CHAT'`.
    socket.on('chat:send', async (raw, ack) => {
        const result = chatSendSchema.safeParse(raw);
        if (!result.success) {
            logger.warn({ driverId, errors: result.error.flatten() }, 'chat:send validation failed');
            ackFail(ack, 'invalid_payload');
            return;
        }
        const { tripId, text } = result.data;
        // The driver may only chat in the trip we paired them with. The
        // trip-events consumer clears activeTripId the moment the ride
        // completes/cancels — so chat dies with the ride.
        const activeTripId = socket.data.activeTripId;
        if (!activeTripId || tripId !== activeTripId) {
            logger.warn({ driverId, payloadTripId: tripId, activeTripId }, 'chat:send dropped — driver is not paired with that trip');
            ackFail(ack, 'not_paired');
            return;
        }
        const ts = Date.now();
        const id = `m-${ts}-${driverId.slice(-6)}`;
        const envelope = {
            tripId,
            eventType: 'CHAT',
            payload: { id, from: 'driver', text },
            ts,
            meta: { eventId: randomUUID() },
        };
        riderNsp.to(`trip:${tripId}`).emit('trip:update', envelope);
        socket.to(`trip:${tripId}`).emit('trip:update', envelope);
        ackOk(ack);
    });
    // ── disconnect ────────────────────────────────────────────────────────────
    socket.on('disconnect', async (reason) => {
        logger.info({ driverId, socketId: socket.id, reason }, 'Driver disconnected');
        try {
            // Reconnect race: a flapping mobile network can produce a new socket
            // (B) for the same driver before the old socket (A) finishes
            // disconnecting. Without this check, A's late `disconnect` would evict
            // a currently-online driver from drivers:available:geo and mark them
            // offline, even though B is healthy. Only run the offline path when
            // the meta still points at THIS socket; otherwise we are the stale
            // one and the active connection has already taken over.
            const currentSocketId = await getDriverSocketId(driverId);
            if (currentSocketId !== null && currentSocketId !== socket.id) {
                logger.info({ driverId, staleSocketId: socket.id, currentSocketId }, 'Stale socket disconnect — leaving driver online');
                return;
            }
            // If the driver never sent driver:online there is no meta to clear and
            // no geo entry to remove. Skip the offline write so we don't leave a
            // half-populated drivers:meta:<id> hash behind.
            if (currentSocketId === null && socket.data.vehicleType === undefined) {
                return;
            }
            await geoRemoveDriver(driverId);
            await setDriverOffline(driverId);
            await sendKafkaMessage(config.KAFKA_TOPIC_DRIVER_LOCATION, driverId, {
                driverId,
                lat: 0,
                lng: 0,
                status: 'offline',
                ts: Date.now(),
                meta: { eventId: randomUUID() },
            });
        }
        catch (err) {
            logger.error({ err, driverId }, 'Error handling driver disconnect');
        }
    });
}
//# sourceMappingURL=driver.handlers.js.map