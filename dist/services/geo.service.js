import { getRedisDataClient } from '../infrastructure/redis.js';
import { logger } from '../logger.js';
// ─── Key constants ────────────────────────────────────────────────────────────
const GEO_KEY = 'drivers:available:geo';
function metaKey(driverId) {
    return `drivers:meta:${driverId}`;
}
function activeTripDriverKey(driverId) {
    return `drivers:active:trip:${driverId}`;
}
function activeTripRiderKey(riderId) {
    return `riders:active:trip:${riderId}`;
}
// ─── Geo operations ───────────────────────────────────────────────────────────
export async function geoAddDriver(driverId, lat, lng) {
    const redis = getRedisDataClient();
    await redis.geoadd(GEO_KEY, lng, lat, driverId);
}
export async function geoRemoveDriver(driverId) {
    const redis = getRedisDataClient();
    await redis.zrem(GEO_KEY, driverId);
}
// ─── Driver metadata ──────────────────────────────────────────────────────────
export async function setDriverOnline(driverId, vehicleType, socketId) {
    const redis = getRedisDataClient();
    await redis.hset(metaKey(driverId), {
        vehicleType,
        status: 'available',
        socketId,
        lastSeen: Date.now().toString(),
    });
}
export async function setDriverOffline(driverId) {
    const redis = getRedisDataClient();
    await redis.hset(metaKey(driverId), {
        status: 'offline',
        lastSeen: Date.now().toString(),
    });
}
export async function updateDriverLastSeen(driverId) {
    const redis = getRedisDataClient();
    try {
        await redis.hset(metaKey(driverId), 'lastSeen', Date.now().toString());
    }
    catch (err) {
        logger.warn({ err, driverId }, 'Failed to update driver lastSeen');
    }
}
export async function setDriverStatus(driverId, status) {
    const redis = getRedisDataClient();
    await redis.hset(metaKey(driverId), 'status', status);
}
export async function getDriverMeta(driverId) {
    const redis = getRedisDataClient();
    return redis.hgetall(metaKey(driverId));
}
/**
 * Returns the socketId currently registered as the driver's live connection,
 * or null if none is recorded. Used by the disconnect handler to avoid evicting
 * a driver from the geo index when a *stale* socket disconnects after a newer
 * one has already taken over (mobile-network reconnect race).
 */
export async function getDriverSocketId(driverId) {
    const redis = getRedisDataClient();
    return redis.hget(metaKey(driverId), 'socketId');
}
// ─── Active trip references ───────────────────────────────────────────────────
export async function getDriverActiveTrip(driverId) {
    const redis = getRedisDataClient();
    return redis.get(activeTripDriverKey(driverId));
}
export async function setDriverActiveTrip(driverId, tripId) {
    const redis = getRedisDataClient();
    await redis.set(activeTripDriverKey(driverId), tripId);
}
export async function clearDriverActiveTrip(driverId) {
    const redis = getRedisDataClient();
    await redis.del(activeTripDriverKey(driverId));
}
export async function getRiderActiveTrip(riderId) {
    const redis = getRedisDataClient();
    return redis.get(activeTripRiderKey(riderId));
}
export async function clearRiderActiveTrip(riderId) {
    const redis = getRedisDataClient();
    await redis.del(activeTripRiderKey(riderId));
}
//# sourceMappingURL=geo.service.js.map