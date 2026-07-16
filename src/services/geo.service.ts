import { getRedisDataClient } from '../infrastructure/redis.js';
import { logger } from '../logger.js';

// ─── Key constants ────────────────────────────────────────────────────────────

const GEO_KEY = 'drivers:available:geo';

function metaKey(driverId: string): string {
  return `drivers:meta:${driverId}`;
}

function activeTripDriverKey(driverId: string): string {
  return `drivers:active:trip:${driverId}`;
}

function activeTripRiderKey(riderId: string): string {
  return `riders:active:trip:${riderId}`;
}

// ─── Geo operations ───────────────────────────────────────────────────────────

export async function geoRemoveDriver(driverId: string): Promise<void> {
  const redis = getRedisDataClient();
  await redis.zrem(GEO_KEY, driverId);
}

// ─── Driver metadata ──────────────────────────────────────────────────────────

/**
 * Register a driver into the AVAILABLE pool: write the meta hash (which stamps
 * lastSeen) AND add them to the geo set, as ONE MULTI so the two land together.
 *
 * The ordering and atomicity both matter. location-svc's SweepStale evicts a
 * geo entry that has no lastSeen as an orphan; issuing the HSET and GEOADD as
 * two separate round-trips left a window where a just-onlined driver sat in the
 * geo set without meta and could be reaped mid-registration. A single MULTI
 * closes that window entirely — the sweep, on its own connection, cannot
 * interleave between the queued commands — and saves a round-trip on every
 * (re)connect. Mirrors location-svc's UpsertAvailable (one pipelined GeoAdd +
 * HSet). HSET is queued first purely for readability; MULTI applies them as a
 * unit regardless.
 */
export async function registerAvailableDriver(
  driverId: string,
  lat: number,
  lng: number,
  vehicleType: string,
  socketId: string,
  /** Optional preferred-route corridor "oLat,oLng;dLat,dLng". When the driver
   *  picks a route at go-online, trip-svc's matcher only offers them rides
   *  whose pickup falls within the configured corridor. Empty/absent clears
   *  any route from a prior session so it doesn't silently constrain them. */
  route?: string,
): Promise<void> {
  const redis = getRedisDataClient();
  await redis
    .multi()
    .hset(metaKey(driverId), {
      vehicleType,
      status: 'available',
      socketId,
      lastSeen: Date.now().toString(),
      route: route ?? '',
    })
    .geoadd(GEO_KEY, lng, lat, driverId)
    .exec();
}

export async function setDriverOffline(driverId: string): Promise<void> {
  const redis = getRedisDataClient();
  await redis.hset(metaKey(driverId), {
    status: 'offline',
    lastSeen: Date.now().toString(),
  });
}

export async function updateDriverLastSeen(driverId: string): Promise<void> {
  const redis = getRedisDataClient();
  try {
    await redis.hset(metaKey(driverId), 'lastSeen', Date.now().toString());
  } catch (err) {
    logger.warn({ err, driverId }, 'Failed to update driver lastSeen');
  }
}

export async function setDriverStatus(
  driverId: string,
  status: 'available' | 'busy' | 'offline',
): Promise<void> {
  const redis = getRedisDataClient();
  await redis.hset(metaKey(driverId), 'status', status);
}

export async function getDriverMeta(
  driverId: string,
): Promise<Record<string, string>> {
  const redis = getRedisDataClient();
  return redis.hgetall(metaKey(driverId));
}

/**
 * Returns the socketId currently registered as the driver's live connection,
 * or null if none is recorded. Used by the disconnect handler to avoid evicting
 * a driver from the geo index when a *stale* socket disconnects after a newer
 * one has already taken over (mobile-network reconnect race).
 */
export async function getDriverSocketId(
  driverId: string,
): Promise<string | null> {
  const redis = getRedisDataClient();
  return redis.hget(metaKey(driverId), 'socketId');
}

// ─── Active trip references ───────────────────────────────────────────────────

export async function getDriverActiveTrip(driverId: string): Promise<string | null> {
  const redis = getRedisDataClient();
  return redis.get(activeTripDriverKey(driverId));
}

export async function setDriverActiveTrip(
  driverId: string,
  tripId: string,
): Promise<void> {
  const redis = getRedisDataClient();
  await redis.set(activeTripDriverKey(driverId), tripId);
}

export async function clearDriverActiveTrip(driverId: string): Promise<void> {
  const redis = getRedisDataClient();
  await redis.del(activeTripDriverKey(driverId));
}

export async function getRiderActiveTrip(riderId: string): Promise<string | null> {
  const redis = getRedisDataClient();
  return redis.get(activeTripRiderKey(riderId));
}

export async function clearRiderActiveTrip(riderId: string): Promise<void> {
  const redis = getRedisDataClient();
  await redis.del(activeTripRiderKey(riderId));
}
