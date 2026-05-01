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

export async function geoAddDriver(
  driverId: string,
  lat: number,
  lng: number,
): Promise<void> {
  const redis = getRedisDataClient();
  await redis.geoadd(GEO_KEY, lng, lat, driverId);
}

export async function geoRemoveDriver(driverId: string): Promise<void> {
  const redis = getRedisDataClient();
  await redis.zrem(GEO_KEY, driverId);
}

// ─── Driver metadata ──────────────────────────────────────────────────────────

export async function setDriverOnline(
  driverId: string,
  vehicleType: string,
  socketId: string,
): Promise<void> {
  const redis = getRedisDataClient();
  await redis.hset(metaKey(driverId), {
    vehicleType,
    status: 'available',
    socketId,
    lastSeen: Date.now().toString(),
  });
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
