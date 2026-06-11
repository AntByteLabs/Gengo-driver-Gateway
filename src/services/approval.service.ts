import { getRedisDataClient } from '../infrastructure/redis.js';
import { config } from '../config.js';
import { logger } from '../logger.js';

// ─── Key constants ────────────────────────────────────────────────────────────

// Short TTL so admin approval/suspension propagates within minutes while the
// gateway never hammers driver-svc on every reconnect.
const APPROVAL_CACHE_TTL_SEC = 300;
const HTTP_TIMEOUT_MS = 5_000;

/** Sentinel cached when driver-svc says the user has no driver profile. */
const NOT_REGISTERED = 'NOT_REGISTERED';

function approvalKey(driverId: string): string {
  return `drivers:approval:${driverId}`;
}

interface DriverStatusEnvelope {
  success?: boolean;
  data?: { approvalStatus?: unknown };
}

/**
 * Resolves the driver's KYC approval status ('PENDING' | 'APPROVED' |
 * 'SUSPENDED' | 'NEEDS_RESUBMISSION' | 'NOT_REGISTERED'), cached in Redis for
 * APPROVAL_CACHE_TTL_SEC. The lookup calls driver-svc's existing
 * `GET /v1/driver/status` endpoint with the driver's own Authorization header
 * (the same `Bearer <jwt>` string the socket presented at handshake), so no
 * new public endpoint or service credential is needed.
 *
 * Only definitive answers are cached. Transport/5xx errors return null so the
 * caller can fail closed without poisoning the cache.
 */
export async function getDriverApprovalStatus(
  driverId: string,
  authorization: string,
): Promise<string | null> {
  const redis = getRedisDataClient();

  try {
    const cached = await redis.get(approvalKey(driverId));
    if (cached) return cached;
  } catch (err) {
    logger.warn({ err, driverId }, 'Approval cache read failed');
  }

  let status: string | null = null;
  try {
    const res = await fetch(`${config.DRIVER_SVC_URL}/v1/driver/status`, {
      headers: { Authorization: authorization },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    });

    if (res.status === 404) {
      // No driver profile exists for this user — definitively not approved.
      status = NOT_REGISTERED;
    } else if (res.ok) {
      const body = (await res.json()) as DriverStatusEnvelope;
      if (typeof body?.data?.approvalStatus === 'string') {
        status = body.data.approvalStatus;
      }
    } else {
      logger.warn(
        { driverId, httpStatus: res.status },
        'driver-svc approval lookup returned non-OK status',
      );
    }
  } catch (err) {
    logger.warn({ err, driverId }, 'driver-svc approval lookup failed');
  }

  if (status) {
    await redis
      .set(approvalKey(driverId), status, 'EX', APPROVAL_CACHE_TTL_SEC)
      .catch((err: unknown) => logger.warn({ err, driverId }, 'Approval cache write failed'));
  }

  return status;
}
