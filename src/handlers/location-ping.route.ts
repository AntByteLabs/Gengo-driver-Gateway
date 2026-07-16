import type { Request, Response } from 'express';
import type { Namespace } from 'socket.io';
import jwt from 'jsonwebtoken';
import { z } from 'zod';
import { config } from '../config.js';
import { logger } from '../logger.js';
import type { JwtPayload } from '../domain/types.js';
import { getDriverActiveTrip, getDriverMeta } from '../services/geo.service.js';
import { ingestDriverLocation } from '../services/location-ingest.js';

const pingSchema = z.object({
  lat: z.number().finite(),
  lng: z.number().finite(),
  heading: z.number().finite().optional(),
});

/**
 * HTTP fallback for a driver's live position when the socket can't run — i.e.
 * the app is backgrounded / screen-locked DURING a trip and the JS runtime that
 * owns the socket is suspended. The app's background-location task (which the OS
 * keeps alive via the foreground service) POSTs fixes here instead.
 *
 * It deliberately does NOT trust a client-supplied trip id: the active trip is
 * read from Redis (drivers:active:trip:<driverId>), the same server-authoritative
 * signal the socket path uses, so a driver can only ever push into their own
 * trip room. A ping with no active trip is a no-op (204) — background tracking is
 * only meaningful on a trip.
 *
 * Auth is the same JWT as the socket handshake. Role is informational per
 * CLAUDE.md §5.4 — the endpoint's own trip lookup is the real boundary.
 */
export function createLocationPingRoute(riderNsp: Namespace) {
  return async (req: Request, res: Response): Promise<void> => {
    const raw = req.headers.authorization;
    if (typeof raw !== 'string' || !raw.startsWith('Bearer ')) {
      res.status(401).json({ error: 'AUTH_MISSING_TOKEN' });
      return;
    }

    let driverId: string;
    try {
      const decoded = jwt.verify(raw.slice(7).trim(), config.JWT_SECRET) as JwtPayload;
      if (!decoded.sub) {
        res.status(401).json({ error: 'AUTH_INVALID_CLAIMS' });
        return;
      }
      driverId = decoded.sub;
    } catch {
      res.status(401).json({ error: 'AUTH_INVALID_TOKEN' });
      return;
    }

    const parsed = pingSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: 'VALIDATION_FAILED' });
      return;
    }

    const activeTripId = await getDriverActiveTrip(driverId);
    if (!activeTripId) {
      // Not on a trip — nothing to forward. Succeed quietly so the client task
      // doesn't retry-storm; it will stop itself when the trip ends anyway.
      res.status(204).send();
      return;
    }

    // Preserve the driver's registered vehicleType so location-svc's meta write
    // doesn't blank it (same failure mode guarded on the socket path).
    const meta = await getDriverMeta(driverId);
    const { lat, lng, heading } = parsed.data;
    ingestDriverLocation(riderNsp, {
      driverId,
      activeTripId,
      lat,
      lng,
      heading,
      vehicleType: meta.vehicleType || undefined,
      status: 'on_trip',
    });

    logger.debug({ driverId, activeTripId }, 'background location ping forwarded');
    res.status(204).send();
  };
}
