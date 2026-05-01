import { z } from 'zod';
import type { Namespace } from 'socket.io';
import { getRedisPubsubClient } from '../infrastructure/pubsub.js';
import { logger } from '../logger.js';
import type { RedisTripOfferMessage } from '../domain/types.js';

// ─── Zod schema ───────────────────────────────────────────────────────────────

const tripOfferSchema = z.object({
  offerId: z.string(),
  tripId: z.string(),
  riderId: z.string(),
  riderName: z.string(),
  pickup: z.object({
    lat: z.number(),
    lng: z.number(),
    address: z.string(),
  }),
  dropoff: z.object({
    lat: z.number(),
    lng: z.number(),
    address: z.string(),
  }),
  distanceKm: z.number(),
  estimatedFareNPR: z.number(),
  paymentMethod: z.string(),
  vehicleType: z.string(),
  expiresInSec: z.number(),
  issuedAt: z.number(),
});

// ─── Pattern matching ─────────────────────────────────────────────────────────

// Channels follow the pattern: driver:{driverId}:offer
const OFFER_CHANNEL_PATTERN = 'driver:*:offer';
const OFFER_CHANNEL_REGEX = /^driver:([^:]+):offer$/;

// ─── Consumer bootstrap ───────────────────────────────────────────────────────

export async function startOfferConsumer(driverNsp: Namespace): Promise<void> {
  const pubsub = getRedisPubsubClient();

  await pubsub.psubscribe(OFFER_CHANNEL_PATTERN);

  pubsub.on('pmessage', (_pattern: string, channel: string, message: string) => {
    const match = OFFER_CHANNEL_REGEX.exec(channel);
    if (!match) return;

    const driverId = match[1];

    let parsed: unknown;
    try {
      parsed = JSON.parse(message);
    } catch {
      logger.warn({ channel }, 'Failed to parse Redis pubsub offer message as JSON');
      return;
    }

    const result = tripOfferSchema.safeParse(parsed);
    if (!result.success) {
      logger.warn(
        { channel, errors: result.error.flatten() },
        'Trip offer message validation failed',
      );
      return;
    }

    const offer = result.data as RedisTripOfferMessage;

    // Forward to the specific driver socket room
    driverNsp.to(`driver:${driverId}`).emit('trip:offer', offer);

    logger.debug({ driverId, offerId: offer.offerId }, 'Forwarded trip offer to driver');
  });

  logger.info({ pattern: OFFER_CHANNEL_PATTERN }, 'Redis offer consumer subscribed');
}
