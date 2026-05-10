import { getDriverActiveTrip, getRiderActiveTrip } from './geo.service.js';
import { logger } from '../logger.js';
/**
 * Looks up whether a driver has an active trip in Redis.
 * Returns the tripId string or null.
 */
export async function resolveDriverActiveTrip(driverId) {
    try {
        return await getDriverActiveTrip(driverId);
    }
    catch (err) {
        logger.warn({ err, driverId }, 'Failed to resolve driver active trip');
        return null;
    }
}
/**
 * Looks up whether a rider has an active trip in Redis.
 * Returns the tripId string or null.
 */
export async function resolveRiderActiveTrip(riderId) {
    try {
        return await getRiderActiveTrip(riderId);
    }
    catch (err) {
        logger.warn({ err, riderId }, 'Failed to resolve rider active trip');
        return null;
    }
}
//# sourceMappingURL=session.service.js.map