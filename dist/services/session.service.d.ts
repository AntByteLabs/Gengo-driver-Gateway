/**
 * Looks up whether a driver has an active trip in Redis.
 * Returns the tripId string or null.
 */
export declare function resolveDriverActiveTrip(driverId: string): Promise<string | null>;
/**
 * Looks up whether a rider has an active trip in Redis.
 * Returns the tripId string or null.
 */
export declare function resolveRiderActiveTrip(riderId: string): Promise<string | null>;
//# sourceMappingURL=session.service.d.ts.map