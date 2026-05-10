export declare function geoAddDriver(driverId: string, lat: number, lng: number): Promise<void>;
export declare function geoRemoveDriver(driverId: string): Promise<void>;
export declare function setDriverOnline(driverId: string, vehicleType: string, socketId: string): Promise<void>;
export declare function setDriverOffline(driverId: string): Promise<void>;
export declare function updateDriverLastSeen(driverId: string): Promise<void>;
export declare function setDriverStatus(driverId: string, status: 'available' | 'busy' | 'offline'): Promise<void>;
export declare function getDriverMeta(driverId: string): Promise<Record<string, string>>;
/**
 * Returns the socketId currently registered as the driver's live connection,
 * or null if none is recorded. Used by the disconnect handler to avoid evicting
 * a driver from the geo index when a *stale* socket disconnects after a newer
 * one has already taken over (mobile-network reconnect race).
 */
export declare function getDriverSocketId(driverId: string): Promise<string | null>;
export declare function getDriverActiveTrip(driverId: string): Promise<string | null>;
export declare function setDriverActiveTrip(driverId: string, tripId: string): Promise<void>;
export declare function clearDriverActiveTrip(driverId: string): Promise<void>;
export declare function getRiderActiveTrip(riderId: string): Promise<string | null>;
export declare function clearRiderActiveTrip(riderId: string): Promise<void>;
//# sourceMappingURL=geo.service.d.ts.map