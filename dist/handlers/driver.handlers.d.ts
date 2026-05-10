import type { Namespace, Socket } from 'socket.io';
interface DriverSocketData {
    driverId: string;
    activeTripId?: string;
    /** Cached from `driver:online` so per-heartbeat Kafka publishes can carry
     *  the vehicleType without an extra Redis HGET on the hot path. Without
     *  this, location-svc's consumer overwrites `drivers:meta:` with empty
     *  vehicleType (its zero value) and matching's filter never finds the
     *  driver. */
    vehicleType?: string;
}
export declare function registerDriverHandlers(socket: Socket<Record<string, never>, Record<string, never>, Record<string, never>, DriverSocketData>, driverNsp: Namespace, riderNsp: Namespace): void;
export {};
//# sourceMappingURL=driver.handlers.d.ts.map