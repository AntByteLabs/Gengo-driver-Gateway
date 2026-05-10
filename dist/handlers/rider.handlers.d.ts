import type { Namespace, Socket } from 'socket.io';
interface RiderSocketData {
    riderId: string;
    /** Set in the namespace connect handler from Redis. The chat handler
     *  refuses to relay into any other trip room, and the trip-events
     *  consumer clears it the instant the trip ends so chat stops working. */
    activeTripId?: string;
}
export declare function registerRiderHandlers(socket: Socket<Record<string, never>, Record<string, never>, Record<string, never>, RiderSocketData>, driverNsp: Namespace): void;
export {};
//# sourceMappingURL=rider.handlers.d.ts.map