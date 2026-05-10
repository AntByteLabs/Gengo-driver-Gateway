export interface JwtPayload {
    sub: string;
    role: 'driver' | 'rider';
    iat?: number;
    exp?: number;
}
export interface TripOffer {
    offerId: string;
    tripId: string;
    riderId: string;
    riderName: string;
    pickup: {
        lat: number;
        lng: number;
        address: string;
    };
    dropoff: {
        lat: number;
        lng: number;
        address: string;
    };
    distanceKm: number;
    estimatedFareNPR: number;
    paymentMethod: string;
    vehicleType: string;
    expiresInSec: number;
    issuedAt: number;
}
export type TripStatus = 'PENDING' | 'ACCEPTED' | 'DRIVER_ARRIVED' | 'IN_PROGRESS' | 'COMPLETED' | 'CANCELLED';
export interface KafkaTripEvent {
    type: 'TRIP_STATUS_CHANGED' | 'TRIP_CANCELLED' | string;
    payload: {
        tripId: string;
        riderId: string;
        driverId?: string;
        driverName?: string;
        status: TripStatus;
        ts: number;
        [key: string]: unknown;
    };
}
export interface KafkaNotificationOutboxEvent {
    type: 'app:config:bumped' | string;
    payload: {
        version?: number;
        [key: string]: unknown;
    };
}
export interface KafkaDriverLocationEvent {
    driverId: string;
    lat: number;
    lng: number;
    heading?: number;
    status: 'available' | 'busy' | 'offline';
    tripId?: string;
    ts: number;
}
export type RedisTripOfferMessage = TripOffer;
//# sourceMappingURL=types.d.ts.map