// ─── JWT payload ─────────────────────────────────────────────────────────────

export interface JwtPayload {
  sub: string;       // driverId or riderId
  role: 'driver' | 'rider';
  iat?: number;
  exp?: number;
}

// ─── Trip Offer ──────────────────────────────────────────────────────────────

export interface TripOffer {
  offerId: string;
  tripId: string;
  riderId: string;
  riderName: string;
  riderPhone?: string;
  riderRating?: number;
  pickup: { lat: number; lng: number; address: string };
  dropoff: { lat: number; lng: number; address: string };
  distanceKm: number;
  estimatedFareNPR: number;
  /** Fair-price classification (P5.2) of the rider's offered fare against the
   *  fair estimate. Optional — absent on older trip-svc builds. */
  fareClass?: 'low' | 'fair' | 'high';
  typicalMinNPR?: number;
  typicalMaxNPR?: number;
  paymentMethod: string;
  vehicleType: string;
  expiresInSec: number;
  issuedAt: number;
  /** True when the trip originated from the admin manual-booking flow.
   *  Driver apps must hide the counter-offer UI for these offers — they
   *  are accept-at-fare only, no negotiation. */
  adminBooked?: boolean;
}

// ─── Kafka trip event shapes ──────────────────────────────────────────────────

export type TripStatus =
  | 'PENDING'
  | 'ACCEPTED'
  | 'DRIVER_ARRIVED'
  | 'IN_PROGRESS'
  | 'COMPLETED'
  | 'CANCELLED';

export interface KafkaTripEvent {
  type: 'TRIP_STATUS_CHANGED' | 'TRIP_CANCELLED' | string;
  payload: {
    tripId: string;
    riderId: string;
    driverId?: string;
    driverName?: string;
    status: TripStatus;
    /** Cancellation cause (e.g. 'no_drivers_available') forwarded to the rider. */
    reason?: string;
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

// ─── Redis pub/sub offer message ──────────────────────────────────────────────

export type RedisTripOfferMessage = TripOffer;
