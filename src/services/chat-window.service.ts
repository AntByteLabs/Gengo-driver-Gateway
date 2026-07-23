import { getRedisDataClient } from '../infrastructure/redis.js';

// Rider ↔ driver contact (chat AND call) is allowed only from ride-confirmed up
// to the moment the driver STARTS the ride (the trip enters IN_PROGRESS). Once
// the rider is in the vehicle there is no need to message or call, so the
// channel closes. This flag enforces that boundary SERVER-SIDE: the UIs hide the
// buttons, but a reconnecting or misbehaving client must not be able to reopen
// the channel, and on reconnect socket.data pairing is re-read from Redis (still
// valid through IN_PROGRESS), so the pairing check alone would let chat back in.
//
// The flag is set when the trip starts and auto-expires well after any trip ends;
// it is also cleared on trip teardown.
const CHAT_CLOSED_TTL_SEC = 6 * 60 * 60;

function chatClosedKey(tripId: string): string {
  return `chat:closed:${tripId}`;
}

/** Close the rider↔driver channel for a trip — called when the ride starts. */
export async function closeChatForTrip(tripId: string): Promise<void> {
  await getRedisDataClient().set(chatClosedKey(tripId), '1', 'EX', CHAT_CLOSED_TTL_SEC);
}

/** True once the ride has started; chat:send / call must be rejected. */
export async function isChatClosed(tripId: string): Promise<boolean> {
  return (await getRedisDataClient().get(chatClosedKey(tripId))) !== null;
}

/** Clear the flag on trip teardown. Best-effort; the TTL is the backstop. */
export async function clearChatWindow(tripId: string): Promise<void> {
  await getRedisDataClient().del(chatClosedKey(tripId)).catch(() => {});
}
