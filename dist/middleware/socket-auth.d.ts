import type { Socket } from 'socket.io';
type NextFn = (err?: Error) => void;
/**
 * Socket.io middleware that:
 *  1. Reads `socket.handshake.auth.token` (expected: "Bearer <jwt>")
 *  2. Verifies signature with JWT_SECRET
 *  3. Attaches decoded payload to `socket.data`
 *  4. Calls next(err) on failure so Socket.io disconnects the socket
 */
export declare function createSocketAuthMiddleware(expectedRole: 'driver' | 'rider'): (socket: Socket, next: NextFn) => void;
export {};
//# sourceMappingURL=socket-auth.d.ts.map