import type { Socket } from 'socket.io';
import { logger } from '../logger.js';
import { TokenError, verifyBearerToken } from './verify-token.js';

type NextFn = (err?: Error) => void;

/**
 * Socket.io middleware that:
 *  1. Reads `socket.handshake.auth.token` (expected: "Bearer <jwt>")
 *  2. Verifies it via the shared verifyBearerToken primitive
 *  3. Gates on the expected role, then attaches the id to `socket.data`
 *  4. Calls next(err) on failure so Socket.io disconnects the socket
 */
export function createSocketAuthMiddleware(expectedRole: 'driver' | 'rider') {
  return (socket: Socket, next: NextFn): void => {
    try {
      const decoded = verifyBearerToken(socket.handshake.auth?.token);

      if (decoded.role && decoded.role !== expectedRole) {
        next(new Error('AUTH_WRONG_ROLE'));
        return;
      }

      // Attach to socket.data for use inside event handlers
      if (expectedRole === 'driver') {
        socket.data.driverId = decoded.sub;
      } else {
        socket.data.riderId = decoded.sub;
      }

      next();
    } catch (err) {
      if (err instanceof TokenError) {
        next(new Error(err.code));
        return;
      }
      logger.warn({ err }, 'Socket authentication failed');
      next(new Error('AUTH_INVALID_TOKEN'));
    }
  };
}
