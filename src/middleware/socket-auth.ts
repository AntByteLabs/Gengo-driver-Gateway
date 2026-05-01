import type { Socket } from 'socket.io';
import jwt from 'jsonwebtoken';
import { config } from '../config.js';
import { logger } from '../logger.js';
import type { JwtPayload } from '../domain/types.js';

type NextFn = (err?: Error) => void;

/**
 * Socket.io middleware that:
 *  1. Reads `socket.handshake.auth.token` (expected: "Bearer <jwt>")
 *  2. Verifies signature with JWT_SECRET
 *  3. Attaches decoded payload to `socket.data`
 *  4. Calls next(err) on failure so Socket.io disconnects the socket
 */
export function createSocketAuthMiddleware(expectedRole: 'driver' | 'rider') {
  return (socket: Socket, next: NextFn): void => {
    try {
      const raw: unknown = socket.handshake.auth?.token;

      if (typeof raw !== 'string' || !raw.startsWith('Bearer ')) {
        next(new Error('AUTH_MISSING_TOKEN'));
        return;
      }

      const token = raw.slice(7).trim();

      const decoded = jwt.verify(token, config.JWT_SECRET) as JwtPayload;

      if (!decoded.sub) {
        next(new Error('AUTH_INVALID_CLAIMS'));
        return;
      }

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
      logger.warn({ err }, 'Socket authentication failed');
      next(new Error('AUTH_INVALID_TOKEN'));
    }
  };
}
