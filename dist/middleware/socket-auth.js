import jwt from 'jsonwebtoken';
import { config } from '../config.js';
import { logger } from '../logger.js';
/**
 * Socket.io middleware that:
 *  1. Reads `socket.handshake.auth.token` (expected: "Bearer <jwt>")
 *  2. Verifies signature with JWT_SECRET
 *  3. Attaches decoded payload to `socket.data`
 *  4. Calls next(err) on failure so Socket.io disconnects the socket
 */
export function createSocketAuthMiddleware(expectedRole) {
    return (socket, next) => {
        try {
            const raw = socket.handshake.auth?.token;
            if (typeof raw !== 'string' || !raw.startsWith('Bearer ')) {
                next(new Error('AUTH_MISSING_TOKEN'));
                return;
            }
            const token = raw.slice(7).trim();
            // Defence-in-depth against the classic `alg: none` JWT bypass: refuse
            // to even hand the token to `jwt.verify` if its header advertises an
            // algorithm we don't accept. `verify` with `algorithms: ['HS256']`
            // already rejects this, but checking the header first means we never
            // log or branch on attacker-controlled `alg` values downstream.
            const headerB64 = token.split('.')[0];
            if (!headerB64) {
                next(new Error('AUTH_INVALID_TOKEN'));
                return;
            }
            try {
                const headerJson = Buffer.from(headerB64, 'base64url').toString('utf8');
                const header = JSON.parse(headerJson);
                if (header.alg !== 'HS256') {
                    next(new Error('AUTH_INVALID_TOKEN'));
                    return;
                }
            }
            catch {
                next(new Error('AUTH_INVALID_TOKEN'));
                return;
            }
            // Pin algorithm to HS256 — matches packages/shared §5.4. Without this,
            // jsonwebtoken accepts whatever the token header says (including
            // RS256 with our HMAC secret being treated as a public key, or `none`
            // on older versions).
            const decoded = jwt.verify(token, config.JWT_SECRET, {
                algorithms: ['HS256'],
            });
            if (!decoded.sub) {
                next(new Error('AUTH_INVALID_CLAIMS'));
                return;
            }
            // Role is informational only — the same token works on both /driver
            // and /rider namespaces. A single user can switch between rider and
            // driver modes in the same session without re-authenticating; the
            // namespace itself is the mode boundary, not the JWT claim.
            // Note: this means the gateway does NOT verify the user is a
            // registered driver. That check belongs in driver-svc (KYC / vehicle
            // approval) and should gate `driver:online` if/when added — not the
            // socket handshake.
            // Attach to socket.data for use inside event handlers
            if (expectedRole === 'driver') {
                socket.data.driverId = decoded.sub;
            }
            else {
                socket.data.riderId = decoded.sub;
            }
            next();
        }
        catch (err) {
            // Log only the error name/message — never the raw error object, which
            // some jsonwebtoken paths attach the offending token to.
            const name = err instanceof Error ? err.name : 'UnknownError';
            const message = err instanceof Error ? err.message : 'unknown';
            logger.warn({ name, message }, 'Socket authentication failed');
            next(new Error('AUTH_INVALID_TOKEN'));
        }
    };
}
//# sourceMappingURL=socket-auth.js.map