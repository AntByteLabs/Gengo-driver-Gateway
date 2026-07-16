import jwt from 'jsonwebtoken';
import { config } from '../config.js';
import type { JwtPayload } from '../domain/types.js';

export type TokenErrorCode = 'AUTH_MISSING_TOKEN' | 'AUTH_INVALID_TOKEN' | 'AUTH_INVALID_CLAIMS';

/** Raised when a Bearer credential is absent, unverifiable, or claimless. The
 *  `code` doubles as the wire error string the socket handshake already emits. */
export class TokenError extends Error {
  constructor(public readonly code: TokenErrorCode) {
    super(code);
    this.name = 'TokenError';
  }
}

/**
 * Verify a "Bearer <jwt>" credential — the ONE token primitive shared by the
 * socket handshake middleware and the HTTP location-ping route, both of which
 * are doors into the same rider trip rooms. Keeping the crypto/claim check in a
 * single place means a future auth-hardening change (issuer/audience checks,
 * clock tolerance, jti revocation, algorithm pinning, key rotation) reaches
 * every entrance at once instead of silently missing the second one.
 *
 * Throws TokenError on any failure; returns the decoded payload (sub guaranteed)
 * on success. Callers layer their own concerns on top: the socket middleware
 * adds the role gate + next(err) shape, the route maps to a 401 JSON body.
 */
export function verifyBearerToken(raw: unknown): JwtPayload {
  if (typeof raw !== 'string' || !raw.startsWith('Bearer ')) {
    throw new TokenError('AUTH_MISSING_TOKEN');
  }
  let decoded: JwtPayload;
  try {
    decoded = jwt.verify(raw.slice(7).trim(), config.JWT_SECRET) as JwtPayload;
  } catch {
    throw new TokenError('AUTH_INVALID_TOKEN');
  }
  if (!decoded.sub) {
    throw new TokenError('AUTH_INVALID_CLAIMS');
  }
  return decoded;
}
