import type { Server, Namespace } from 'socket.io';
/**
 * Configures the /rider Socket.io namespace.
 * The sibling /driver namespace is resolved lazily from `io` at connection time.
 */
export declare function setupRiderNamespace(io: Server): Namespace;
//# sourceMappingURL=rider.namespace.d.ts.map