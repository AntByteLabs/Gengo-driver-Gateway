import type { Server, Namespace } from 'socket.io';
/**
 * Configures the /driver Socket.io namespace.
 * The sibling /rider namespace is resolved lazily from `io` at connection time
 * to avoid forward-reference issues during startup.
 */
export declare function setupDriverNamespace(io: Server): Namespace;
//# sourceMappingURL=driver.namespace.d.ts.map