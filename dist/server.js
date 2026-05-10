import express from 'express';
import { createServer } from 'node:http';
import { Server } from 'socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import { Redis } from 'ioredis';
import { config } from './config.js';
import { logger } from './logger.js';
import { setupDriverNamespace } from './namespaces/driver.namespace.js';
import { setupRiderNamespace } from './namespaces/rider.namespace.js';
import { startTripEventsConsumer } from './consumers/trip-events.consumer.js';
import { startOfferConsumer } from './consumers/offer.consumer.js';
import { closeRedisDataClient } from './infrastructure/redis.js';
import { closeRedisPubsubClient } from './infrastructure/pubsub.js';
import { closeKafkaConsumer, closeKafkaProducer } from './infrastructure/kafka.js';
// ─── Express app (health / readiness probes only) ─────────────────────────────
const app = express();
app.disable('x-powered-by');
app.use(express.json());
app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'driver-gateway', ts: Date.now() });
});
app.get('/ready', (_req, res) => {
    res.json({ status: 'ready' });
});
// ─── HTTP server ──────────────────────────────────────────────────────────────
const httpServer = createServer(app);
// ─── Socket.io server ─────────────────────────────────────────────────────────
const io = new Server(httpServer, {
    cors: {
        origin: config.CORS_ORIGINS,
        methods: ['GET', 'POST'],
        credentials: true,
    },
    // WebSocket only. The HTTP long-poll fallback puts the JWT in the URL
    // query string on every poll request, which lands in proxy/access logs
    // and browser history. Mobile clients (the only consumers of this
    // gateway) all support raw WebSocket; falling back to polling buys us
    // nothing here and leaks credentials.
    transports: ['websocket'],
    pingTimeout: 20_000,
    pingInterval: 25_000,
    connectTimeout: 10_000,
});
// ─── Redis adapter for horizontal scaling ─────────────────────────────────────
const adapterPubClient = new Redis(config.REDIS_URL);
const adapterSubClient = adapterPubClient.duplicate();
adapterPubClient.on('error', (err) => logger.error({ err }, 'Socket.io Redis adapter pub error'));
adapterSubClient.on('error', (err) => logger.error({ err }, 'Socket.io Redis adapter sub error'));
io.adapter(createAdapter(adapterPubClient, adapterSubClient));
// ─── Namespace setup ──────────────────────────────────────────────────────────
// Both namespaces receive the root `io` instance so they can resolve the
// sibling namespace lazily at event time (avoids forward-reference issues).
const driverNsp = setupDriverNamespace(io);
const riderNsp = setupRiderNamespace(io);
// ─── Kafka consumers ──────────────────────────────────────────────────────────
await startTripEventsConsumer(driverNsp, riderNsp);
await startOfferConsumer(driverNsp);
// ─── Start listening ──────────────────────────────────────────────────────────
export function startServer() {
    httpServer.listen(config.PORT, () => {
        logger.info({ port: config.PORT }, 'driver-gateway listening');
    });
}
// ─── Graceful shutdown ────────────────────────────────────────────────────────
/** How long we let in-flight WS emits drain after we ask sockets to close,
 *  before tearing down HTTP and the Redis adapter. Long enough that a
 *  pending `trip:status` or `driver:location` reaches the client; short
 *  enough that K8s' default 30s grace period covers us. */
const WS_DRAIN_MS = 3_000;
export async function gracefulShutdown(signal) {
    logger.info({ signal }, 'Shutting down driver-gateway');
    // 1. Stop accepting new HTTP connections (Express side). `httpServer.close`
    //    is non-blocking — already-established sockets keep running until the
    //    Socket.io disconnect path completes.
    httpServer.close((err) => {
        if (err)
            logger.warn({ err }, 'httpServer.close reported error');
    });
    // 2. Tell every connected socket to disconnect. The `true` argument
    //    closes the underlying transport AFTER flushing pending packets —
    //    so in-flight emits actually leave the process. We then wait briefly
    //    for the adapter's pub/sub fan-out to drain before closing Redis.
    try {
        io.disconnectSockets(true);
    }
    catch (err) {
        logger.warn({ err }, 'io.disconnectSockets threw');
    }
    // 3. Brief drain window. Without this, the Redis adapter's pub/sub
    //    clients can be quit mid-broadcast and other gateway pods miss
    //    end-of-life events.
    await new Promise((resolve) => setTimeout(resolve, WS_DRAIN_MS));
    // 4. Close Kafka and the data Redis client. These are independent of
    //    the adapter pub/sub used for cross-pod fan-out.
    await Promise.allSettled([
        closeKafkaConsumer(),
        closeKafkaProducer(),
        closeRedisDataClient(),
        closeRedisPubsubClient(),
    ]);
    // 5. Close the Socket.io server itself, then the adapter clients LAST —
    //    once io is shut, no more publishes can hit the adapter. Closing
    //    the adapter clients before this can race with adapter publishes
    //    fired from within the io close path.
    await new Promise((resolve) => io.close(() => resolve()));
    await Promise.allSettled([adapterPubClient.quit(), adapterSubClient.quit()]);
    logger.info('driver-gateway shutdown complete');
}
//# sourceMappingURL=server.js.map