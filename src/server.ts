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
  transports: ['websocket', 'polling'],
  pingTimeout: 20_000,
  pingInterval: 25_000,
  connectTimeout: 10_000,
});

// ─── Redis adapter for horizontal scaling ─────────────────────────────────────

const adapterPubClient = new Redis(config.REDIS_URL);
const adapterSubClient = adapterPubClient.duplicate();

adapterPubClient.on('error', (err: Error) =>
  logger.error({ err }, 'Socket.io Redis adapter pub error'),
);
adapterSubClient.on('error', (err: Error) =>
  logger.error({ err }, 'Socket.io Redis adapter sub error'),
);

// Verify the adapter's Redis is actually reachable before wiring it. Without
// this, a Redis outage at boot leaves the gateway "healthy" while cross-instance
// socket delivery silently fails (events emitted on one pod never reach clients
// on another). Fail fast instead — the gateway depends on Redis to scale.
try {
  await Promise.race([
    adapterPubClient.ping(),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error('Redis ping timed out after 5s')), 5_000),
    ),
  ]);
} catch (err) {
  logger.fatal({ err }, 'Socket.io Redis adapter unreachable at startup — exiting');
  process.exit(1);
}

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

export function startServer(): void {
  httpServer.listen(config.PORT, () => {
    logger.info({ port: config.PORT }, 'driver-gateway listening');
  });
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────

export async function gracefulShutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'Shutting down driver-gateway');

  // Stop accepting new connections
  io.close();

  await Promise.allSettled([
    closeKafkaConsumer(),
    closeKafkaProducer(),
    closeRedisDataClient(),
    closeRedisPubsubClient(),
    adapterPubClient.quit(),
    adapterSubClient.quit(),
  ]);

  logger.info('driver-gateway shutdown complete');
}
