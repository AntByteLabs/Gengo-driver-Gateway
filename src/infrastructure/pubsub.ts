import Redis from 'ioredis';
import { config } from '../config.js';
import { logger } from '../logger.js';

// ─── Pubsub client (SUBSCRIBE only — monopolises connection) ─────────────────

let _pubsubClient: Redis | null = null;

export function getRedisPubsubClient(): Redis {
  if (!_pubsubClient) {
    _pubsubClient = new Redis(config.REDIS_PUBSUB_URL, {
      lazyConnect: false,
      enableReadyCheck: true,
      maxRetriesPerRequest: null, // subscriber mode — retry indefinitely
      retryStrategy: (times) => {
        return Math.min(times * 300, 5000);
      },
    });

    _pubsubClient.on('connect', () => logger.info('Redis pubsub client connected'));
    _pubsubClient.on('error', (err) => logger.error({ err }, 'Redis pubsub client error'));
    _pubsubClient.on('close', () => logger.warn('Redis pubsub client connection closed'));
  }
  return _pubsubClient;
}

export async function closeRedisPubsubClient(): Promise<void> {
  if (_pubsubClient) {
    await _pubsubClient.quit();
    _pubsubClient = null;
    logger.info('Redis pubsub client closed');
  }
}
