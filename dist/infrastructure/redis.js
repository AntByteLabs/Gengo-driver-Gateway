import { Redis } from 'ioredis';
import { config } from '../config.js';
import { logger } from '../logger.js';
// ─── Data client (GEOADD, HSET, GET/SET, etc.) ───────────────────────────────
let _dataClient = null;
export function getRedisDataClient() {
    if (!_dataClient) {
        _dataClient = new Redis(config.REDIS_URL, {
            lazyConnect: false,
            enableReadyCheck: true,
            maxRetriesPerRequest: 3,
            retryStrategy: (times) => {
                if (times > 10)
                    return null;
                return Math.min(times * 200, 3000);
            },
        });
        _dataClient.on('connect', () => logger.info('Redis data client connected'));
        _dataClient.on('error', (err) => logger.error({ err }, 'Redis data client error'));
        _dataClient.on('close', () => logger.warn('Redis data client connection closed'));
    }
    return _dataClient;
}
export async function closeRedisDataClient() {
    if (_dataClient) {
        await _dataClient.quit();
        _dataClient = null;
        logger.info('Redis data client closed');
    }
}
//# sourceMappingURL=redis.js.map