import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3011),

  JWT_SECRET: z.string().min(1, 'JWT_SECRET is required'),

  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  REDIS_PUBSUB_URL: z.string().url().default('redis://localhost:6379'),

  KAFKA_BROKERS: z
    .string()
    .default('localhost:9092')
    .transform((v) => v.split(',').map((b) => b.trim())),
  KAFKA_CONSUMER_GROUP: z.string().default('driver-gateway-consumer'),
  KAFKA_TOPIC_TRIP_EVENTS: z.string().default('trip.events'),
  KAFKA_TOPIC_DRIVER_LOCATION: z.string().default('driver.location.updated'),
  KAFKA_TOPIC_NOTIFICATION_OUTBOX: z.string().default('notification.outbox'),

  CORS_ORIGINS: z
    .string()
    .default('*')
    .transform((v) => (v === '*' ? '*' : v.split(',').map((o) => o.trim()))),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('Invalid environment configuration:');
  console.error(parsed.error.format());
  process.exit(1);
}

export const config = parsed.data;
export type Config = typeof config;
