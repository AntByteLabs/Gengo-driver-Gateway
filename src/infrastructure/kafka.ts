import { Kafka, Producer, Consumer, logLevel } from 'kafkajs';
import { config } from '../config.js';
import { logger } from '../logger.js';

const kafka = new Kafka({
  clientId: 'driver-gateway',
  brokers: config.KAFKA_BROKERS,
  logLevel: config.NODE_ENV === 'production' ? logLevel.WARN : logLevel.INFO,
  retry: {
    initialRetryTime: 300,
    retries: 10,
  },
});

// ─── Producer ─────────────────────────────────────────────────────────────────

let _producer: Producer | null = null;

export async function getKafkaProducer(): Promise<Producer> {
  if (!_producer) {
    // This producer only carries driver location/presence beats
    // (KAFKA_TOPIC_DRIVER_LOCATION) — lossy-tolerant, high-frequency events.
    // `idempotent` is deliberately NOT set: it forces acks=-1 (full ISR ack)
    // on every send, which serializes the hot location path against broker
    // replication. If trip-critical events ever need to flow through this
    // gateway, create a SEPARATE producer with idempotent/acks=-1 semantics
    // rather than re-hardening this one.
    _producer = kafka.producer({
      allowAutoTopicCreation: false,
    });
    await _producer.connect();
    logger.info('Kafka producer connected');
  }
  return _producer;
}

export async function closeKafkaProducer(): Promise<void> {
  if (_producer) {
    await _producer.disconnect();
    _producer = null;
    logger.info('Kafka producer disconnected');
  }
}

// ─── Consumer ─────────────────────────────────────────────────────────────────

let _consumer: Consumer | null = null;

export async function getKafkaConsumer(): Promise<Consumer> {
  if (!_consumer) {
    _consumer = kafka.consumer({
      groupId: config.KAFKA_CONSUMER_GROUP,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
    });
    await _consumer.connect();
    logger.info('Kafka consumer connected');
  }
  return _consumer;
}

export async function closeKafkaConsumer(): Promise<void> {
  if (_consumer) {
    await _consumer.disconnect();
    _consumer = null;
    logger.info('Kafka consumer disconnected');
  }
}

// ─── Helper: send a single message ───────────────────────────────────────────

// Tuned for small, lossy-tolerant location/presence payloads:
// - acks: 1 (leader only) — no full-ISR wait per beat.
// - no compression — GZIP on a ~150-byte JSON payload costs CPU for nothing.
// Do not use this helper for events that must survive broker failover.
export async function sendKafkaMessage(
  topic: string,
  key: string,
  value: object,
): Promise<void> {
  const producer = await getKafkaProducer();
  await producer.send({
    topic,
    acks: 1,
    messages: [{ key, value: JSON.stringify(value) }],
  });
}
