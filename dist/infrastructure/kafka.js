import { Kafka, CompressionTypes, logLevel } from 'kafkajs';
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
let _producer = null;
export async function getKafkaProducer() {
    if (!_producer) {
        _producer = kafka.producer({
            allowAutoTopicCreation: false,
            idempotent: true,
        });
        await _producer.connect();
        logger.info('Kafka producer connected');
    }
    return _producer;
}
export async function closeKafkaProducer() {
    if (_producer) {
        await _producer.disconnect();
        _producer = null;
        logger.info('Kafka producer disconnected');
    }
}
// ─── Consumer ─────────────────────────────────────────────────────────────────
let _consumer = null;
export async function getKafkaConsumer() {
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
export async function closeKafkaConsumer() {
    if (_consumer) {
        await _consumer.disconnect();
        _consumer = null;
        logger.info('Kafka consumer disconnected');
    }
}
// ─── Helper: send a single message ───────────────────────────────────────────
export async function sendKafkaMessage(topic, key, value) {
    const producer = await getKafkaProducer();
    await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: [{ key, value: JSON.stringify(value) }],
    });
}
//# sourceMappingURL=kafka.js.map