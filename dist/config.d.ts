export declare const config: {
    NODE_ENV: "development" | "test" | "production";
    PORT: number;
    JWT_SECRET: string;
    REDIS_URL: string;
    REDIS_PUBSUB_URL: string;
    KAFKA_BROKERS: string[];
    KAFKA_CONSUMER_GROUP: string;
    KAFKA_TOPIC_TRIP_EVENTS: string;
    KAFKA_TOPIC_DRIVER_LOCATION: string;
    KAFKA_TOPIC_NOTIFICATION_OUTBOX: string;
    CORS_ORIGINS: string[] | "*";
};
export type Config = typeof config;
//# sourceMappingURL=config.d.ts.map