import { Producer, Consumer } from 'kafkajs';
export declare function getKafkaProducer(): Promise<Producer>;
export declare function closeKafkaProducer(): Promise<void>;
export declare function getKafkaConsumer(): Promise<Consumer>;
export declare function closeKafkaConsumer(): Promise<void>;
export declare function sendKafkaMessage(topic: string, key: string, value: object): Promise<void>;
//# sourceMappingURL=kafka.d.ts.map