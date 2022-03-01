/* eslint-disable @typescript-eslint/no-explicit-any */
import {BindingScope, inject, injectable} from '@loopback/core';
import {
  ConsumerGroup,
  HighLevelProducer,
  KafkaClient,
  ProduceRequest,
} from 'kafka-node';
import uuid = require('uuid');

const KAFKA_HOST = 'localhost:9092';

@injectable({scope: BindingScope.APPLICATION})
export class KafkaService {
  private client: KafkaClient;
  private producer: HighLevelProducer;

  constructor(
    @inject('kafka.host', {optional: true})
    private kafkaHost: string = KAFKA_HOST,
  ) {
    this.client = new KafkaClient({kafkaHost});
    this.producer = new HighLevelProducer(this.client, {});
  }

  async publish(message: string, topic: string): Promise<any> {
    // await this.isProducerReady();
    const req: ProduceRequest = {topic, messages: [message]};
    return new Promise<any>((resolve, reject) => {
      this.producer.send([req], (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  consumeMessages(topic: string) {
    const consumer = this.createConsumer('', [topic], 'none');

    consumer.on('message', message => {
      console.log(
        `id: ${message.offset}\nevent: message\ndata: ${JSON.stringify(
          message,
        )}\n`,
      );
    });
  }

  private createConsumer(
    groupId: string,
    topics: string[],
    clientId = uuid.v4(),
  ) {
    const consumer = new ConsumerGroup(
      {
        kafkaHost: this.kafkaHost,
        groupId: groupId || 'DefaultGroup',
        fromOffset: 'latest',
        id: clientId,
      },
      topics,
    );
    return consumer;
  }
}
