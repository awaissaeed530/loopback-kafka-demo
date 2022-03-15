/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {BindingScope, injectable} from '@loopback/core';
import {
  ConsumerGroup,
  HighLevelProducer,
  KafkaClient,
  ProduceRequest,
} from 'kafka-node';
import EventEmitter = require('events');
import uuid = require('uuid');

const RandomKey = (length: number) => {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  for (let i = 0; i < length; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
};

@injectable({scope: BindingScope.APPLICATION})
export class KafkaService extends EventEmitter {
  private client: KafkaClient;
  private producer: HighLevelProducer;
  private kafkaHost: string;

  constructor() {
    super();
    this.kafkaHost = 'localhost:9092';
    this.client = new KafkaClient({kafkaHost: this.kafkaHost});
    this.producer = new HighLevelProducer(this.client);
  }

  publish(messages: any, topic: string, key?: string): Promise<any> {
    const payloads: ProduceRequest[] = [
      {messages: JSON.stringify(messages), key: key ?? RandomKey(30), topic},
    ];
    return new Promise<any>((resolve, reject) => {
      this.producer.send(payloads, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  consume(topic: string, groupId: string, callback: (value: any) => any) {
    const consumer = this.createConsumer(groupId, [topic]);
    const eventName = RandomKey(25);
    this.on(eventName, callback);

    consumer.on('message', message => {
      const value = JSON.parse(message.value.toString());
      this.emit(eventName, value);
      consumer.commit(() => {});
    });
    consumer.on('error', error => {
      throw new Error(error);
    });
  }

  requestSync(messages: any, action: string): Promise<any> {
    const key = RandomKey(30);
    const replyTopic = `${action}Reply`;

    const messageData = {
      body: messages,
      callback: {
        type: 'sync',
        kafka: {topic: replyTopic, key},
      },
    };

    this.listenReplyEvent(replyTopic);

    return new Promise(resolve => {
      this.once(key, (response: any) => {
        return resolve(response);
      });

      this.publish(messageData, action, key).then();
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

  private listenReplyEvent(topic: string) {
    const consumer = this.createConsumer('userReplyConsumer', [topic]);
    consumer.on('message', message => {
      let response;
      try {
        response = JSON.parse(message.value.toString());
      } catch (err) {
        response = message.value;
      }
      this.emit(message.key as string, response);
      consumer.commit(() => {});
    });
  }
}
