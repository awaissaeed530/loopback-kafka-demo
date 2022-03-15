/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {BindingScope, injectable} from '@loopback/core';
import {
  ConsumerGroup,
  HighLevelProducer,
  KafkaClient,
  Message,
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

  consume(
    topic: string,
    groupId: string,
    callback: (value: any, message: Message) => any,
  ) {
    const consumer = this.createConsumer(groupId, [topic]);
    const eventName = RandomKey(25);
    this.on(eventName, callback);

    consumer.on('message', message => {
      const value = JSON.parse(message.value.toString());
      this.emit(eventName, value, message);
      consumer.commit(() => {});
    });
    consumer.on('error', error => {
      throw new Error(error);
    });
  }

  publishSync(messages: any, action: string): Promise<any> {
    const key = RandomKey(30);
    const replyTopic = `${action}Reply`;

    const messageData = {
      body: messages,
      callback: {
        type: 'sync',
        topic: replyTopic,
        key,
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

  consumeSync(action: string, callback: (value: any) => any) {
    this.consume(action, `${action}Consumer`, value => {
      const response = callback(value.body);
      this.publish(response, value.callback.topic, value.callback.key);
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
    this.consume(topic, `${topic}Consumer`, (response, message) => {
      this.emit(message.key as string, response);
    });
  }
}
