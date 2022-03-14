/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {BindingScope, injectable} from '@loopback/core';
import {
  ConsumerGroup,
  HighLevelProducer,
  KafkaClient,
  ProduceRequest,
} from 'kafka-node';
import {KafkaNodeReply} from './kafka-reply.service';
import uuid = require('uuid');

@injectable({scope: BindingScope.APPLICATION})
export class KafkaService {
  private client: KafkaClient;
  private producer: HighLevelProducer;
  private kafkaHost: string;

  constructor() {
    this.kafkaHost = 'localhost:9092';
    this.client = new KafkaClient({kafkaHost: this.kafkaHost});
    this.producer = new HighLevelProducer(this.client);
  }

  async publish(messages: string, topic: string, key?: string): Promise<any> {
    const payloads: ProduceRequest[] = [{messages, key, topic}];
    return new Promise<any>((resolve, reject) => {
      this.producer.send(payloads, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  consumeMessages(topic: string) {
    const consumer = this.createConsumer('getUserConsumer', [topic]);

    consumer.on('message', message => {
      console.log(
        `Recieved message from getUser topic: ${JSON.stringify(message.value)}`,
      );
      const parsedMessage = JSON.parse(message.value as string);
      this.publish(
        JSON.stringify({
          users: [{name: 'Awais Saeed'}, {name: 'Muhammad Tayyab'}],
        }),
        parsedMessage.callback.kafka.topic,
        parsedMessage.callback.kafka.key,
      ).then();
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

  async replyRequest(message: string) {
    const requestTopicOptions = {
      topic: 'getUser',
    };

    const responseTopicOptions = {
      topic: 'getUserReply',
      consumerOptions: {
        groupId: 'getUserReplyConsumer',
      },
    };

    const kafkaReqRes = new KafkaNodeReply(
      this.client,
      requestTopicOptions,
      responseTopicOptions,
    );

    const request = {
      ation: 'getUser',
      body: {
        message,
      },
      headers: {
        ContentType: 'json/application',
      },
    };

    return kafkaReqRes.requestSync(request, 30000);
  }
}
