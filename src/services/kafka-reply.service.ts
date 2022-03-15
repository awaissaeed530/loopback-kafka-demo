/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {BindingScope, injectable, service} from '@loopback/core';
import {EventEmitter} from 'events';
import {KafkaService} from './kafka.service';

const RandomKey = (length: number) => {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  for (let i = 0; i < length; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
};

@injectable({scope: BindingScope.APPLICATION})
export class KafkaReplyService extends EventEmitter {
  constructor(@service(KafkaService) private kafkaService: KafkaService) {
    super();
  }

  /**
   * Create request synchronous.
   * An message will be publish to topic `config.topicRequest` which format json below
   * {
   *     "action": "getBatch",
   *     "body": {
   *         "limit": 20
   *     },
   *     "contentType": "application/json",
   *     "headers": {
   *         "Authorization": "Bearer AuthenicationToken"
   *     },
   *     "callback": {
   *         "type": "sync",
   *         "kafka": {
   *             "topic": "PmsPropertyRoomCreate_Request",
   *             "partition": 8,
   *             "key": "Tg9PHN5tlSoI42X0VmoLqhyMsPpSqf"
   *         }
   *     },
   *     "date": "2019-04-11T04:59:22.006Z"
   * }
   *
   * @param {Object} messages The message
   * @param {String} message.action The action that consumer process
   * @param {Object} message.body The request body message
   * @param {String} message.contentType The message content type
   * @param {Object} message.headers The request headers, like http request headers
   * @param {Obejct} message.callback The callback info. Include type of request and method callback. Default callback through kafka service
   * @param {String} message.callback.type Type of request. Value `sync` it mean request require response from consumer to complete request
   * @param {Object} message.callback.kafka Callback method, current support only kafka
   * @param {String} message.callback.kafka.topic The topic name which consumer will send message
   * @param {String} message.callback.kafka.partition The partition specify that consumer will send message to topic above
   * @param {String} message.callback.kafka.key The request key that consumer must send message with key of message for requestor determine it's response
   * @param {Integer} timeout Request timeout in milisecond
   *
   * Other consumer consume topic above and process base on business logic.
   * After process, if request has callback info. Consumer must send response to callback info.
   *
   * Returns:
   * Promise response from consumer
   */
  requestSync(
    messages: any,
    timeout: number,
    requestTopic: string,
    replyTopic: string,
  ): Promise<any> {
    const key = RandomKey(30);
    if (!messages.callback) {
      const callback = {
        type: 'sync',
        kafka: {topic: replyTopic, key},
      };
      messages.callback = callback;
    }

    messages = JSON.stringify(messages);
    this.listenReplyEvent(replyTopic);

    return new Promise((resolve, reject) => {
      const settimeout = setTimeout(() => {
        const error = new Error(
          `[KafkaNodeReply] Request ${key} TIMEOUT after ${timeout}ms`,
        );
        return reject(error);
      }, timeout);

      this.once(key, (response: any) => {
        clearTimeout(settimeout);
        return resolve(response);
      });

      this.kafkaService.publish(messages, requestTopic, key).then();
    });
  }

  private listenReplyEvent(topic: string) {
    const consumer = this.kafkaService.createConsumer('userReplyConsumer', [
      topic,
    ]);
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
