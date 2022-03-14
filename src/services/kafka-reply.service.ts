/* eslint-disable @typescript-eslint/no-explicit-any */
import events, {EventEmitter} from 'events';
import {
  ConsumerGroup,
  ConsumerGroupOptions,
  CustomPartitioner,
  HighLevelProducer,
  KafkaClient,
} from 'kafka-node';

const RandomKey = (length: number) => {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  for (let i = 0; i < length; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  return text;
};

export class KafkaNodeReply {
  client: KafkaClient;
  topicRequest: RequestOptions;
  topicReply: ReplyOptions;
  producer: HighLevelProducer;
  consumer: ConsumerGroup;
  replyEmitter: EventEmitter.EventEmitter;

  /**
   * @param client The kafka-node client
   * @param topicRequest The topic request config
   * @param topicReply The topic request config
   *
   * @returns HighLevelProducer with property function requestSync
   */
  constructor(
    client: KafkaClient,
    topicRequest: RequestOptions,
    topicReply: ReplyOptions,
  ) {
    this.client = client;
    this.topicRequest = topicRequest;
    this.topicReply = topicReply;
    this.replyEmitter = new events.EventEmitter();
    this.consumer = this.newConsumerReply(this.topicReply);
    this.producer = this.newHighLevelProducer();

    // Start consumer
    this.listenReplyEvent();

    return this;
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
   * @param {Object} message The message
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
  requestSync(message: any, timeout: number) {
    // Random key
    const key = RandomKey(30);
    if (!message.callback) {
      const callback = {
        type: 'sync',
        kafka: {
          topic: this.topicReply.topic,
          key: key,
        },
      };
      message.callback = callback;
    }

    // Stringify message
    message = JSON.stringify(message);

    // Prepare && Wrap message
    const topic = {
      messages: message,
      key: key,
      topic: this.topicRequest.topic,
    };
    const payloads = [topic];

    return new Promise((resolve, reject) => {
      const settimeout = setTimeout(() => {
        const error = new Error(
          `[KafkaNodeReply] Request ${key} TIMEOUT after ${timeout}ms`,
        );
        return reject(error);
      }, timeout);

      this.replyEmitter.once(topic.key, (response: any) => {
        clearTimeout(settimeout);
        return resolve(response);
      });

      this.producer?.send(payloads, function (err, data) {
        if (err) {
          console.log('DEBUG', data);
          return reject(err);
        }
      });
    });
  }

  /**
   * Create consumer group, config and options same to initialize a https://www.npmjs.com/package/kafka-node#consumer
   *
   * @param {Object} `options`
   * @param {String} `topicReply.topic` Name of topic that reply consumer receive message
   * @param {Object} `topicReply.options`
   * @param {String} `topicReply.options.groupId` Unique groupId to identified group consume topic
   * Example:
   *  {
   *     topicRequest: "PmsPropertyRoomCreate_Request",
   *     topicReply: "PmsPropertyRoomCreate_Request",
   *     options: {
   *       groupId: "hms.cts.organization"
   *     }
   *  }
   */
  private newConsumerReply(options: ReplyOptions) {
    const topicPartition: any = {};
    topicPartition[options.topic] = undefined;

    const consumerGroup = new ConsumerGroup(
      options.consumerOptions,
      options.topic,
    );
    consumerGroup.on('error', () => {
      let consumerConnect = false;
      const t = setTimeout(() => {
        if (consumerConnect === false) {
          this.newConsumerReply(this.topicReply);
        } else {
          consumerConnect = true;
          clearTimeout(t);
        }
      }, 1000);
    });
    return consumerGroup;
  }

  /**
   * Create new Producer base on KafkaNode.HighLevelProducer
   *
   * Returns:
   * Kafka producer
   */
  private newHighLevelProducer() {
    const producer = new HighLevelProducer(this.client);

    producer.on('ready', function () {
      console.info('[KafkaNodeReply] New Producer already');
    });
    producer.on('error', function (err: any) {
      console.error('[KafkaNodeReply] ERROR when new Producer ', err);
    });
    return producer;
  }

  /**
   * Start consumer and wait for message receive
   * When receive a message. The events emit will publish this message value with key in message
   */
  private listenReplyEvent() {
    this.consumer?.on('message', message => {
      let response;
      try {
        response = JSON.parse(message.value.toString());
      } catch (err) {
        response = message.value;
      }
      this.replyEmitter.emit(message.key as string, response);
      this.consumer?.commit(() => {});
    });

    this.consumer?.on('error', error => {
      console.error('[KafkaNodeReply] ERROR when consume: ', error);
    });
  }
}

type RequestOptions = {
  topic: string;
  customPartitioner?: CustomPartitioner;
};

type ReplyOptions = {
  topic: string;
  consumerOptions: ConsumerGroupOptions;
};
