import {lifeCycleObserver, LifeCycleObserver, service} from '@loopback/core';
import {KafkaService} from '../services';

@lifeCycleObserver('')
export class MessagingObserver implements LifeCycleObserver {
  constructor(@service(KafkaService) private kafkaService: KafkaService) {}

  /**
   * This method will be invoked when the application initializes. It will be
   * called at most once for a given application instance.
   */
  async init(): Promise<void> {
    this.kafkaService.consumeGetUserMessages('getUser');
  }

  /**
   * This method will be invoked when the application starts.
   */
  async start(): Promise<void> {
    // Add your logic for start
  }

  /**
   * This method will be invoked when the application stops.
   */
  async stop(): Promise<void> {
    // Add your logic for stop
  }
}
