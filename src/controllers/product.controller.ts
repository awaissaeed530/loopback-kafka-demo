// Uncomment these imports to begin using these cool features!

import {service} from '@loopback/core';
import {getModelSchemaRef, post, requestBody} from '@loopback/rest';
import {Product} from '../models';
import {KafkaReplyService} from '../services/kafka-reply.service';

// import {inject} from '@loopback/core';

export class ProductController {
  constructor(
    @service(KafkaReplyService) private kafkaReplyService: KafkaReplyService,
  ) {}

  @post('/produts')
  createProduct(
    @requestBody({
      content: {
        'application/json': {
          schema: getModelSchemaRef(Product),
        },
      },
    })
    product: Product,
  ) {
    return this.kafkaReplyService.requestSync(
      {
        body: {
          name: product.name,
        },
      },
      30000,
      'getUser',
      'getUserReply',
    );
  }
}
