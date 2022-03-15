// Uncomment these imports to begin using these cool features!

import {service} from '@loopback/core';
import {getModelSchemaRef, post, requestBody} from '@loopback/rest';
import {Product} from '../models';
import {KafkaService} from '../services';

// import {inject} from '@loopback/core';

export class ProductController {
  constructor(@service(KafkaService) private kafkaService: KafkaService) {}

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
    return this.kafkaService.publishSync({name: product.name}, 'products');
  }
}
