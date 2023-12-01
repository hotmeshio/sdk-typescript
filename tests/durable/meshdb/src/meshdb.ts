import * as Redis from 'redis';

import config from '../../../$setup/config'
import { Durable } from '../../../../services/durable';
import { WorkflowSearchOptions } from '../../../../types/durable';

export class MeshDBTest extends Durable.MeshDB {

  namespace = 'staging';
  taskQueue = 'inventory';

  redisClass = Redis;
  redisOptions = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  }

  search: WorkflowSearchOptions = {
    index: 'customer-orders',
    prefix: ['ord_'],
    schema: {
      quantity: {
        type: 'NUMERIC', // | TEXT | TAG
        sortable: true
      }
    }
  };

  constructor(id?: string, taskQueue?: string) {
    super(id, taskQueue);
  }
}
