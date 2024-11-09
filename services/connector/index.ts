import { guid, identifyRedisTypeFromClass } from '../../modules/utils';
import { HotMeshEngine, HotMeshWorker } from '../../types/hotmesh';
import {
  RedisClass,
  RedisOptions,
  RedisRedisClassType,
  RedisRedisClientOptions,
  IORedisClassType,
  IORedisClientOptions,
} from '../../types/redis';

import { RedisConnection as IORedisConnection } from './providers/ioredis';
import { RedisConnection } from './providers/redis';

export class ConnectorService {
  //1) Initialize `store`, `stream`, and `subscription` Redis clients.
  //2) Bind to the target if not already present
  static async initRedisClients(
    Redis: Partial<RedisClass>,
    options: Partial<RedisOptions>,
    target: HotMeshEngine | HotMeshWorker,
  ): Promise<void> {
    if (!target.store || !target.stream || !target.sub) {
      const instances = [];
      if (identifyRedisTypeFromClass(Redis) === 'redis') {
        for (let i = 1; i <= 3; i++) {
          instances.push(
            RedisConnection.connect(
              guid(),
              Redis as RedisRedisClassType,
              options as RedisRedisClientOptions,
            ),
          );
        }
      } else {
        for (let i = 1; i <= 3; i++) {
          instances.push(
            IORedisConnection.connect(
              guid(),
              Redis as IORedisClassType,
              options as IORedisClientOptions,
            ),
          );
        }
      }
      const [store, stream, sub] = await Promise.all(instances);
      target.store = target.store || store.getClient();
      target.stream = target.stream || stream.getClient();
      target.sub = target.sub || sub.getClient();
    }
  }
}
