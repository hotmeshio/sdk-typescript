import Redis from 'ioredis';

import config from '../../../$setup/config';
import { ConnectorService } from '../../../../services/connector/factory';
import { RedisConnection } from '../../../../services/connector/providers/ioredis';
import { HotMeshEngine, HotMeshWorker } from '../../../../types/hotmesh';
import { RedisOptions } from '../../../../types/redis';

describe('ConnectorService Functional Test', () => {
  let target: HotMeshEngine;
  const redisOptions: RedisOptions = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  const RedisClass = Redis;

  beforeEach(() => {
    target = {} as HotMeshEngine;
  });

  it('should initialize Redis clients if not already present', async () => {
    const target: HotMeshEngine | HotMeshWorker = {
      connection: {
        class: RedisClass,
        options: redisOptions,
      },
      store: undefined,
      stream: undefined,
      sub: undefined,
    };
    await ConnectorService.initClients(target);

    // Verify that the target object has store, stream, and sub properties
    expect(target.store).toBeDefined();
    expect(target.stream).toBeDefined();
    expect(target.sub).toBeDefined();

    // Verify they can actually interact with Redis
    await target?.store?.set('testKeyStore', 'testValue');
    const valueStore = await target?.store?.get('testKeyStore');
    expect(valueStore).toBe('testValue');
  });

  // Disconnect from Redis after all tests
  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });
});
