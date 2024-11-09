import * as Redis from 'redis';

import config from '../$setup/config';
import { HotMesh } from '../../services/hotmesh';
import { MeshOS } from '../../services/meshos';
import { RedisConnection } from '../../services/connector/providers/redis';
import * as HotMeshTypes from '../../types';
import { guid } from '../../modules/utils';

import { Widget } from './src/widget';
import { schema } from './src/schema';

describe('MeshData`', () => {
  const options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      HotMesh.guid(),
      Redis as unknown as HotMeshTypes.RedisRedisClassType,
      options,
    );
    redisConnection.getClient().flushDb();
  }, 5_000);

  afterAll(async () => {
    //wait for cleanup (various asyn processes should be allowed to complete)
    //todo: verify that memory space is empty
    await new Promise((resolve) => setTimeout(resolve, 25_000));
    //shutdown all connections
    await MeshOS.shutdown();
  }, 30_000);

  describe('connect', () => {
    it('should connect a function and auto-deploy HotMesh to Redis', async () => {
      //registration methods (register the participants in the mesh)

      MeshOS.registerDatabase('redis', {
        label: 'Redis',
        name: 'redis',
        search: true,
        config: {
          REDIS_DATABASE: config.REDIS_DATABASE,
          REDIS_HOST: config.REDIS_HOST,
          REDIS_PASSWORD: config.REDIS_PASSWORD,
          REDIS_PORT: config.REDIS_PORT,
          REDIS_USE_TLS: config.REDIS_USE_TLS,
          REDIS_USERNAME: config.REDIS_USERNAME,
        },
      });

      MeshOS.registerEntity('widget', {
        name: 'widget',
        label: 'Widget',
        schema: schema,
        class: Widget,
      });

      // many to many
      MeshOS.registerNamespace('meshostest', {
        type: 'meshostest',
        name: 'meshostest',
        label: 'MeshOS TEST',
        entities: [MeshOS.entities['widget']],
      });

      // many to many
      MeshOS.registerProfile('redis', {
        db: MeshOS.databases.redis,
        namespaces: {
          meshostest: MeshOS.namespaces.meshostest,
        },
      });

      MeshOS.registerSchema('widget', schema);

      MeshOS.registerClass('Widget', Widget);

      //init everything; start it up!!!!!
      await MeshOS.init();

      //locate an entity instance (class instance of the widget class in the specific database/namespace)
      //the mesh initialization script allows for an entity to exist anywhere, so it must be specifically targeted
      const entity = MeshOS.findEntity('redis', 'meshostest', 'widget');
      expect(entity).toBeDefined();

      //create a widget (both a workflow and data record)
      const id = guid();
      const response = (await entity?.create({
        $entity: 'widget',
        id,
        active: 'y',
      })) as { hello: string };
      expect(response.hello).toBe(id);
      const response2 = (await entity?.update(id, { active: 'n' })) as {
        active: 'y' | 'n';
      };
      expect(response2.active).toBe('n');
    }, 15_000);
  });
});
