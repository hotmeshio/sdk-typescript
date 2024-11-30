import * as Redis from 'redis';
import { Client as Postgres } from 'pg';

import config from '../$setup/config';
import { HotMesh } from '../../services/hotmesh';
import { MeshOS } from '../../services/meshos';
import { RedisConnection } from '../../services/connector/providers/redis';
import * as HotMeshTypes from '../../types';
import { guid } from '../../modules/utils';
import { PostgresConnection } from '../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../types/provider';
import { dropTables } from '../$setup/postgres';

import { schema } from './src/schema';
import { Widget } from './src/widget';

describe('MeshOS | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const redis_options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };
  const postgres_options = {
    user: config.POSTGRES_USER,
    host: config.POSTGRES_HOST,
    database: config.POSTGRES_DB,
    password: config.POSTGRES_PASSWORD,
    port: config.POSTGRES_PORT,
  };

  beforeAll(async () => {
    // Initialize Postgres and drop tables (and data) from prior tests
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      HotMesh.guid(),
      Redis as unknown as HotMeshTypes.RedisRedisClassType,
      redis_options,
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
    it('should connect a function and auto-deploy HotMesh to Postgres', async () => {
      //registration methods (register the participants in the mesh)

      MeshOS.registerDatabase('postgres', {
        name: 'postgres',
        label: 'Postgres',
        search: true, //searchable
        connection: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
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
        name: 'meshostest',
        type: 'meshostest',
        label: 'MeshOS TEST',
        entities: [MeshOS.entities['widget']],
      });

      // many to many
      MeshOS.registerProfile('postgres', {
        db: MeshOS.databases.postgres,
        namespaces: {
          meshostest: MeshOS.namespaces.meshostest,
        },
      });

      MeshOS.registerSchema('widget', schema);

      MeshOS.registerClass('Widget', Widget);

      //connect to the mesh (if we're the first to connect, we ARE the mesh)
      await MeshOS.init();

      //locate an entity instance (singleton instance of the widget class in the specific database/namespace)
      //the mesh initialization script allows for an entity to exist anywhere, so it must be specifically targeted
      const entity = MeshOS.findEntity('postgres', 'meshostest', 'widget');
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

      const json = MeshOS.toJSON();
      expect(json).toBeDefined();
    }, 15_000);
  });
});
