import { Pool as PostgresPool } from 'pg';
import { connect as NATS } from 'nats';

import { MeshOS } from '../../services/meshos';
import { guid } from '../../modules/utils';
import { dropTables, nats_options, postgres_options } from '../$setup/postgres';

import { schema } from './src/schema';
import { Widget } from './src/widget';

describe('MeshOS | Postgres+NATS', () => {
  let postgresPoolClient: any;

  beforeAll(async () => {
    //instance a pool client
    postgresPoolClient = new PostgresPool(postgres_options);

    //drop old tables (full clean start)
    await dropTables(postgresPoolClient);
  });

  afterAll(async () => {
    await new Promise((resolve) => setTimeout(resolve, 15_000));
    await MeshOS.shutdown();
  }, 30_000);

  describe('connect', () => {
    it('should connect a function and auto-deploy HotMesh to Postgres', async () => {
      //registration methods (register the participants in the mesh)
      MeshOS.registerDatabase('postgres', {
        name: 'postgres',
        label: 'Postgres',
        search: true,
        connection: {
          store: {
            class: postgresPoolClient,
            options: {},
          },
          stream: {
            class: postgresPoolClient,
            options: {},
          },
          sub: {
            class: NATS,
            options: nats_options,
          },
        },
      });

      MeshOS.registerEntity('widget', {
        name: 'widget',
        label: 'Widget',
        schema: schema,
        class: Widget,
        taskQueue: 'v1',
      });

      // many to many
      MeshOS.registerNamespace('meshostest', {
        name: 'meshostest',
        type: 'meshostest', //unused; todo: remove next build
        module: 'meshdata',
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

    //run another test. spin up a new client
    it('should connect a new client and auto-deploy HotMesh to Postgres', async () => {
      //locate an entity instance (singleton instance of the widget class in the specific database/namespace)
      const entity = MeshOS.findEntity('postgres', 'meshostest', 'widget');
      expect(entity).toBeDefined();

      //create a widget (both a workflow and data record)
      const id = guid();
      const response = (await entity?.create({
        $entity: 'widget',
        id,
        active: 'n',
      })) as { hello: string };
      expect(response.hello).toBe(id);

      const widget = (await entity?.retrieve(id)) as {
        id: string;
        $entity: 'widget';
        active: 'y' | 'n';
      };
      expect(widget.active).toBe('n');
      expect(widget.id).toBe(id);
      expect(widget.$entity).toBe('widget');

      const json = MeshOS.toJSON();
      expect(json).toBeDefined();
    }, 15_000);
  });
});
