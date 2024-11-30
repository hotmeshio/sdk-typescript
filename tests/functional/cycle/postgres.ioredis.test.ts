import { Client as Postgres } from 'pg';
import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { guid } from '../../../modules/utils';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { StreamData, StreamDataResponse } from '../../../types/stream';
import { ProviderNativeClient } from '../../../types/provider';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';

describe('FUNCTIONAL | Activity Cycles | Postgres+IORedis', () => {
  const appConfig = { id: 'cycle' };
  let counter = 0;
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    // Initialize Postgres
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    // Drop tables
    await dropTables(postgresClient);

    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      redis_options,
    );
    redisConnection.getClient().flushdb();

    //init HotMesh
    const hmshConfig: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        connection: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
        },
      },

      workers: [
        //this worker will return a 200 status code; the yaml
        //model is cofigured to cycle as long as `counter < 5`
        //this worker runs as part of flow v1
        {
          topic: 'cycle.count',
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          callback: async (
            streamData: StreamData,
          ): Promise<StreamDataResponse> => {
            return {
              metadata: { ...streamData.metadata },
              data: {
                counter: ++counter,
              },
            } as StreamDataResponse;
          },
        },

        //this worker will return a 500 status code for 5 times and then a 200
        //the yaml model dictates that the flow should cycle as long as
        //code is 500;
        //this worker runs as part of flow v2
        {
          topic: 'cycle.err',
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Redis, options: redis_options },
          },
          callback: async (
            streamData: StreamData,
          ): Promise<StreamDataResponse> => {
            counter++;
            return {
              metadata: { ...streamData.metadata },
              code: counter == 5 ? 200 : 500,
              data: {
                counter: counter,
              },
            } as StreamDataResponse;
          },
        },
      ],
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy('/app/tests/$setup/apps/cycle/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  }, 15_000);

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Cycle', () => {
    it('cycles 5 times and then exits', async () => {
      counter = 0;
      const result = await hotMesh.pubsub('cycle.test', {}, null, 10_000);
      const data = result?.data as { counter: number };
      expect(data.counter).toBe(5);
    }, 10_000);
  });

  describe('Pending', () => {
    it('should hot deploy version 2', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/cycle/v2/hotmesh.yaml');
      await hotMesh.activate('2');
    });

    it('cycles while in an error state and then exits', async () => {
      counter = 0;
      const result = await hotMesh.pubsub('cycle.test', {}, null, 10_000);
      const data = result?.data as { counter: number };
      expect(data.counter).toBe(5);
      const exported = await hotMesh.export(result.metadata.jid);
      expect(exported?.status).toBe('0');
    }, 10_000);
  });
});
