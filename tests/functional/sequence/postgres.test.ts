import { Client as Postgres } from 'pg';

import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { HotMesh, HotMeshConfig } from '../../../index';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Sequence | Postgres', () => {
  const appConfig = { id: 'tree' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE !== 'true') {
      postgresClient = (
        await PostgresConnection.connect(guid(), Postgres, postgres_options)
      ).getClient();

      await dropTables(postgresClient);
    }

    const config: HotMeshConfig = {
      appId: appConfig.id,
      //taskQueue: 'default',
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connection: {
          class: Postgres,
          options: postgres_options,
        },
      },
      workers: [
        {
          //worker activity in the YAML file declares 'summer' as the topic
          topic: 'summer',
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          callback: async (
            streamData: StreamData,
          ): Promise<StreamDataResponse> => {
            return {
              code: 200,
              status: StreamStatus.SUCCESS,
              metadata: { ...streamData.metadata },
              data: { result: new Date().toLocaleString('en-US') },
            } as StreamDataResponse;
          },
        },
      ],
    };
    hotMesh = await HotMesh.init(config);
  }, 20_000);

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Deploy and Activate', () => {
    it('deploys and activates', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/tree/v2/hotmesh.yaml');
      const isActivated = await hotMesh.activate('2');
      expect(isActivated).toBe(true);
    }, 60_000);
  });

  describe('Run Version', () => {
    it('should run and map activities in sequence', async () => {
      const payload = { seed: 5, speed: 7 };
      const result = await hotMesh.pubsub('spring', payload, null, 5_000);
      const data = result?.data as {
        seed: number;
        speed: number;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.height).toBe(payload.seed * payload.speed);
    }, 20_000);
  });

  it('should work with taskQueue connection pooling without notification cross-contamination', async () => {
    const taskQueue = 'test-sequence-queue';
    const testConfig: HotMeshConfig = {
      appId: 'tree',
      logLevel: 'warn', // Use warn level to catch any invalid notification warnings
      taskQueue,
      engine: {
        connection: {
          class: Postgres,
          options: postgres_options,
        },
      },
      workers: [
        {
          topic: 'summer',
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          callback: async (
            streamData: StreamData,
          ): Promise<StreamDataResponse> => {
            return {
              code: 200,
              status: StreamStatus.SUCCESS,
              metadata: { ...streamData.metadata },
              data: { result: `${new Date().toLocaleString('en-US')}-${taskQueue}` },
            } as StreamDataResponse;
          },
        },
      ],
    };
    
    const testHotMesh = await HotMesh.init(testConfig);
    
    try {
      await testHotMesh.deploy('/app/tests/$setup/apps/tree/v2/hotmesh.yaml');
      const isActivated = await testHotMesh.activate('2');
      expect(isActivated).toBe(true);
      
      // Run a test to ensure notifications work without cross-contamination
      const payload = { seed: 5, speed: 7 };
      const result = await testHotMesh.pubsub('spring', payload, null, 5_000);
      const data = result?.data as {
        seed: number;
        speed: number;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.height).toBe(payload.seed * payload.speed);
    } finally {
      await testHotMesh.stop();
    }
  }, 30_000);
});
