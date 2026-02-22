import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { dropTables } from '../../$setup/postgres';

describe('FUNCTIONAL | Parallel', () => {
  const appConfig = { id: 'tree' };
  const options = {
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    user: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    database: config.POSTGRES_DB,
  };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Postgres and flush db
    const postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, options)
    ).getClient();
    await dropTables(postgresClient);

    //init/activate HotMesh (test both `engine` and `worker` roles)
    const config: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connection: { class: Postgres, options },
      },
      workers: [
        {
          //worker activity in the YAML file declares 'summer' as the topic
          topic: 'summer',
          connection: { class: Postgres, options },
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
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Deploy and Activate', () => {
    it('deploys and activates', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/tree/v1/hotmesh.yaml');
      const isActivated = await hotMesh.activate('1');
      expect(isActivated).toBe(true);
    });
  });

  describe('Run Version', () => {
    it('should run and map activities in parallel', async () => {
      const payload = { seed: 2, speed: 3 };
      const result = await hotMesh.pubsub('spring', payload, null, 1500);
      const data = result?.data as {
        seed: number;
        speed: number;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.height).toBe(payload.seed * payload.speed);
      const exported = await hotMesh.export(result.metadata.jid);
      expect(exported.status).toBe('0');
    }, 2_000);
  });
});
