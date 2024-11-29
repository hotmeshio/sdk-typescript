import { Client as Postgres } from 'pg';
import { connect as NATS } from 'nats';

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
import { dropTables, nats_options, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Sequence | Postgres+NATS', () => {
  const appConfig = { id: 'tree' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    //init/activate HotMesh (test both `engine` and `worker` roles)
    const config: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connections: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: NATS, options: nats_options },
        },
      },
      workers: [
        {
          //worker activity in the YAML file declares 'summer' as the topic
          topic: 'summer',
          connections: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: NATS, options: nats_options },
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
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Deploy and Activate', () => {
    it('deploys and activates', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/tree/v2/hotmesh.yaml');
      const isActivated = await hotMesh.activate('2');
      expect(isActivated).toBe(true);
    });
  });

  describe('Run Version', () => {
    it('should run and map activities in sequence', async () => {
      const payload = { seed: 5, speed: 7 };
      const result = await hotMesh.pubsub('spring', payload, null, 3_500);
      const data = result?.data as {
        seed: number;
        speed: number;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.height).toBe(payload.seed * payload.speed);
    });
  });
});
