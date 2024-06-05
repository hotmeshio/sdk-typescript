import * as Redis from 'redis';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/clients/redis';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { RedisRedisClassType } from '../../../types/redis';

describe('FUNCTIONAL | Sequence', () => {
  const appConfig = { id: 'tree' };
  const options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis as unknown as RedisRedisClassType,
      options,
    );
    redisConnection.getClient().flushDb();

    //init/activate HotMesh (test both `engine` and `worker` roles)
    const config: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: { class: Redis, options },
      },
      workers: [
        {
          //worker activity in the YAML file declares 'summer' as the topic
          topic: 'summer',
          redis: { class: Redis, options },
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
      const result = await hotMesh.pubsub('spring', payload, null, 2_000);
      const data = result?.data as {
        seed: number;
        speed: number;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.height).toBe(payload.seed * payload.speed);
    }, 2_500);
  });
});
