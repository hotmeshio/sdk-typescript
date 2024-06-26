import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import config from '../../$setup/config';

describe('FUNCTIONAL | EXPIRED', () => {
  const appConfig = { id: 'expired', version: '1' };
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      options,
    );
    redisConnection.getClient().flushdb();

    const config: HotMeshConfig = {
      appId: appConfig.id,
      namespace: HMNS,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        redis: { class: Redis, options },
      },

      workers: [
        {
          topic: 'expired.test.worker',
          redis: { class: Redis, options },
          callback: async (
            streamData: StreamData,
          ): Promise<StreamDataResponse> => {
            return {
              status: StreamStatus.SUCCESS,
              metadata: { ...streamData.metadata },
              data: { status: 'success' },
            } as StreamDataResponse;
          },
        },
      ],
    };
    hotMesh = await HotMesh.init(config);
    await hotMesh.deploy('/app/tests/$setup/apps/expired/v1/hotmesh.yaml');
    await hotMesh.activate(appConfig.version);
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Create Expired Job', () => {
    it('should create a job and scrub (after a delay)', async () => {
      //create a job in an expired state
      const secondsToWaitBeforeScrubbing = 2;
      const jobId = await hotMesh.pub('expired.test', {}, undefined, {
        expired: secondsToWaitBeforeScrubbing,
      });
      expect(jobId).toBeDefined();
      const status = await hotMesh.getStatus(jobId);
      expect(status).toBe(-1); //expired jobs are set to -1

      //wait longer than 2 seconds to ensure redis scrubs the job
      await sleepFor(2_250);
      try {
        await hotMesh.getStatus(jobId);
      } catch (error) {
        expect(error.message).toBe(`Job ${jobId} not found`);
      }
    });
  });
});
