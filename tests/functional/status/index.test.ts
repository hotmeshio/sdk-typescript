import { nanoid } from 'nanoid';
import Redis from 'ioredis';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus } from '../../../types/stream';

describe('FUNCTIONAL | Status Codes', () => {
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  const REASON = 'the account_id field is missing';
  const appConfig = { id: 'def' };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(nanoid(), Redis, options);
    redisConnection.getClient().flushdb();

    //init HotMesh
    const hmshConfig: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: 'debug',

      engine: {
        redis: { class: Redis, options }
      },

      workers: [
        {
          topic: 'work.do',
          redis: { class: Redis, options },
          callback: async (streamData: StreamData): Promise<StreamDataResponse> => {
            let status: StreamStatus;
            let data: { [key: string]: string | number } = {
              code: streamData.data.code as number
            };
            if (streamData.data.code == 202) {
              data.percentage = 49;
              status = StreamStatus.PENDING;

              //send a second message on a delay;
              // it's a 'success' message so it will
              //close the channel
              setTimeout(function() {
                hotMesh.add({
                  code: 200,
                  status: StreamStatus.SUCCESS,
                  metadata: { ...streamData.metadata },
                  data: { code: 200, percentage: 99 }
                });
              }, 250);
            } else if (streamData.data.code == 422) {
              data.message = 'invalid input';
              data.reason = REASON;
              status = StreamStatus.ERROR;
            } else {
              data.code = 200;
              status = StreamStatus.SUCCESS;
            }
            return {
              code: data.code,
              status,
              metadata: { ...streamData.metadata },
              data
            } as StreamDataResponse;
          }
        }
      ]
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy('/app/tests/$setup/apps/def/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  }, 10_000);

  afterAll(async () => {
    await StreamSignaler.stopConsuming();
    await RedisConnection.disconnectAll();
  });

  describe('Run Without Catch', () => {
    it('routes worker 200 and returns a success message', async () => {
      const payload = { code: 200 };
      const result = await hotMesh.pubsub('def.test', payload);
      const data = result?.data as {
        code: number;
        message: string;
      };
      expect(data.code).toBe(payload.code);
      expect(data.message).toBe('success'); //static data in YAML file
    });

    it('does NOT catch worker 422 and returns an error message', async () => {
      const payload = { code: 422 };
      let data: {
        code: number;
        message: string;
        job_id: string;
      };
      try {
        await hotMesh.pubsub('def.test', payload);
      } catch (err) {
        data = err
        expect(data.code).toBe(payload.code);
        expect(data.message).toBe('invalid input');
        expect(data.job_id).not.toBeUndefined();

        await hotMesh.getState('def.test', data.job_id);
      }
    });
  });

  describe('Run With Catch', () => {
    it('should hot deploy version 2', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/def/v2/hotmesh.yaml');
      await hotMesh.activate('2');
    });

    it('routes worker 200 and returns a success message', async () => {
      const payload = { code: 200 };
      const result = await hotMesh.pubsub('def.test', payload);
      const data = result?.data as {
        code: number;
        message: string;
      };
      expect(data.code).toBe(payload.code);
      expect(data.message).toBe('success'); //static data in YAML file
    });
  });

  describe('Pending', () => {
    it('should hot deploy version 3', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/def/v3/hotmesh.yaml');
      await hotMesh.activate('3');
    });

    it('routes worker 202 and returns a success message', async () => {
      const payload = { code: 202 };
      const result = await hotMesh.pubsub('def.test', payload);
      const data = result?.data as {
        code: number;
        percentage: number;
        message: string;
      };
      expect(data.code).toBe(200);
      expect(data.percentage).toBe(99);
      expect(data.message).toBe('success');
    }, 10_000);
  });
});
