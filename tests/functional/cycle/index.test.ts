import Redis from 'ioredis';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamData, StreamDataResponse } from '../../../types/stream';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';

describe('FUNCTIONAL | Activity Cycles', () => {
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  const appConfig = { id: 'cycle' };
  let hotMesh: HotMesh;
  let counter = 0;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(guid(), Redis, options);
    redisConnection.getClient().flushdb();

    //init HotMesh
    const hmshConfig: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        redis: { class: Redis, options }
      },

      workers: [

        //this worker will return a 200 status code; the yaml
        //model is cofigured to cycle as long as `counter < 5`
        //this worker runs as part of flow v1
        {
          topic: 'cycle.count',
          redis: { class: Redis, options },
          callback: async (streamData: StreamData): Promise<StreamDataResponse> => {
            return {
              metadata: { ...streamData.metadata },
              data: {
                counter: ++counter
              }
            } as StreamDataResponse;
          }
        },

        //this worker will return a 500 status code for 5 times and then a 200
        //the yaml model dictates that the flow should cycle as long as
        //code is 500;
        //this worker runs as part of flow v2
        {
          topic: 'cycle.err',
          redis: { class: Redis, options },
          callback: async (streamData: StreamData): Promise<StreamDataResponse> => {
            counter++;
            return {
              metadata: { ...streamData.metadata },
              code: counter == 5 ? 200 : 500,
              data: {
                counter: counter
              }
            } as StreamDataResponse;
          }
        },
      ]
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
    }, 10_000);
  });
});
