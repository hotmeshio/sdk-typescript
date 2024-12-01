import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { JobOutput } from '../../../types/job';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { ioredis_options } from '../../$setup/postgres';

describe('FUNCTIONAL | EMIT | IORedis', () => {
  const appConfig = { id: 'emit', version: '1' };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      ioredis_options,
    );
    redisConnection.getClient().flushdb();

    const config: HotMeshConfig = {
      appId: appConfig.id,
      namespace: HMNS,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        connection: {
          class: Redis,
          options: ioredis_options,
        },
      },

      workers: [
        {
          topic: 'emit.test.worker',
          connection: {
            class: Redis,
            options: ioredis_options,
          },
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
    await hotMesh.deploy('/app/tests/$setup/apps/emit/v1/hotmesh.yaml');
    await hotMesh.activate(appConfig.version);
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Emit Interim Job State', () => {
    it('should emit the interim job state', async () => {
      let jobId: string;
      const job_id = 'myjob123';
      let isDone = false;

      //subscribe to the 'emit.tested' topic
      await hotMesh.psub(
        'emit.tested*',
        (topic: string, message: JobOutput) => {
          //results are broadcast here
          expect(topic).toBe('emit.tested');
          expect(message.data.status).toBe('success');

          //two messages are published when emit is used;
          // the second message includes a 'done' property
          if (message.data.done) {
            isDone = true;
          }
        },
      );

      const payload = { job_id };

      //publish emit.test
      jobId = (await hotMesh.pub('emit.test', payload)) as string;

      //wait for the second message to be published (the one with 'done' property)
      while (!isDone) {
        await sleepFor(500);
      }

      //unsubscribe from the 'emit.tested' topic
      await hotMesh.punsub('emit.tested.*');
    }, 15_000);
  });
});
