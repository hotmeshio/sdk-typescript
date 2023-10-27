import { nanoid } from 'nanoid';
import Redis from 'ioredis';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';
import { JobOutput } from '../../../types/job';
import { sleepFor } from '../../../modules/utils';

describe('FUNCTIONAL | Signal', () => {
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  const appConfig = { id: 'signal' };
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
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy('/app/tests/$setup/apps/signal/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  });

  afterAll(async () => {
    await StreamSignaler.stopConsuming();
    await RedisConnection.disconnectAll();
  });

  describe('Signal All', () => {
    it('sends a signal to awaken all paused jobs', async () => {
      let isDone = false;

      await hotMesh.psub('signal.tested*', (topic: string, message: JobOutput) => {
        expect(topic).toBe('signal.tested');
        isDone = true;
      });

      let jobId: string;
      await hotMesh.psub('hook.tested*', (topic: string, message: JobOutput) => {
        expect(topic).toBe('hook.tested');
        expect(message.data.parent_job_id).toBe(jobId);
      });

      jobId = await hotMesh.pub('signal.test', {});
      while (!isDone) {
        await sleepFor(100);
      }

      await hotMesh.punsub('signal.tested.*');
      await hotMesh.punsub('hook.tested.*');
    }, 25_000);
  });
});
