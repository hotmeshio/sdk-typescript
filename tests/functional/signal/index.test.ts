import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { guid, sleepFor } from '../../../modules/utils';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { JobOutput } from '../../../types/job';
import config from '../../$setup/config';

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
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      options,
    );
    redisConnection.getClient().flushdb();

    //init HotMesh
    const hmshConfig: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        redis: { class: Redis, options },
      },
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy('/app/tests/$setup/apps/signal/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Signal All', () => {
    it('sends a signal to awaken all paused jobs', async () => {
      let isDone = false;

      await hotMesh.psub(
        'signal.tested*',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('signal.tested');
          isDone = true;
        },
      );

      let jobId: string;
      await hotMesh.sub(
        'hook.tested.abc123',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('hook.tested');
          expect(message.data.parent_job_id).toBe(jobId);
        },
      );

      jobId = await hotMesh.pub('signal.test', { child_flow_id: 'abc123' });
      while (!isDone) {
        await sleepFor(100);
      }

      await hotMesh.punsub('signal.tested*');
      await hotMesh.unsub('hook.tested.abc123');
    }, 25_000);
  });

  describe('Signal One', () => {
    it('sends a signal to awaken one paused job', async () => {
      let isDone = false;

      await hotMesh.psub(
        'signal.one.tested*',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('signal.one.tested');
          isDone = true;
        },
      );

      let jobId: string;
      await hotMesh.psub(
        'hook.tested*',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('hook.tested');
          expect(message.data.parent_job_id).toBe(jobId);
        },
      );

      jobId = await hotMesh.pub('signal.one.test', { child_flow_id: 'xyz456' });
      while (!isDone) {
        await sleepFor(100);
      }

      await hotMesh.punsub('signal.one.tested*');
      await hotMesh.punsub('hook.tested*');
    }, 25_000);
  });
});
