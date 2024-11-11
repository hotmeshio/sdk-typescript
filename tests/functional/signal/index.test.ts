import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { guid, sleepFor } from '../../../modules/utils';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
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

  describe('Dynamic Activation Control', () => {
    it('it exits early and does not register for outside signals', async () => {
      let isDone = false;
      const jobId = 'xyz123';
      await hotMesh.sub(
        'signal.toggled.xyz123',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('signal.toggled');
          expect(message.data.done).toBe(false);
          expect(message.metadata.jid).toBe(jobId);
          isDone = true;
        },
      );

      await hotMesh.pub('signal.toggle', { jobId, statusThreshold: 1 });
      while (!isDone) {
        await sleepFor(100);
      }

      await hotMesh.unsub('signal.toggled.xyz123');
    });

    it('exits as expected after processing an outside signal', async () => {
      let isDone = false;
      const jobId = 'pdq123';
      const signal = { id: jobId, done: true, howdy: 'pardner' };
      //subscribe to the workflow output
      await hotMesh.sub(
        'signal.toggled.pdq123',
        (topic: string, message: JobOutput) => {
          expect(topic).toBe('signal.toggled');
          expect(message.data.done).toBe(signal.done);
          expect(message.data.howdy).toBe(signal.howdy);
          expect(message.metadata.jid).toBe(jobId);
          isDone = true;
        },
      );
      //kick off the test by publishing a message
      await hotMesh.pub('signal.toggle', { jobId });
      //sleep for a bit to make sure the job registers for signals and then goes to sleep
      await sleepFor(500);
      //send a signal to awaken the job
      await hotMesh.hook('waitForSignaler.doPleaseResume', signal);
      while (!isDone) {
        //loop until we know the job is complete
        await sleepFor(100);
      }
      //unsubscribe and cleanup
      await hotMesh.unsub('signal.toggled.pdq123');
    });
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
