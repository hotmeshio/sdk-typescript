import { nanoid } from 'nanoid';
import Redis from 'ioredis';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { StreamSignaler } from '../../../services/signaler/stream';
import { JobOutput } from '../../../types/job';
import { sleepFor } from '../../../modules/utils';

describe('FUNCTIONAL | Hook', () => {
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  const appConfig = { id: 'hook' };
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
    await hotMesh.deploy('/app/tests/$setup/apps/hook/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  }, 15_000);

  afterAll(async () => {
    await StreamSignaler.stopConsuming();
    await RedisConnection.disconnectAll();
  });

  describe('Hook All', () => {
    it('sleeps until a `hookAll` signal', async () => {
      const parent_job_id = nanoid();
      const id = nanoid();
      let isDone = false;
      let shouldResume = false;

      //subscribe to the 'hook.tested' topic
      await hotMesh.psub('hook.tested*', (topic: string, message: JobOutput) => {
        //results are broadcast here
        expect(topic).toBe('hook.tested');
        expect(message.data.parent_job_id).toBe(parent_job_id);
        expect(message.data.job_id).toBe(id);

        //two messages will be published, because the trigger has an 'emit' flag in the YAML
        //the first publication is just the emit signal
        // the second message includes a 'done' property and is the final job result
        if (message.data.done) {
          isDone = true;
        } else {
          shouldResume = true;
        }
      });

      const payload = { parent_job_id: parent_job_id, job_id: id };
      await hotMesh.pub('hook.test', payload);
      while (!shouldResume) {
        await sleepFor(100);
      }

      //reconstruct the jobKey
      //according to the YAML, the jobKey is generated using the `parent_job_id`
      //refer to the YAML to see how the job `key` is generated using the parent_job_id
      const jobKeyQuery = {
        data: { parent_job_id: parent_job_id },
        scrub: true, //self-clean the indexes upon use (this is a single-use index)
      };

      //jobKeys can contain more than one indexed list of job ids (the segmentation is quite robus)
      //target just the index called 'parent_job_id' that matches.
      //refer to the YAML to see this index listed (parent_job_id)
      const indexQueryFacets = [`parent_job_id:${parent_job_id}`];

      //hookAll will resume all paused jobs that match the query
      //NOTE: 'targets' is the REDIS address of the index that contains all jobs that match
      const targets = await hotMesh.hookAll('hook.resume', { done: true }, jobKeyQuery, indexQueryFacets);

      //NOTE: in the YAML, `granularity` is set to `infinity`, so all jobs are listed in a single index (not a time series index)
      //      This is why targets.length is `1`; otherwise, `indexQueryFacets` would have included a time range
      //      to target only those jobs were indexed (created) during the desired time slice window
      expect(targets.length).toBe(1);
      while (!isDone) {
        await sleepFor(100);
      }
      await hotMesh.punsub('hook.tested.*');
    }, 15_000);
  });
});
