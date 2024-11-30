import Redis from 'ioredis';
import { Client as Postgres } from 'pg';

import { HotMesh, HotMeshConfig } from '../../../index';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import {
  dropTables,
  ioredis_options as redis_options,
  postgres_options,
} from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../../types/provider';

describe('FUNCTIONAL | Interrupt', () => {
  const appConfig = { id: 'tree' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    // init Postgres and drop tables
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    // init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      redis_options,
    );
    redisConnection.getClient().flushdb();

    //init/activate HotMesh
    const config: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connection: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Redis, options: redis_options },
        },
      },
    };
    hotMesh = await HotMesh.init(config);
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Deploy and Activate', () => {
    it('deploys and activates', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/tree/v9/hotmesh.yaml');
      const isActivated = await hotMesh.activate('9');
      expect(isActivated).toBe(true);
    });
  });

  describe('Run Version', () => {
    it('should interrupt the job and return the current job state', async () => {
      const payload = { seed: 2, speed: 3, throw: false };
      const result = await hotMesh.pubsub('spring', payload, null, 1500);
      //subset of output data model for job as defined in YAML (/v9/hotmesh.yaml)
      const data = result?.data as {
        seed: number;
        speed: number;
        throw: boolean;
        height: number;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.throw).toBe(payload.throw);
      expect(data.height).toBe(payload.seed * payload.speed);
    }, 2_000);

    it('should interrupt the job and throw a 410 error', async () => {
      //throw an error instead (the interrupt activity in the YAML maps to this field)
      const payload = { seed: 2, speed: 3, throw: true };
      try {
        await hotMesh.pubsub('spring', payload, null, 1500);
      } catch (err) {
        expect(err.code).toBe(410);
      }
    }, 2_000);
  });

  describe('Deploy and Activate', () => {
    it('deploys and activates a new version additively and safely', async () => {
      await hotMesh.deploy('/app/tests/$setup/apps/tree/v10/hotmesh.yaml');
      const isActivated = await hotMesh.activate('10');
      expect(isActivated).toBe(true);
    });
  });

  describe('Run Version', () => {
    it('should interrupt another job and return the interrupted job state', async () => {
      //this is a somewhat complicated test:
      // 1) `job a` await activity spawns `job b` (and passes inputs along with workflowId to use)
      // 2) `job b` is interrupted by `job a` interrupt activity (it targets the workflowId of `job b`)
      //    (interrupt is configured to tell `job b` NOT to throw an error...just stop and return its current state)
      // 3) `job a` await activity receives final state from `job b`
      //    (the final state is the state at the time of interruption)
      const payload = {
        seed: 4,
        speed: 6,
        throw: false,
        workflowId: 'abcSuccess',
      };
      const result = await hotMesh.pubsub('winter', payload, null, 30_000);
      //subset of output data model for job as defined in YAML (/v10/hotmesh.yaml)
      const data = result?.data as {
        seed: number;
        speed: number;
        throw: boolean;
        height: number;
        sheer: string;
        shave?: string;
      };
      expect(data.seed).toBe(payload.seed);
      expect(data.speed).toBe(payload.speed);
      expect(data.throw).toBe(payload.throw);
      expect(data.height).toBe(payload.seed * payload.speed);
      //the 'sheer' activity runs IMMEDIATELY and will complete in time before the interrupt signal
      expect(data.sheer).not.toBeUndefined();
      //the 'shave' activity runs AFTER a sleep activity and will not run before the interrupt signal is received
      expect(data.shave).toBeUndefined();
    }, 35_000);

    it('should interrupt another job and re/throw a 410 error', async () => {
      //throw an error instead (the interrupt activity in the YAML maps to this field)
      //this is the same complicated test as above with interactions
      //between testa and testb...but this time we expect an error,
      //since testa interrupt activity sends signal with throw=true
      const payload = { seed: 6, speed: 9, throw: true, workflowId: 'abcFail' };
      try {
        await hotMesh.pubsub('winter', payload, null, 30_000);
      } catch (err) {
        //the winter job is rethrowing the 410 error from the morning job
        //the await activity in `flow a` could have been designed
        //to include a transtion to handle the 410 and then continue
        expect(err.code).toBe(410);
      }
    }, 35_000);
  });
});
