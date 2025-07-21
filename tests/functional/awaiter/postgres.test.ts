import { Client as Postgres } from 'pg';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { HMNS } from '../../../modules/key';
import { guid } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProviderNativeClient } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

describe('FUNCTIONAL | AWAIT (OR NOT) | Postgres', () => {
  const appConfig = { id: 'awaiter', version: '1' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE !== 'true') {
      postgresClient = (
        await PostgresConnection.connect(guid(), Postgres, postgres_options)
      ).getClient();
      await dropTables(postgresClient);
    }
    const config: HotMeshConfig = {
      appId: appConfig.id,
      namespace: HMNS,
      taskQueue: 'default',
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connection: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Postgres, options: postgres_options },
        },
      },
    };

    hotMesh = await HotMesh.init(config);
    await hotMesh.deploy('/app/tests/$setup/apps/awaiter/hotmesh.yaml');
    await hotMesh.activate(appConfig.version);
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Await (or NOT)', () => {
    it('should NOT await the spawned job response', async () => {
      //default value for the 'await' flag is undefined (which is true: parents always awaiit child jobs)
      const payload: { do_await?: boolean } = {};
      let job = await hotMesh.pubsub('awaiter.test', payload, null, 10_000);
      expect(job.data.child_job_id).toBeUndefined();
      expect(job.data.child_some_data).toBeTruthy();

      //this should have no impact. `true` and `undefined` are the same
      payload.do_await = true;
      job = await hotMesh.pubsub('awaiter.test', payload, null, 10_000);
      expect(job.data.child_job_id).toBeUndefined();
      expect(job.data.child_some_data).toBeTruthy();
    }, 20_000);

    it('should await the spawned job response', async () => {
      //this is the exception (not to wait); explicitly pass `false`
      //the YAML is configured to read this payload input value and then map
      //the 'await' field for the 'await' activity to the value of this boolean
      const payload: { do_await?: boolean } = { do_await: false };
      const job = await hotMesh.pubsub('awaiter.test', payload, null, 10_000);
      expect(job.data.child_job_id).toBeTruthy();
    }, 10_000);
  });
});
