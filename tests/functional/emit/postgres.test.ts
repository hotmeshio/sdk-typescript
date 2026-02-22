import { describe, it, expect, beforeAll, afterAll, test } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { JobOutput } from '../../../types/job';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | EMIT | Postgres', () => {
  const appConfig = { id: 'emit', version: '1' };
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
      logLevel: HMSH_LOGLEVEL,

      engine: {
        connection: { class: Postgres, options: postgres_options },
      },

      workers: [
        {
          topic: 'emit.test.worker',
          connection: { class: Postgres, options: postgres_options },
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
      let isDone = false;
      const job_id = 'myjob123';

      //postgres does sub (not patterned-sub), so subscription must be specific
      await hotMesh.sub(
        `emit.tested.${job_id}`,
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

      //unsubscribe from the topic
      await hotMesh.unsub(`emit.tested.${job_id}`);
    }, 15_000);
  });
});
