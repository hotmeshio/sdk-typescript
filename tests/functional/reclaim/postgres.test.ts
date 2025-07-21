import { Client as Postgres } from 'pg';

import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { HotMesh, HotMeshConfig } from '../../../index';
import { MathHandler } from '../../../services/pipe/functions/math';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Reclaim', () => {
  const appConfig = { id: 'calc', version: '1' };
  let isSlow = true;
  let hotMesh: HotMesh;
  let hotMesh2: HotMesh;
  let postgresClient: ProviderNativeClient;

  //This is the common callback function used by workers during the test runs
  // The first call will hang for 6 seconds; subsequent calls will hang for 1 second
  // This is to simulate a slow/stalled/unresponsive worker being 'helped' by
  // the quorum of workers. When the latter calls complete, they' claim the
  // message out from under the first worker (the reclaim timeout is configured to
  // be short during testing (1500ms))
  const callback = async (
    streamData: StreamData,
  ): Promise<StreamDataResponse> => {
    const values = JSON.parse(streamData.data.values as string) as number[];
    const operation = streamData.data.operation as
      | 'add'
      | 'subtract'
      | 'multiply'
      | 'divide';
    const result = new MathHandler()[operation](values);

    if (isSlow) {
      isSlow = false;
      await sleepFor(6_000);
    } else {
      await sleepFor(1_000);
    }
    return {
      status: StreamStatus.SUCCESS,
      metadata: { ...streamData.metadata },
      data: { result },
    } as StreamDataResponse;
  };

  beforeAll(async () => {
    const hmshFactory = async (version: string, first = false) => {
      if (first) {
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
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          reclaimDelay: 1_500, //default 5_000
        },
        workers: [
          {
            topic: 'calculation.execute',
            connection: {
              class: Postgres,
              options: postgres_options,
            },
            reclaimDelay: 1_500, //default 60_000
            callback,
          },
        ],
      };
      const instance = await HotMesh.init(config);
      return instance;
    };
    hotMesh = await hmshFactory('1', true);
    hotMesh2 = await hmshFactory('2');
    await hotMesh.deploy('/app/tests/$setup/apps/calc/v1/hotmesh.yaml');
    await hotMesh.activate(appConfig.version);
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  beforeEach(() => {});

  describe('Claim failed tasks', () => {
    it('continues stalled jobs using xclaim', async () => {
      const payload = {
        operation: 'add',
        values: JSON.stringify([1, 2, 3, 4, 5]),
      };

      //Job 1 is processed immediately by worker 1 and will sleep for 6s.
      //Jobs 2 and 3 (which last 1s each) will be completed by the second worker.
      //At this point the the second worker will be bored and take the job (xclaim)
      //and complete it. The first worker will eventually resume after 6s, but it
      //won't matter as the job will have been completed by the second worker and
      //a warning will print to the log that the `too-late` response was ignored
      const jobId1 = await hotMesh.pub('calculate', { ...payload });
      let status1 = await hotMesh2.getStatus(jobId1 as string);
      const jobId2 = await hotMesh.pub('calculate', { ...payload });
      const jobId3 = await hotMesh.pub('calculate', { ...payload });
      expect(status1).toEqual(1);

      //as long as the other jobs are in process the first job will be 1
      while (
        await hotMesh.getStatus(jobId2 as string) !== 0 ||
        await hotMesh.getStatus(jobId3 as string) !== 0
      ) {
        status1 = await hotMesh2.getStatus(jobId1 as string);
        expect(status1).toEqual(1);
        await sleepFor(1000);
      }

      //give time for the free worker to claim the job
      await sleepFor(6_000);
      //verify that job 1 is completed after being reclaimed
      status1 = await hotMesh2.getStatus(jobId1 as string);
      expect(status1).toEqual(0);
    }, 20_000);
  });
});
