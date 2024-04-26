import Redis from 'ioredis';

import config from '../../$setup/config'
import { Durable } from '../../../services/durable';
import * as workflows from './src/workflows';
import * as childWorkflows from './child/workflows';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import { ClientService } from '../../../services/durable/client';
import { guid, sleepFor } from '../../../modules/utils';
import { HMNS, KeyService, KeyType } from '../../../modules/key';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | hook | `Workflow Promise.all proxyActivities`', () => {
  const namespace = 'staging';
  const prefix = 'bye-world-';
  let client: ClientService;
  let workflowGuid: string;
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(guid(), Redis, options);
    redisConnection.getClient().flushdb();
  });

  afterAll(async () => {
    await Durable.shutdown();
  }, 15_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Redis config', async () => {
        const connection = await Connection.connect({
          class: Redis,
          options,
        });
        expect(connection).toBeDefined();
        expect(connection.options).toBeDefined();
      });
    });
  });

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        client = new Client({ connection: { class: Redis, options }});
        workflowGuid = prefix + guid();

        const handle = await client.workflow.start({
          namespace,
          args: ['HookMesh'],
          taskQueue: 'hook-world',
          workflowName: 'example',
          workflowId: workflowGuid,
          expire: 600,
          //SEED the initial workflow state with data (this is
          //different than the 'args' input data which the workflow
          //receives as its first argument...this data is available
          //to the workflow via the 'search' object)
          //NOTE: data can also be updated during workflow execution
          search: {
            data: {
              fred: 'flintstone',
              barney: 'rubble',
            }
          }
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create a worker', async () => {
        const worker = await Worker.create({
          namespace,
          connection: { class: Redis, options },
          taskQueue: 'hook-world',
          workflow: workflows.example,
          //INDEX the search space; if the index doesn't exist, it will be created
          //(this is supported by Redis backends with the FT module enabled)
          search: {
            index: 'bye-bye',
            prefix: [prefix],
            schema: {
              custom1: {
                type: 'TEXT',
                sortable: true,
              },
              custom2: {
                type: 'NUMERIC', //or TAG
                sortable: true
              }
            }
          }
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create and run the CHILD workflow worker', async () => {
        //the main flow has an execChild command which will be serviced
        //by this worker
        const worker = await Worker.create({
          namespace,
          connection: { class: Redis, options },
          taskQueue: 'child-world',
          workflow: childWorkflows.childExample,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create a hook worker', async () => {
        const worker = await Worker.create({
          namespace,
          connection: { class: Redis, options },
          taskQueue: 'hook-world',
          workflow: workflows.exampleHook,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should spawn a hook and run the hook function', async () => {
        //sleep so the main thread fully executes and gets into a paused state
        //where it is awaiting a signal
        await sleepFor(2_500);

        //send a `hook` to spawn a hook thread attached to this workflow
        //the exampleHook function will be invoked in job context, allowing
        //it to read/write/augment shared job state with transactional integrity
        await client.workflow.hook({
          namespace,
          taskQueue: 'hook-world',
          workflowName: 'exampleHook',
          workflowId: workflowGuid,
          args: ['HotMeshHook'],
         });
      }, 10_000);        
    });
  });

  describe('WorkflowHandle', () => {
    describe('result', () => {
      it('should return the workflow execution result', async () => {
        //get the workflow handle and wait for the result
        const handle = await client.workflow.getHandle(
          'hook-world',
          workflows.example.name,
          workflowGuid,
          namespace
        );
        const result = await handle.result(true);
        expect(result).toEqual('Hello, HookMesh! - Goodbye, HookMesh!');
        const exported = await handle.export();
        expect(exported.status).not.toBeUndefined();
        expect(exported.data.fred).toBe('flintstone');
        expect(exported.state.data.done).toBe(true);

        //call the FT search module to locate the workflow via fuzzy search
        //NOTE: always include an underscore prefix before your search term (e.g., `_custom1`).
        //      HotMesh uses this to avoid collisions with reserved words
        const [count, ...results] = await client.workflow.search(
          'hook-world',
          workflows.example.name,
          namespace,
          'bye-bye',
          '@_custom1:durable'
        );
        expect(count).toEqual(1);
        const [id, ..._rest2] = results;

        const keyParams = { appId: namespace, jobId: workflowGuid }
        const expectedGuid = KeyService.mintKey(HMNS, KeyType.JOB_STATE, keyParams);
        expect(id).toEqual(expectedGuid);
        await sleepFor(5_000);
      }, 30_000);
    });
  });
});
