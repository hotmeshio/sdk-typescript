import { nanoid } from 'nanoid';
import { APP_ID, APP_VERSION, DEFAULT_COEFFICIENT, SUBSCRIBES_TOPIC, getWorkflowYAML } from './factory';
import { WorkflowHandleService } from './handle';
import { HotMeshService as HotMesh } from '../hotmesh';
import {
  ClientConfig,
  Connection,
  WorkflowOptions } from '../../types/durable';
import { JobState } from '../../types/job';
import { KeyType } from '../../modules/key';

/*
Here is an example of how the methods in this file are used:

./client.ts

import { Durable } from '@hotmeshio/hotmesh';
import Redis from 'ioredis';
import { example } from './workflows';
import { nanoid } from 'nanoid';

async function run() {
  const connection = await Durable.Connection.connect({
    class: Redis,
    options: {
      host: 'localhost',
      port: 6379,
    },
  });

  const client = new Durable.Client({
    connection,
  });

  const handle = await client.workflow.start({
    args: ['HotMesh'],
    taskQueue: 'hello-world',
    workflowName: 'example',
    workflowId: 'workflow-' + nanoid(),
  });

  console.log(`Started workflow ${handle.workflowId}`);
  console.log(await handle.result());
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

*/

export class ClientService {

  connection: Connection;
  options: WorkflowOptions;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  getHotMeshClient = async (worflowTopic: string) => {
    //NOTE: every unique topic inits a new engine
    if (ClientService.instances.has(worflowTopic)) {
      return await ClientService.instances.get(worflowTopic);
    }

    const hotMeshClient = HotMesh.init({
      appId: APP_ID,
      engine: {
        redis: {
          class: this.connection.class,
          options: this.connection.options,
        }
      }
    });
    ClientService.instances.set(worflowTopic, hotMeshClient);

    //since the YAML topic is dynamic, it MUST be manually created before use
    const store = (await hotMeshClient).engine.store;
    const params = { appId: APP_ID, topic: worflowTopic };
    const streamKey = store.mintKey(KeyType.STREAMS, params);
    try {
      await store.xgroup('CREATE', streamKey, 'WORKER', '$', 'MKSTREAM');
    } catch (err) {
      //ignore if already exists
    }
    await this.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  }

  workflow = {
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.taskQueue;
      const workflowName = options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //topic is concat of taskQueue and workflowName
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(workflowTopic);
      const payload = {
        arguments: [...options.args],
        workflowId: options.workflowId || nanoid(),
        workflowTopic: workflowTopic,
        backoffCoefficient: options.config?.backoffCoefficient || DEFAULT_COEFFICIENT,
      }
      const context = { metadata: { trc, spn }, data: {}};
      const jobId = await hotMeshClient.pub(
        SUBSCRIBES_TOPIC,
        payload,
        context as JobState);
      return new WorkflowHandleService(hotMeshClient, workflowTopic, jobId);
    },

    signal: async (signalId: string, data: Record<any, any>): Promise<string> => {
      return await (await this.getHotMeshClient('durable.wfs.signal')).hook('durable.wfs.signal', { id: signalId, data });
    }
  }

  async activateWorkflow(hotMesh: HotMesh, appId = APP_ID, version = APP_VERSION): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', { error });
        throw error;
      }
    } else if(app && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', { error});
        throw error;
      }
    }
  }

  static async shutdown(): Promise<void> {
    for (const [key, value] of ClientService.instances) {
      const hotMesh = await value;
      await hotMesh.stop();
    }
  }
}
