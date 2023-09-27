import { WorkflowHandleService } from "./handle";
import { HotMeshService as HotMesh } from "../hotmesh";
import { ClientConfig, Connection, WorkflowOptions } from "../../types/durable";
import { getWorkflowYAML } from "./factory";
import { JobState } from "../../types/job";

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

  getHotMesh = async (worflowTopic: string) => {
    if (ClientService.instances.has(worflowTopic)) {
      return await ClientService.instances.get(worflowTopic);
    }

    const hotMesh = HotMesh.init({
      appId: worflowTopic,
      engine: {
        redis: {
          class: this.connection.class,
          options: this.connection.options,
        }
      }
    });
    ClientService.instances.set(worflowTopic, hotMesh);
    await this.activateWorkflow(await hotMesh, worflowTopic);
    return hotMesh;
  }

  workflow = {
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.taskQueue;
      const workflowName = options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMesh = await this.getHotMesh(workflowTopic);
      const payload = {
        arguments: [...options.args],
        workflowId: options.workflowId,
      }
      const context = { metadata: { trc, spn }, data: {}};
      const jobId = await hotMesh.pub(workflowTopic, payload, context as JobState);
      return new WorkflowHandleService(hotMesh, workflowTopic, jobId);
    },
  };

  async activateWorkflow(hotMesh: HotMesh, workflowTopic: string): Promise<void> {
    const version = '1';
    const app = await hotMesh.engine.store.getApp(workflowTopic);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      try {
        await hotMesh.deploy(getWorkflowYAML(workflowTopic, version));
        await hotMesh.activate(version);
      } catch (err) {
        hotMesh.engine.logger.error('durable-client-workflow-activation-err', err);
        throw err;
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
