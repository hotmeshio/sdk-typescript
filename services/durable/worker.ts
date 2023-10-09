import { asyncLocalStorage } from './asyncLocalStorage';
import { HotMeshService as HotMesh } from '../hotmesh';
import { RedisClass, RedisOptions } from '../../types/redis';
import { StreamData, StreamDataResponse, StreamStatus } from '../../types/stream';
import { ActivityDataType, Connection, Registry, WorkerConfig, WorkflowDataType } from "../../types/durable";
import { getWorkflowYAML, getActivityYAML } from './factory';

/*
Here is an example of how the methods in this file are used:

./worker.ts

import { Durable: { NativeConnection, Worker } } from '@hotmeshio/hotmesh';
import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`

import * as activities from './activities';

async function run() {
  const connection = await NativeConnection.connect({
    class: Redis,
    options: {
      host: 'localhost',
      port: 6379,
    },
  });
  const worker = await Worker.create({
    connection,
    namespace: 'default',
    taskQueue: 'hello-world',
    workflowsPath: require.resolve('./workflows'),
    activities,
  });
  await worker.run();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
*/

export class WorkerService {
  static activityRegistry: Registry = {}; //user's activities
  static connection: Connection;
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  workflowRunner: HotMesh;

  static getHotMesh = async (worflowTopic: string) => {
    if (WorkerService.instances.has(worflowTopic)) {
      return await WorkerService.instances.get(worflowTopic);
    }
    const hotMesh = HotMesh.init({
      appId: worflowTopic,
      engine: { redis: { ...WorkerService.connection } }
    });
    WorkerService.instances.set(worflowTopic, hotMesh);
    await WorkerService.activateWorkflow(await hotMesh, worflowTopic, getWorkflowYAML);
    return hotMesh;
  }

  static async activateWorkflow(hotMesh: HotMesh, topic: string, factory: Function) {
    const version = '1';
    const app = await hotMesh.engine.store.getApp(topic);
    const appVersion = app?.version;
    if(!appVersion) {
      try {
        await hotMesh.deploy(factory(topic, version));
        await hotMesh.activate(version);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-deploy-activate-err', err);
        throw err;
      }
    } else if(app && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-activate-err', err);
        throw err;
      }
    }
  }

  /**
   * The `worker` calls `registerActivities` immediately BEFORE
   * dynamically importing the user's workflow module. That file
   * contains a call, `proxyActivities`, which needs this info.
   * 
   * NOTE: The `worker` and `client` both call `proxyActivities`,
   * as a natural result of importing worflows.ts. However,
   * because the worker imports the workflows dynamically AFTER
   * the activities are loaded, there will be items in the registry,
   * allowing proxyActivities to succeed.
   */
  static registerActivities<ACT>(activities: ACT): Registry  {
    if (typeof activities === 'function') {
      WorkerService.activityRegistry[activities.name] = activities as Function;
    } else {
      Object.keys(activities).forEach(key => {
        WorkerService.activityRegistry[activities[key].name] = (activities as any)[key] as Function;
      });
    }
    return WorkerService.activityRegistry;
  }

  static async create(config: WorkerConfig) {
    //always call `registerActivities` before `import`
    WorkerService.connection = config.connection;
    WorkerService.registerActivities<typeof config.activities>(config.activities);
    const workflow = await import(config.workflowsPath);
    const [workflowFunctionName, workflowFunction] = WorkerService.resolveWorkflowTarget(workflow);
    const baseTopic = `${config.taskQueue}-${workflowFunctionName}`;
    const activityTopic = `${baseTopic}-activity`;
    const workflowTopic = `${baseTopic}`;

    //initialize supporting workflows
    const worker = new WorkerService();
    const activityRunner = await worker.initActivityWorkflow(config, activityTopic);
    await WorkerService.activateWorkflow(activityRunner, activityTopic, getActivityYAML);
    worker.workflowRunner = await worker.initWorkerWorkflow(config, workflowTopic, workflowFunction);
    await WorkerService.activateWorkflow(worker.workflowRunner, workflowTopic, getWorkflowYAML);
    return worker;
  }

  static resolveWorkflowTarget(workflow: object | Function): [string, Function] {
    let workflowFunction: Function;
    if (typeof workflow === 'function') {
      workflowFunction = workflow;
    } else {
      const workflowFunctionNames = Object.keys(workflow);
      workflowFunction = workflow[workflowFunctionNames[workflowFunctionNames.length - 1]];
      return WorkerService.resolveWorkflowTarget(workflowFunction);
    }
    return [workflowFunction.name, workflowFunction];
  }

  async run() {
    if (this.workflowRunner) {
      this.workflowRunner.engine.logger.info('WorkerService is running');
    } else {
      console.log('WorkerService is running');
    }
  }

  async initActivityWorkflow(config: WorkerConfig, activityTopic: string): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions
    };
    const hmshInstance = await HotMesh.init({
      appId: activityTopic,
      engine: { redis: redisConfig },
      workers: [
        { topic: activityTopic,
          redis: redisConfig,
          callback: this.wrapActivityFunctions().bind(this)
        }
      ]
    });
    WorkerService.instances.set(activityTopic, hmshInstance);
    return hmshInstance;
  }

  wrapActivityFunctions(): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      try {
        //always run the activity function when instructed; return the response
        const activityInput = data.data as unknown as ActivityDataType;
        const activityName = activityInput.activityName;
        const activityFunction = WorkerService.activityRegistry[activityName];
        const pojoResponse = await activityFunction.apply(this, activityInput.arguments);

        return {
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: pojoResponse }
        };
      } catch (err) {
        console.error(err);
        //todo (make retry configurable)
        return {
          status: StreamStatus.PENDING,
          metadata: { ...data.metadata },
          data: { error: err }
        } as StreamDataResponse;
      }
    }
  }

  async activateActivityWorkflow(hotMesh: HotMesh, activityTopic: string) {
    const version = '1';
    const app = await hotMesh.engine.store.getApp(activityTopic);
    const appVersion = app?.version as unknown as number;
    if(isNaN(appVersion)) {
      try {
        await hotMesh.deploy(getActivityYAML(activityTopic, version));
        await hotMesh.activate(version);
      } catch (err) {
        console.log('durable-worker-activity-deploy-activate-error', err);
        throw err;
      }
    } else if(app && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-activity-activate-err', err);
        throw err;
      }
    }
  }

  async initWorkerWorkflow(config: WorkerConfig, workflowTopic: string, workflowFunction: Function): Promise<HotMesh> {
    const redisConfig = {
      class: config.connection.class as RedisClass,
      options: config.connection.options as RedisOptions
    };
    const hmshInstance = await HotMesh.init({
      appId: workflowTopic,
      engine: { redis: redisConfig },
      workers: [
        { topic: workflowTopic,
          redis: redisConfig,
          callback: this.wrapWorkflowFunction(workflowFunction, workflowTopic).bind(this)
        }
      ]
    });
    WorkerService.instances.set(workflowTopic, hmshInstance);
    return hmshInstance;
  }

  static Context = {
    info: () => {
      return {
        workflowId: '',
        workflowTopic: '',
      }
    },
  };

  wrapWorkflowFunction(workflowFunction: Function, workflowTopic: string): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      try {
        //incoming data payload has arguments and workflowId
        const workflowInput = data.data as unknown as WorkflowDataType;
        const context = new Map();
        const counter = { counter: 0 };
        context.set('counter', counter);
        context.set('workflowId', workflowInput.workflowId);
        context.set('workflowTopic', workflowTopic);
        context.set('workflowName', workflowTopic.split('-').pop());
        context.set('workflowTrace', data.metadata.trc);
        context.set('workflowSpan', data.metadata.spn);
        const workflowResponse = await asyncLocalStorage.run(context, async () => {
          return await workflowFunction.apply(this, workflowInput.arguments);
        });

        return {
          code: 200,
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: workflowResponse }
        };
      } catch (err) {
        //todo: (retryable error types)
        return {
          code: 500,
          status: StreamStatus.PENDING,
          metadata: { ...data.metadata },
          data: { error: err }
        } as StreamDataResponse;
      }
    }
  }

  static async shutdown(): Promise<void> {
    for (const [key, value] of WorkerService.instances) {
      const hotMesh = await value;
      await hotMesh.stop();
    }
  }
}
