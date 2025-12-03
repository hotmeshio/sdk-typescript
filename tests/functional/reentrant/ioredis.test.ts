import Redis from 'ioredis';

import { HotMesh, HotMeshConfig } from '../../../index';
import {
  HMSH_CODE_MEMFLOW_ALL,
  HMSH_CODE_MEMFLOW_CHILD,
  HMSH_CODE_MEMFLOW_PROXY,
  HMSH_CODE_MEMFLOW_SLEEP,
  HMSH_CODE_MEMFLOW_WAIT,
  HMSH_LOGLEVEL,
} from '../../../modules/enums';
import {
  MemFlowChildError,
  MemFlowFatalError,
  MemFlowMaxedError,
  MemFlowProxyError,
  MemFlowSleepError,
  MemFlowTimeoutError,
  MemFlowWaitForAllError,
  MemFlowWaitForError,
} from '../../../modules/errors';
import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import config from '../../$setup/config';
import { RedisConnection } from '../../../services/connector/providers/ioredis';
import {
  JobOutput,
  StringAnyType,
  StreamData,
  StreamDataResponse,
  StreamStatus,
  WorkflowOptions,
} from '../../../types';

describe('FUNCTIONAL | MEMFLOW | IORedis', () => {
  const appConfig = { id: 'memflow', version: '1' };
  let activityErrorCounter = 0;
  let workflowId = 'memflow';
  const originJobId = null;
  const workflowTopic = 'childWorld-childWorld';
  const activityTopic = `${workflowTopic}-activity`;
  const activityName = 'helloWorld';
  const workflowName = 'childWorld';
  const entityName = 'childWorld';
  const collatorSignalTopic = 'memflow.wfs.signal';
  const signalId = 'abcdefg';

  type SignalResponseType = { response: string; id: string };
  type ProxyResponseType = { response: string; errorCount: number };
  type ChildResponseType = { response: string };

  const signalResponse: SignalResponseType = {
    response: 'signal response',
    id: '',
  };
  const proxyResponse: ProxyResponseType = {
    response: 'proxy response',
    errorCount: activityErrorCounter,
  };
  const childResponse: ChildResponseType = { response: 'child response' };

  let shouldSleep = false;
  let shouldWait = false;
  let shouldProxy = false;
  let shouldChild = false;
  let shouldPromise = false;

  let awaitChild = true;

  const replay: StringAnyType = {}; //simulate replay history

  //the list of interruptions (the reason...what needs to be resolved before reentering)
  const interruptionRegistry: any[] = [];
  const interruptionList: any[] = [];
  let counter = 0;

  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  let hotMesh: HotMesh;

  const xHandleError = (err: any, interruptionList: any[]) => {
    if (err instanceof MemFlowSleepError) {
      interruptionList.push({
        code: err.code,
        message: JSON.stringify({
          duration: err.duration,
          index: err.index,
          workflowDimension: err.workflowDimension,
        }),
        duration: err.duration,
        index: err.index,
        workflowDimension: err.workflowDimension,
      });
    } else if (err instanceof MemFlowWaitForError) {
      interruptionList.push({
        code: err.code,
        message: JSON.stringify(err.signalId),
        signalId: err.signalId,
      });
    } else if (err instanceof MemFlowProxyError) {
      interruptionList.push({
        arguments: err.arguments,
        code: err.code,
        workflowDimension: err.workflowDimension,
        index: err.index,
        originJobId: err.originJobId,
        parentWorkflowId: err.parentWorkflowId,
        workflowId: err.workflowId,
        workflowTopic: err.workflowTopic,
        activityName: err.activityName,
        backoffCoefficient: err.backoffCoefficient,
        maximumAttempts: err.maximumAttempts,
        maximumInterval: err.maximumInterval,
      });
    } else if (err instanceof MemFlowChildError) {
      interruptionList.push({
        arguments: err.arguments,
        code: err.code,
        workflowDimension: err.workflowDimension,
        index: err.index,
        originJobId: err.originJobId,
        parentWorkflowId: err.parentWorkflowId,
        workflowId: err.workflowId,
        workflowTopic: err.workflowTopic,
        await: err.await,
      });
    } else if (err instanceof MemFlowWaitForAllError) {
      interruptionList.push({
        items: err.items,
        code: err.code,
        workflowDimension: err.workflowDimension,
        index: err.index,
        size: err.size,
        originJobId: err.originJobId,
        parentWorkflowId: err.parentWorkflowId,
        workflowId: err.workflowId,
        workflowTopic: err.workflowTopic,
      });
    } else {
      console.error('Retrying forever!!!');
    }
  };

  //588: HMSH_CODE_MEMFLOW_SLEEP
  const xSleepFor = async (millis: string): Promise<number> => {
    const seconds = Number(millis) / 1000;
    if (shouldSleep) {
      shouldSleep = false;
      const execIndex = counter++;
      interruptionRegistry.push({
        code: HMSH_CODE_MEMFLOW_SLEEP,
        duration: seconds,
        index: execIndex,
        workflowDimension: '',
      });
      await sleepFor(0);
      throw new MemFlowSleepError({
        workflowId,
        duration: seconds,
        index: execIndex,
        workflowDimension: '',
      });
    }
    return seconds;
  };

  //590: HMSH_CODE_MEMFLOW_CHILD
  const xChild = async <T>(options: WorkflowOptions): Promise<T> => {
    if (shouldChild) {
      //SYNC
      shouldChild = false;
      const execIndex = counter++;
      const workflowDimension = '';
      const entityOrEmptyString = options.entity ?? '';
      const childJobId =
        options.workflowId ??
        `${entityOrEmptyString}-${workflowId}-$${options.entity ?? options.workflowName}${workflowDimension}-${execIndex}`;
      const taskQueueName = options.entity ?? options.taskQueue;
      const workflowName = options.entity ?? options.workflowName;
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      interruptionRegistry.push({
        code: HMSH_CODE_MEMFLOW_CHILD,
        arguments: options.args,
        workflowDimension: workflowDimension,
        index: execIndex,
        originJobId: originJobId || workflowId,
        parentWorkflowId: workflowId,
        workflowId: childJobId,
        workflowTopic,
        await: options.await,
      });

      //ASYNC
      await sleepFor(0);
      throw new MemFlowChildError({
        arguments: options.args,
        workflowDimension: workflowDimension,
        index: execIndex,
        originJobId: originJobId || workflowId,
        parentWorkflowId: workflowId,
        workflowId: childJobId,
        workflowTopic,
        await: options.await,
      });
    }
    return childResponse as T;
  };

  //591: HMSH_CODE_MEMFLOW_PROXY
  const xProxyActivity = async <T>(...args: any[]): Promise<T> => {
    if (shouldProxy) {
      //SYNC
      shouldProxy = false;
      const execIndex = counter++;
      const workflowDimension = '';
      const activityJobId = `-${workflowId}-$${activityName}${workflowDimension}-${execIndex}`;
      const activityTopic = `${workflowName}-${workflowName}-activity`;
      interruptionRegistry.push({
        arguments: args,
        code: HMSH_CODE_MEMFLOW_PROXY,
        workflowDimension: workflowDimension,
        index: execIndex,
        originJobId: originJobId || workflowId,
        parentWorkflowId: workflowId,
        workflowId: activityJobId,
        workflowTopic: activityTopic,
      });

      //ASYNC
      await sleepFor(0);
      throw new MemFlowProxyError({
        activityName,
        arguments: args,
        workflowDimension: workflowDimension,
        index: execIndex,
        originJobId: originJobId || workflowId,
        parentWorkflowId: workflowId,
        workflowId: activityJobId,
        workflowTopic: activityTopic,
      });
    }
    return {
      response: { ...proxyResponse, errorCount: activityErrorCounter },
    } as T;
  };

  //595: HMSH_CODE_MEMFLOW_WAIT
  const xWaitFor = async <T>(key: string): Promise<T> => {
    if (shouldWait) {
      //SYNC
      shouldWait = false;
      const execIndex = counter++;
      interruptionRegistry.push({
        code: HMSH_CODE_MEMFLOW_WAIT,
        signalId: key,
        index: execIndex,
        workflowDimension: '',
      });
      //ASYNC
      await sleepFor(0);
      throw new MemFlowWaitForError({
        signalId: key,
        index: execIndex,
        workflowDimension: '',
        workflowId,
      });
    }
    return { ...signalResponse, id: key } as T;
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      guid(),
      Redis,
      options,
    );
    redisConnection.getClient().flushdb();

    const config: HotMeshConfig = {
      appId: appConfig.id,
      namespace: HMNS,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        connection: { class: Redis, options },
      },

      workers: [
        {
          topic: workflowTopic,
          connection: { class: Redis, options },

          callback: async (data: StreamData): Promise<StreamDataResponse> => {
            let sleepData: number | null = null;
            let signalData: { response: string; id: string } | null = null;
            let proxyData: { response: string } | null = null;
            let childData: { response: string } | null = null;
            let allData: any[] = [];

            try {
              //sleep
              sleepData = await xSleepFor('2000');

              //wait
              signalData = await xWaitFor<SignalResponseType>(signalId);

              //proxy
              proxyData = await xProxyActivity<ProxyResponseType>(
                ...(data.data.arguments as any[]),
              );

              //child
              childData = await xChild<ChildResponseType>({
                args: ['f', { g: 7 }],
                await: awaitChild, // if this is false, use startChild, otherwise execChild
                workflowName, // `entity` clobbers this
                entity: entityName, // hotmesh syntax (use taskQueue in memflow function)
              });

              //all
              if (shouldPromise) {
                //fetch the data
                shouldPromise = false;
                shouldSleep = true;
                shouldWait = true;
                shouldProxy = true;
                shouldChild = true;
                allData = await Promise.all([
                  xSleepFor('3000'),
                  xWaitFor<SignalResponseType>(`${signalId}hi`),
                  xProxyActivity<ProxyResponseType>(
                    ...(data.data.arguments as any[]),
                  ),
                  //standard memflows format with taskQueue and workflowName
                  //this is standard format (combining workflowName and taskQueue)
                  //xstream address is always taskQueue + workflowName
                  xChild<ChildResponseType>({
                    args: ['f', { g: 7 }],
                    workflowName,
                    taskQueue: entityName,
                  }),
                ]);
              } else {
                //data was fetched or not testing...regardless, just return the sync test data
                shouldSleep = false;
                shouldWait = false;
                shouldProxy = false;
                shouldChild = false;
                allData = [
                  await xSleepFor('3000'),
                  await xWaitFor<SignalResponseType>(`${signalId}hi`),
                  await xProxyActivity<ProxyResponseType>(
                    ...(data.data.arguments as any[]),
                  ),
                  await xChild<ChildResponseType>({
                    args: ['f', { g: 7 }],
                    workflowName,
                    taskQueue: entityName,
                  }),
                ];
              }
            } catch (err) {
              xHandleError(err, interruptionList);
            }

            if (interruptionRegistry.length > 0) {
              let errData: Record<string, unknown>;
              const workflowDimension = '';
              const execIndex = counter++;
              if (
                interruptionRegistry.length === 1 &&
                interruptionRegistry[0].code !== HMSH_CODE_MEMFLOW_WAIT
              ) {
                //signals are the exception (HMSH_CODE_MEMFLOW_WAIT). They are bundled as an array
                errData = interruptionList[0];
              } else {
                const collatorFlowId = `-${workflowId}-$${workflowDimension}-$${execIndex}`;
                errData = {
                  code: HMSH_CODE_MEMFLOW_ALL,
                  items: [...interruptionRegistry],
                  size: interruptionRegistry.length,
                  workflowDimension: '',
                  index: execIndex,
                  originJobId: originJobId || workflowId,
                  parentWorkflowId: workflowId,
                  workflowId: collatorFlowId,
                  workflowTopic: activityTopic,
                };
              }
              const interruptResponse = {
                status: StreamStatus.SUCCESS,
                code: errData.code,
                metadata: { ...data.metadata },
                data: { ...errData },
              } as StreamDataResponse;
              interruptionList.length = 0;
              interruptionRegistry.length = 0;
              return interruptResponse;
            }
            interruptionList.length = 0;
            interruptionRegistry.length = 0;

            //final return (function is over)
            return {
              status: StreamStatus.SUCCESS,
              metadata: { ...data.metadata },
              data: {
                response: {
                  sleepResponse: sleepData as number,
                  signalResponse: signalData,
                  proxyResponse: proxyData,
                  childResponse: childData,
                  allResponse: allData,
                },
                done: true,
              },
            } as StreamDataResponse;
          },
        },

        {
          topic: activityTopic,
          connection: { class: Redis, options },

          callback: async (data: StreamData): Promise<StreamDataResponse> => {
            try {
              if (activityErrorCounter++ < 2) {
                throw new Error(
                  `my test activity error message [${activityErrorCounter}]`,
                );
              }
              return {
                status: StreamStatus.SUCCESS,
                metadata: { ...data.metadata },
                data: {
                  response: {
                    ...proxyResponse,
                    errorCount: activityErrorCounter,
                  },
                },
              } as StreamDataResponse;
            } catch (error) {
              if (
                error instanceof MemFlowFatalError ||
                error instanceof MemFlowMaxedError ||
                error instanceof MemFlowTimeoutError
              ) {
                return {
                  status: StreamStatus.SUCCESS, //todo: might be better to return .ERROR status
                  code: error.code, //these errors are fatal
                  metadata: { ...data.metadata },
                  data: {
                    status: 'error',
                    code: 'error.code',
                    message: error.message,
                  },
                } as StreamDataResponse;
              } else {
                return {
                  status: StreamStatus.SUCCESS,
                  code: 599, //retry forever
                  metadata: { ...data.metadata },
                  data: {
                    status: 'error',
                    code: 599,
                    message: error.message,
                    stack: error.stack,
                    name: error.name,
                  },
                } as StreamDataResponse;
              }
            }
          },
        },
      ],
    };
    hotMesh = await HotMesh.init(config);
    await hotMesh.deploy('/app/tests/$setup/apps/memflow/v1/hotmesh.yaml');
    await hotMesh.activate(appConfig.version);
  }, 10_000);

  afterAll(async () => {
    //await sleepFor(10_000);
    hotMesh.stop();
    await HotMesh.stop();
  }, 15_000);

  describe('MemFlow Function State', () => {
    afterEach(async () => {
      interruptionRegistry.length = 0;
      interruptionList.length = 0;
      activityErrorCounter = 0;
      awaitChild = true;
    });

    it('should COMPLETE', async () => {
      //no interruptions...just test the persistent function pipes
      workflowId = 'reentrant200';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['', { b: 2 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      const response = await hotMesh.pubsub('memflow.execute', payload);
      expect(response.data.done).toBe(true);
    });

    it('should SLEEP, exit, reenter, complete', async () => {
      shouldSleep = true;
      workflowId = 'reentrant588';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['a', { b: 2 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      const response = await hotMesh.pubsub(
        'memflow.execute',
        payload,
        null,
        10_000,
      );
      expect(response.data.done).toBe(true);
      expect(shouldSleep).toBe(false);
    }, 10_000);

    it('should WAIT, exit, reenter, complete', async () => {
      shouldWait = true;
      workflowId = 'reentrant595';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['b', { c: 3 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      const jobId = await hotMesh.pub('memflow.execute', payload);
      await sleepFor(2_500);

      await hotMesh.hook(collatorSignalTopic, {
        id: signalId,
        data: { ...signalResponse, id: signalId },
      });

      //loop until the change percolates up and the job is complete
      let response: JobOutput;
      do {
        await sleepFor(1_000);
        response = await hotMesh.getState('memflow.execute', jobId);
      } while (!response.data.done);
      expect(shouldWait).toBe(false);
    }, 10_000);

    it('should PROXY, exit, reenter, complete', async () => {
      shouldProxy = true;
      workflowId = 'reentrant591';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['c', { d: 4 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      //activity is designed to throw three errors before completing successfully
      const response = await hotMesh.pubsub(
        'memflow.execute',
        payload,
        null,
        15_000,
      );
      expect(response.data.done).toBe(true);
      expect(shouldProxy).toBe(false);
    }, 20_000);

    it('should START CHILD, exit, reenter, complete', async () => {
      shouldChild = true;
      awaitChild = false;
      workflowId = 'reentrant590X';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['d', { e: 5 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      const response = await hotMesh.pubsub(
        'memflow.execute',
        payload,
        null,
        2_500,
      );
      expect(response.data.done).toBe(true);
      expect(shouldChild).toBe(false);
    }, 10_000);

    it('should EXECUTE CHILD, exit, reenter, complete', async () => {
      //switch the flag to wait once
      shouldChild = true;
      workflowId = 'reentrant590';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['d', { e: 5 }],
        backoffCoefficient: 2,
        maximumAttempts: 3,
        expire: 120,
      };
      const response = await hotMesh.pubsub(
        'memflow.execute',
        payload,
        null,
        2_500,
      );
      expect(response.data.done).toBe(true);
      expect(shouldChild).toBe(false);
    }, 10_000);

    it('should WAIT ALL, exit, reenter, complete', async () => {
      shouldPromise = true;
      workflowId = 'reentrant589';
      const payload = {
        workflowId,
        workflowTopic,
        arguments: ['e', { f: 6 }],
        backoffCoefficient: 1,
        maximumAttempts: 3, //speed up the test
        expire: 120,
      };
      //start the job and sleep for a bit (the workflow will pause in an awaited state)
      const jobId = await hotMesh.pub('memflow.execute', payload);
      await sleepFor(2_500);

      //send the signal so that the workflow can continue
      await hotMesh.hook(collatorSignalTopic, {
        id: `${signalId}hi`,
        data: { ...signalResponse, id: `${signalId}hi` },
      });

      //wait for all jobs to complete
      let response: JobOutput;
      do {
        await sleepFor(1000);
        response = await hotMesh.getState('memflow.execute', jobId);
      } while (!response.data.done);

      expect(shouldPromise).toBe(false);
    }, 20_000);
  });
});
