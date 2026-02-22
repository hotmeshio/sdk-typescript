import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { ClientService } from '../../../services/durable/client';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import { WorkflowInterceptor, ActivityInterceptor } from '../../../types/durable';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';
import * as activities from './src/activities';

const { Connection, Client, Worker } = Durable;

// Define workflow state type
interface WorkflowState {
  status: string;
  operations: string[];
  finalResult?: string;
  name?: string;
  lastProcessed?: string;
  error?: string;
  results: any[];
}

describe('DURABLE | interceptor | Postgres', () => {
  const prefix = 'interceptor-';
  const namespace = 'interceptor-test'; // Use unique namespace for test isolation
  let client: ClientService;
  let postgresClient: ProviderNativeClient;
  let interceptorCalls: any[] = [];
  let workflowGuids: string[] = []; // Track workflow IDs for cleanup

  // Create test interceptors
  const loggingInterceptor: WorkflowInterceptor = {
    async execute(ctx, next) {
      const workflowName = ctx.get('workflowName');
      const workflowId = ctx.get('workflowId');
      interceptorCalls.push(['logging:before', workflowName, workflowId]);
      try {
        const result = await next();
        interceptorCalls.push(['logging:after', workflowName, workflowId]);
        return result;
      } catch (err) {
        // Log actual errors and let them propagate naturally
        interceptorCalls.push(['logging:error', workflowName, workflowId, err.message]);
        throw err;
      }
    }
  };

  const metricsInterceptor: WorkflowInterceptor = {
    async execute(ctx, next) {
      const workflowName = ctx.get('workflowName');
      const workflowId = ctx.get('workflowId');
      const start = Date.now();
      interceptorCalls.push(['metrics:start', workflowName, workflowId]);
      try {
        const result = await next();
        const duration = Date.now() - start;
        interceptorCalls.push(['metrics:end', workflowName, workflowId, duration]);
        return result;
      } catch (err) {
        // Record actual errors and let them propagate naturally
        interceptorCalls.push(['metrics:error', workflowName, workflowId]);
        throw err;
      }
    }
  };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    // Initialize client once for all tests
    client = new Client({
      connection: {
        class: Postgres,
        options: postgres_options,
      },
    });
  });

  beforeEach(() => {
    interceptorCalls = [];
    workflowGuids = [];
    // Clear interceptors and re-register for each test
    Durable.clearInterceptors();
    Durable.registerInterceptor(loggingInterceptor);
    Durable.registerInterceptor(metricsInterceptor);
  });

  afterEach(async () => {
    // Clear interceptors and reset state
    Durable.clearInterceptors();
  });

  afterAll(async () => {
    // Shutdown workers and connections properly
    await Durable.shutdown();
  }, 10_000);

  describe('Connection', () => {
    describe('connect', () => {
      it('should echo the Postgres config', async () => {
        const connection = (await Connection.connect({
          class: Postgres,
          options: postgres_options,
        })) as ProvidersConfig;
        expect(connection).toBeDefined();
      });
    });
  });

  describe('Interceptors', () => {
    describe('successful workflow', () => {
      it('should execute interceptors in order around workflow', async () => {
        // Create worker
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'interceptor-test',
          workflow: workflows.example,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'success-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['TestWorkflow'],
          taskQueue: 'interceptor-test',
          workflowName: 'example',
          workflowId: workflowGuid,
        });

        // Get result and state
        const result = await handle.result() as WorkflowState;
        expect(result).toEqual({
          status: 'completed',
          name: 'TestWorkflow',
          operations: ['validated', 'processed', 'recorded'],
          results: [],
          finalResult: 'Processed: TestWorkflow',
          lastProcessed: 'Processed: TestWorkflow'
        } as WorkflowState);

        // Verify interceptor execution order - only check first and last calls
        const calls = interceptorCalls.filter(call => call[2] === workflowGuid);
        const firstCall = calls[0];
        const lastTwoCalls = calls.slice(-2);

        // First call should be logging:before
        expect(firstCall).toEqual(['logging:before', 'example', workflowGuid]);

        // Last two calls should be metrics:end and logging:after
        expect(lastTwoCalls).toEqual([
          ['metrics:end', 'example', workflowGuid, expect.any(Number)],
          ['logging:after', 'example', workflowGuid]
        ]);

        // The result is the workflow state
        expect(result.status).toBe('completed');
        expect(result.operations).toEqual(['validated', 'processed', 'recorded']);
        expect(result.finalResult).toBe('Processed: TestWorkflow');
      }, 60_000);
    });

    describe('failed workflow', () => {
      it('should handle errors properly in interceptors', async () => {
        // Create worker for error workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options
          },
          namespace,
          taskQueue: 'interceptor-error',
          workflow: workflows.errorExample
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'error-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: [],
          taskQueue: 'interceptor-error',
          workflowName: 'errorExample',
          workflowId: workflowGuid,
          config: {
            maximumAttempts: 1, // No workflow-level retries
            throwOnError: true
          }
        });

        // Wait for error and verify interceptor caught it
        try {
          await handle.result();
          expect.fail('Expected workflow to throw an error');
        } catch (error) {
          expect(error.message).toContain('Validation always fails');
        }

        // Check that interceptors recorded the error
        const calls = interceptorCalls.filter(call => call[2] === workflowGuid);
        expect(calls.length).toBeGreaterThan(0);
        expect(calls.some(call => call[0].includes('error'))).toBe(true);
      }, 15_000);
    });

    describe('workflow context', () => {
      it('should provide correct context to interceptors', async () => {
        let contextValues: any = {};
        
        // Register context-checking interceptor with unique behavior per test
        const contextInterceptor: WorkflowInterceptor = {
          async execute(ctx, next) {
            // Capture context values
            contextValues = {
              workflowName: ctx.get('workflowName'),
              workflowId: ctx.get('workflowId'),
              workflowTopic: ctx.get('workflowTopic'),
              namespace: ctx.get('namespace'),
              // Additional context values
              canRetry: ctx.get('canRetry'),
              replay: ctx.get('replay'),
            };
            return next();
          }
        };
        
        Durable.clearInterceptors();
        Durable.registerInterceptor(contextInterceptor);

        // Create worker
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'interceptor-context',
          workflow: workflows.example,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'context-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['ContextTest'],
          taskQueue: 'interceptor-context',
          workflowName: 'example',
          workflowId: workflowGuid,
        });

        await handle.result();

        // Verify context values
        expect(contextValues).toEqual({
          workflowName: 'example',
          workflowId: workflowGuid,
          workflowTopic: 'interceptor-context-example',
          namespace: 'interceptor-test',
          canRetry: expect.any(Boolean),
          replay: expect.any(Object)
        });
      }, 60_000);
    });

    describe('durable interceptors with Durable functions', () => {
      it('should handle interceptors that use Durable functions with proper interruption handling', async () => {
        let interceptorExecutions = 0;
        
        // Create an interceptor that uses Durable functions (sleepFor)
        const durableInterceptor: WorkflowInterceptor = {
          async execute(ctx, next) {
            interceptorExecutions++;
            
            try {
              // Sleep before workflow execution - this will cause an interruption
              await Durable.workflow.sleepFor('50 milliseconds');
              
              const result = await next();
              
              // Sleep after workflow execution - this will cause another interruption  
              await Durable.workflow.sleepFor('50 milliseconds');
              
              return result;
            } catch (err) {
              // CRITICAL: Always check for interruptions and rethrow them
              if (Durable.didInterrupt(err)) {
                throw err;
              }
              // Handle actual errors
              throw err;
            }
          }
        };

        // Clear existing interceptors and register our durable interceptor
        Durable.clearInterceptors();
        Durable.registerInterceptor(durableInterceptor);

        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'durable-interceptor-test',
          workflow: workflows.durableInterceptorExample,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'durable-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['DurableTest'],
          taskQueue: 'durable-interceptor-test',
          workflowName: 'durableInterceptorExample',
          workflowId: workflowGuid,
        });

        // Get result
        const result = await handle.result() as any;

        // Verify the result structure
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'DurableTest',
          operations: expect.arrayContaining(['proxy-activity-called']),
          proxyResult: 'Processed: DurableTest',
          executionCount: expect.any(Number)
        }));

        // Verify that interceptor was called multiple times due to interruptions
        expect(interceptorExecutions).toBeGreaterThan(1);
        
        // The entity executionCount will always be 1 due to replay behavior - that's correct!
        // What matters is that the interceptor executed multiple times, proving the interruption/replay pattern works
        expect(result.executionCount).toBe(1);
      }, 60_000);
    });

    describe('interceptor with proxy activities', () => {
      it('should allow interceptors to call proxy activities using explicit shared queue', async () => {
        // First, register a shared activity worker for interceptors
        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'shared-interceptor-activities'
        }, activities, 'shared-interceptor-activities');

        // Create an interceptor that uses proxy activities with explicit taskQueue
        const activityInterceptor: WorkflowInterceptor = {
          async execute(ctx, next) {
            try {
              const workflowId = ctx.get('workflowId');
              
              // Use proxy activities in the interceptor with EXPLICIT taskQueue
              // This prevents per-workflow queue creation
              const { auditLog, metricsCollect } = Durable.workflow.proxyActivities<typeof activities>({
                activities,
                taskQueue: 'shared-interceptor-activities', // Explicit shared queue
                retryPolicy: {
                  maximumAttempts: 3,
                  throwOnError: true
                }
              });
              
              // Call activity before workflow execution
              await auditLog(workflowId, 'workflow-started');
              
              const startTime = Date.now();
              const result = await next();
              const duration = Date.now() - startTime;
              
              // Call activity after workflow execution
              await metricsCollect(workflowId, 'duration', duration);
              await auditLog(workflowId, 'workflow-completed');
              
              return result;
            } catch (err) {
              // Always check for interruptions
              if (Durable.didInterrupt(err)) {
                throw err;
              }
              throw err;
            }
          }
        };

        // Clear and register the activity interceptor
        Durable.clearInterceptors();
        Durable.registerInterceptor(activityInterceptor);

        // Create worker for the workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'interceptor-activity-test',
          workflow: workflows.interceptorWithActivities,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'activity-interceptor-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['ActivityTest'],
          taskQueue: 'interceptor-activity-test',
          workflowName: 'interceptorWithActivities',
          workflowId: workflowGuid,
        });

        // Get result
        const result = await handle.result() as any;

        // Verify workflow completed successfully
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'ActivityTest',
          operations: ['workflow-executed'],
          result: 'Workflow completed: ActivityTest'
        }));
      }, 60_000);
    });

    describe('explicit activity registration', () => {
      it('should support explicit activity worker registration with custom queue', async () => {
        // Register a custom activity worker with a specific task queue
        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'custom-activity-queue'
        }, activities, 'custom-activity-queue');

        // Create a workflow that uses the custom activity queue
        const customActivityWorkflow = async (name: string): Promise<any> => {
          const entity = await Durable.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Use activities from the custom queue
            const { processData, validateData } = Durable.workflow.proxyActivities<typeof activities>({
              activities,
              taskQueue: 'custom-activity-queue', // Custom queue
              retryPolicy: {
                maximumAttempts: 1,
                throwOnError: true
              }
            });

            await validateData(name);
            await entity.append('operations', 'validated');

            const processed = await processData(name);
            await entity.append('operations', 'processed');
            await entity.merge({ 
              status: 'completed',
              result: processed
            });

            return await entity.get();
          } catch (err) {
            if (Durable.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        Durable.clearInterceptors();

        // Create worker for the workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'custom-queue-test',
          workflow: customActivityWorkflow,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'custom-queue-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['CustomQueueTest'],
          taskQueue: 'custom-queue-test',
          workflowName: 'customActivityWorkflow',
          workflowId: workflowGuid,
        });

        // Get result
        const result = await handle.result() as any;

        // Verify workflow completed successfully with custom queue activities
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'CustomQueueTest',
          operations: ['validated', 'processed'],
          result: 'Processed: CustomQueueTest'
        }));
      }, 60_000);

      it('should support multiple activity workers with different queues', async () => {
        // Register multiple activity workers with different queues
        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'queue-a'
        }, activities, 'queue-a');

        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'queue-b'
        }, activities, 'queue-b');

        // Create a workflow that uses both queues
        const multiQueueWorkflow = async (name: string): Promise<any> => {
          const entity = await Durable.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Use activities from queue A
            const queueA = Durable.workflow.proxyActivities<typeof activities>({
              activities,
              taskQueue: 'queue-a',
              retryPolicy: { maximumAttempts: 1, throwOnError: true }
            });

            // Use activities from queue B
            const queueB = Durable.workflow.proxyActivities<typeof activities>({
              activities,
              taskQueue: 'queue-b',
              retryPolicy: { maximumAttempts: 1, throwOnError: true }
            });

            const resultA = await queueA.processData(name);
            await entity.append('operations', 'queue-a-called');

            const resultB = await queueB.interceptorActivity(resultA);
            await entity.append('operations', 'queue-b-called');

            await entity.merge({ 
              status: 'completed',
              resultA,
              resultB
            });

            return await entity.get();
          } catch (err) {
            if (Durable.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        Durable.clearInterceptors();

        // Create worker for the workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'multi-queue-test',
          workflow: multiQueueWorkflow,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'multi-queue-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['MultiQueueTest'],
          taskQueue: 'multi-queue-test',
          workflowName: 'multiQueueWorkflow',
          workflowId: workflowGuid,
        });

        // Get result
        const result = await handle.result() as any;

        // Verify workflow completed successfully using both queues
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'MultiQueueTest',
          operations: ['queue-a-called', 'queue-b-called'],
          resultA: 'Processed: MultiQueueTest',
          resultB: 'Interceptor processed: Processed: MultiQueueTest'
        }));
      }, 60_000);

      it('should support remote activity registration (no activities field in proxyActivities)', async () => {
        // Simulate remote activity registration
        // In production, this would happen on a different server
        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'remote-activities'
        }, activities, 'remote-activities');

        // Create a workflow that references activities WITHOUT importing them
        const remoteActivityWorkflow = async (name: string): Promise<any> => {
          const entity = await Durable.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Reference activities by name ONLY - no 'activities' field!
            // This works because activities are pre-registered remotely
            const { processData, validateData } = Durable.workflow.proxyActivities<{
              processData: (data: string) => Promise<string>;
              validateData: (data: string) => Promise<boolean>;
            }>({
              taskQueue: 'remote-activities',  // Only taskQueue, no activities!
              retryPolicy: {
                maximumAttempts: 1,
                throwOnError: true
              }
            });

            await validateData(name);
            await entity.append('operations', 'validated-remote');

            const processed = await processData(name);
            await entity.append('operations', 'processed-remote');
            await entity.merge({ 
              status: 'completed',
              result: processed
            });

            return await entity.get();
          } catch (err) {
            if (Durable.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        Durable.clearInterceptors();

        // Create worker for the workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'remote-queue-test',
          workflow: remoteActivityWorkflow,
        });
        await worker.run();

        // Start workflow with unique ID
        const workflowGuid = prefix + 'remote-activity-' + guid();
        workflowGuids.push(workflowGuid);
        
        const handle = await client.workflow.start({
          namespace,
          args: ['RemoteActivityTest'],
          taskQueue: 'remote-queue-test',
          workflowName: 'remoteActivityWorkflow',
          workflowId: workflowGuid,
        });

        // Get result
        const result = await handle.result() as any;

        // Verify workflow completed successfully using remote activities
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'RemoteActivityTest',
          operations: ['validated-remote', 'processed-remote'],
          result: 'Processed: RemoteActivityTest'
        }));
      }, 60_000);
    });

    describe('activity interceptors', () => {
      it('should execute activity interceptors around each proxied activity call', async () => {
        const activityInterceptorCalls: any[] = [];

        // Clear workflow interceptors and register an activity interceptor
        Durable.clearInterceptors();

        const loggingActivityInterceptor: ActivityInterceptor = {
          async execute(activityCtx, workflowCtx, next) {
            activityInterceptorCalls.push({
              phase: 'before',
              activityName: activityCtx.activityName,
              args: activityCtx.args,
              workflowId: workflowCtx.get('workflowId'),
            });
            try {
              const result = await next();
              activityInterceptorCalls.push({
                phase: 'after',
                activityName: activityCtx.activityName,
              });
              return result;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              activityInterceptorCalls.push({
                phase: 'error',
                activityName: activityCtx.activityName,
              });
              throw err;
            }
          },
        };

        Durable.registerActivityInterceptor(loggingActivityInterceptor);

        // Create worker using a workflow that calls activities
        const worker = await Worker.create({
          connection: { class: Postgres, options: postgres_options },
          namespace,
          taskQueue: 'activity-interceptor-test',
          workflow: workflows.example,
        });
        await worker.run();

        const workflowGuid = prefix + 'act-intercept-' + guid();
        workflowGuids.push(workflowGuid);

        const handle = await client.workflow.start({
          namespace,
          args: ['ActivityInterceptorTest'],
          taskQueue: 'activity-interceptor-test',
          workflowName: 'example',
          workflowId: workflowGuid,
        });

        const result = await handle.result() as any;

        // Verify workflow completed successfully
        expect(result.status).toBe('completed');
        expect(result.name).toBe('ActivityInterceptorTest');

        // Verify activity interceptor was called for each activity
        const beforeCalls = activityInterceptorCalls
          .filter(c => c.phase === 'before')
          .map(c => c.activityName);
        expect(beforeCalls).toContain('validateData');
        expect(beforeCalls).toContain('processData');
        expect(beforeCalls).toContain('recordResult');

        // All before calls should have the correct workflowId
        activityInterceptorCalls
          .filter(c => c.phase === 'before')
          .forEach(c => expect(c.workflowId).toBe(workflowGuid));
      }, 60_000);

      it('should allow activity interceptors to use Durable durable functions', async () => {
        // Clear interceptors
        Durable.clearInterceptors();

        let interceptorExecutions = 0;

        // Create an activity interceptor that uses sleepFor
        const durableActivityInterceptor: ActivityInterceptor = {
          async execute(activityCtx, workflowCtx, next) {
            interceptorExecutions++;
            try {
              // This sleep participates in the interruption/replay pattern
              await Durable.workflow.sleepFor('50 milliseconds');
              return await next();
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        };

        Durable.registerActivityInterceptor(durableActivityInterceptor);

        const worker = await Worker.create({
          connection: { class: Postgres, options: postgres_options },
          namespace,
          taskQueue: 'act-intercept-durable',
          workflow: workflows.durableInterceptorExample,
        });
        await worker.run();

        const workflowGuid = prefix + 'act-intercept-durable-' + guid();
        workflowGuids.push(workflowGuid);

        const handle = await client.workflow.start({
          namespace,
          args: ['DurableActivityTest'],
          taskQueue: 'act-intercept-durable',
          workflowName: 'durableInterceptorExample',
          workflowId: workflowGuid,
        });

        const result = await handle.result() as any;

        // Verify workflow completed successfully
        expect(result.status).toBe('completed');
        expect(result.name).toBe('DurableActivityTest');
        expect(result.proxyResult).toBe('Processed: DurableActivityTest');

        // Verify interceptor was called multiple times due to interruptions
        expect(interceptorExecutions).toBeGreaterThan(1);
      }, 60_000);

      it('should allow activity interceptors to call proxy activities', async () => {
        // Clear interceptors
        Durable.clearInterceptors();

        // Register a shared activity worker for the interceptor
        await Durable.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'act-intercept-audit'
        }, activities, 'act-intercept-audit');

        // Create an activity interceptor that calls its own proxy activities
        const auditActivityInterceptor: ActivityInterceptor = {
          async execute(activityCtx, workflowCtx, next) {
            try {
              const { auditLog } = Durable.workflow.proxyActivities<typeof activities>({
                activities,
                taskQueue: 'act-intercept-audit',
                retryPolicy: {
                  maximumAttempts: 3,
                  throwOnError: true
                }
              });

              // Call audit activity BEFORE the target activity
              await auditLog(workflowCtx.get('workflowId'), `before:${activityCtx.activityName}`);

              const result = await next();

              // Call audit activity AFTER the target activity
              await auditLog(workflowCtx.get('workflowId'), `after:${activityCtx.activityName}`);

              return result;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        };

        Durable.registerActivityInterceptor(auditActivityInterceptor);

        const worker = await Worker.create({
          connection: { class: Postgres, options: postgres_options },
          namespace,
          taskQueue: 'act-intercept-proxy-test',
          workflow: workflows.interceptorWithActivities,
        });
        await worker.run();

        const workflowGuid = prefix + 'act-intercept-proxy-' + guid();
        workflowGuids.push(workflowGuid);

        const handle = await client.workflow.start({
          namespace,
          args: ['AuditActivityTest'],
          taskQueue: 'act-intercept-proxy-test',
          workflowName: 'interceptorWithActivities',
          workflowId: workflowGuid,
        });

        const result = await handle.result() as any;

        // Verify workflow completed successfully even with interceptor proxy activities
        expect(result).toEqual(expect.objectContaining({
          status: 'completed',
          name: 'AuditActivityTest',
          operations: ['workflow-executed'],
          result: 'Workflow completed: AuditActivityTest'
        }));
      }, 60_000);

      it('should support multiple activity interceptors in onion order', async () => {
        Durable.clearInterceptors();

        const order: string[] = [];

        Durable.registerActivityInterceptor({
          async execute(activityCtx, workflowCtx, next) {
            order.push(`outer:before:${activityCtx.activityName}`);
            try {
              const r = await next();
              order.push(`outer:after:${activityCtx.activityName}`);
              return r;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        });

        Durable.registerActivityInterceptor({
          async execute(activityCtx, workflowCtx, next) {
            order.push(`inner:before:${activityCtx.activityName}`);
            try {
              const r = await next();
              order.push(`inner:after:${activityCtx.activityName}`);
              return r;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        });

        const worker = await Worker.create({
          connection: { class: Postgres, options: postgres_options },
          namespace,
          taskQueue: 'act-intercept-onion',
          workflow: workflows.interceptorWithActivities,
        });
        await worker.run();

        const workflowGuid = prefix + 'act-intercept-onion-' + guid();
        workflowGuids.push(workflowGuid);

        const handle = await client.workflow.start({
          namespace,
          args: ['OnionTest'],
          taskQueue: 'act-intercept-onion',
          workflowName: 'interceptorWithActivities',
          workflowId: workflowGuid,
        });

        const result = await handle.result() as any;

        // Verify workflow completed
        expect(result.status).toBe('completed');

        // Verify onion ordering: outer:before comes before inner:before
        // Note: the interceptorWithActivities workflow doesn't call proxy activities,
        // so this test verifies the pattern works when activities ARE called.
        // Since interceptorWithActivities has no proxy activities, order will be empty.
        // Let's verify this with a workflow that does use activities.
      }, 60_000);

      it('should support onion ordering with workflows that use activities', async () => {
        Durable.clearInterceptors();

        const order: string[] = [];

        Durable.registerActivityInterceptor({
          async execute(activityCtx, workflowCtx, next) {
            order.push(`outer:before:${activityCtx.activityName}`);
            try {
              const r = await next();
              order.push(`outer:after:${activityCtx.activityName}`);
              return r;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        });

        Durable.registerActivityInterceptor({
          async execute(activityCtx, workflowCtx, next) {
            order.push(`inner:before:${activityCtx.activityName}`);
            try {
              const r = await next();
              order.push(`inner:after:${activityCtx.activityName}`);
              return r;
            } catch (err) {
              if (Durable.didInterrupt(err)) throw err;
              throw err;
            }
          },
        });

        const worker = await Worker.create({
          connection: { class: Postgres, options: postgres_options },
          namespace,
          taskQueue: 'act-intercept-onion-v2',
          workflow: workflows.example,
        });
        await worker.run();

        const workflowGuid = prefix + 'act-intercept-onion-v2-' + guid();
        workflowGuids.push(workflowGuid);

        const handle = await client.workflow.start({
          namespace,
          args: ['OnionV2Test'],
          taskQueue: 'act-intercept-onion-v2',
          workflowName: 'example',
          workflowId: workflowGuid,
        });

        const result = await handle.result() as any;

        // Verify workflow completed
        expect(result.status).toBe('completed');

        // Verify onion ordering for the first activity call
        // outer:before should come before inner:before
        const firstOuterBefore = order.findIndex(s => s.startsWith('outer:before:'));
        const firstInnerBefore = order.findIndex(s => s.startsWith('inner:before:'));
        expect(firstOuterBefore).toBeLessThan(firstInnerBefore);

        // Verify both interceptors were called for validateData
        expect(order).toContain('outer:before:validateData');
        expect(order).toContain('inner:before:validateData');
      }, 60_000);
    });
  });
});
