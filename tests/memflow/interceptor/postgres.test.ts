import { Client as Postgres } from 'pg';

import { MemFlow } from '../../../services/memflow';
import { ClientService } from '../../../services/memflow/client';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import { WorkflowInterceptor } from '../../../types/memflow';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';
import * as activities from './src/activities';

const { Connection, Client, Worker } = MemFlow;

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

describe('MEMFLOW | interceptor | Postgres', () => {
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
    MemFlow.clearInterceptors();
    MemFlow.registerInterceptor(loggingInterceptor);
    MemFlow.registerInterceptor(metricsInterceptor);
  });

  afterEach(async () => {
    // Clear interceptors and reset state
    MemFlow.clearInterceptors();
  });

  afterAll(async () => {
    // Shutdown workers and connections properly
    await MemFlow.shutdown();
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
          fail('Expected workflow to throw an error');
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
        
        MemFlow.clearInterceptors();
        MemFlow.registerInterceptor(contextInterceptor);

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

    describe('durable interceptors with MemFlow functions', () => {
      it('should handle interceptors that use MemFlow functions with proper interruption handling', async () => {
        let interceptorExecutions = 0;
        
        // Create an interceptor that uses MemFlow functions (sleepFor)
        const durableInterceptor: WorkflowInterceptor = {
          async execute(ctx, next) {
            interceptorExecutions++;
            
            try {
              // Sleep before workflow execution - this will cause an interruption
              await MemFlow.workflow.sleepFor('50 milliseconds');
              
              const result = await next();
              
              // Sleep after workflow execution - this will cause another interruption  
              await MemFlow.workflow.sleepFor('50 milliseconds');
              
              return result;
            } catch (err) {
              // CRITICAL: Always check for interruptions and rethrow them
              if (MemFlow.didInterrupt(err)) {
                throw err;
              }
              // Handle actual errors
              throw err;
            }
          }
        };

        // Clear existing interceptors and register our durable interceptor
        MemFlow.clearInterceptors();
        MemFlow.registerInterceptor(durableInterceptor);

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
        await MemFlow.registerActivityWorker({
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
              const { auditLog, metricsCollect } = MemFlow.workflow.proxyActivities<typeof activities>({
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
              if (MemFlow.didInterrupt(err)) {
                throw err;
              }
              throw err;
            }
          }
        };

        // Clear and register the activity interceptor
        MemFlow.clearInterceptors();
        MemFlow.registerInterceptor(activityInterceptor);

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
        await MemFlow.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'custom-activity-queue'
        }, activities, 'custom-activity-queue');

        // Create a workflow that uses the custom activity queue
        const customActivityWorkflow = async (name: string): Promise<any> => {
          const entity = await MemFlow.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Use activities from the custom queue
            const { processData, validateData } = MemFlow.workflow.proxyActivities<typeof activities>({
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
            if (MemFlow.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        MemFlow.clearInterceptors();

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
        await MemFlow.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'queue-a'
        }, activities, 'queue-a');

        await MemFlow.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'queue-b'
        }, activities, 'queue-b');

        // Create a workflow that uses both queues
        const multiQueueWorkflow = async (name: string): Promise<any> => {
          const entity = await MemFlow.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Use activities from queue A
            const queueA = MemFlow.workflow.proxyActivities<typeof activities>({
              activities,
              taskQueue: 'queue-a',
              retryPolicy: { maximumAttempts: 1, throwOnError: true }
            });

            // Use activities from queue B
            const queueB = MemFlow.workflow.proxyActivities<typeof activities>({
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
            if (MemFlow.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        MemFlow.clearInterceptors();

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
        await MemFlow.registerActivityWorker({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'remote-activities'
        }, activities, 'remote-activities');

        // Create a workflow that references activities WITHOUT importing them
        const remoteActivityWorkflow = async (name: string): Promise<any> => {
          const entity = await MemFlow.workflow.entity();
          
          await entity.set({
            name,
            status: 'started',
            operations: []
          });

          try {
            // Reference activities by name ONLY - no 'activities' field!
            // This works because activities are pre-registered remotely
            const { processData, validateData } = MemFlow.workflow.proxyActivities<{
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
            if (MemFlow.didInterrupt(err)) {
              throw err;
            }
            await entity.merge({ status: 'failed', error: err.message });
            throw err;
          }
        };

        // Clear interceptors
        MemFlow.clearInterceptors();

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
  });
}); 