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
  });
}); 