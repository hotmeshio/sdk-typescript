import { Client as Postgres } from 'pg';

import { MemFlow, } from '../../../services/memflow';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ClientService } from '../../../services/memflow/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options,
} from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | context | `get, set, merge` | Postgres', () => {
  const prefix = 'context-';
  const namespace = 'prod';
  let client: ClientService;
  let workflowGuid: string;
  let postgresClient: ProviderNativeClient;
  let handle: WorkflowHandleService;
  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
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

  describe('Client', () => {
    describe('start', () => {
      it('should connect a client and start a workflow execution', async () => {
        client = new Client({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
        });

        workflowGuid = prefix + guid();

        handle = await client.workflow.start({
          namespace,
          entity: 'user',
          args: ['HotMesh'],
          taskQueue: 'contextual',
          workflowName: 'example',
          workflowId: workflowGuid,
          expire: 120, //keep in DB after completion for 120 seconds (expire is a soft-delete)
        });
        expect(handle.workflowId).toBeDefined();
      });
    });
  });

  describe('Worker', () => {
    describe('create', () => {
      it('should create a worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'contextual',
          workflow: workflows.example,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create hook1 worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'contextual',
          workflow: workflows.hook1,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create hook2 worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'contextual',
          workflow: workflows.hook2,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should test infinite loop protection', async () => {
        // Create a worker for the test function
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'contextual',
          workflow: workflows.testInfiniteLoopProtection,
        });
        await worker.run();

        // Start the test workflow
        const testHandle = await client.workflow.start({
          namespace,
          args: ['TestUser'],
          taskQueue: 'contextual',
          workflowName: 'testInfiniteLoopProtection',
          workflowId: prefix + 'infinite-loop-test-' + guid(),
          expire: 30,
        });

        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        // Cast response to any to access properties since we know the structure
        const result = response as any;
        
        expect(result.success).toBe(true);
        expect(result.message).toBe('Infinite loop protection working correctly');
        expect(result.error).toContain('MemFlow Hook Error: Potential infinite loop detected!');
        expect(result.error).toContain('taskQueue');
        expect(result.error).toContain('entity');
      });

      it('should test execHook functionality', async () => {
        // Create worker for testExecHook workflow
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'contextual',
          workflow: workflows.testExecHook,
        });
        await worker.run();

        // Start the testExecHook workflow
        const testHandle = await client.workflow.start({
          entity: 'user',
          namespace,
          args: ['ExecHookUser'],
          taskQueue: 'contextual',
          workflowName: 'testExecHook',
          workflowId: prefix + 'exec-hook-test-' + guid(),
          expire: 30,
        });

        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        // Cast response to any to access properties since we know the structure
        const result = response as any;
        
        // Validate basic response structure
        expect(result).toHaveProperty('success', true);
        expect(result).toHaveProperty('message', 'ExecHook functionality working correctly');
        expect(result).toHaveProperty('signalResult');
        expect(result).toHaveProperty('initialContext');
        expect(result).toHaveProperty('mergedContext');
        expect(result).toHaveProperty('finalContext');
        
        // Validate the signal result structure (hook1 response)
        const signalResult = result.signalResult;
        expect(signalResult).toHaveProperty('hook', 'hook1');
        expect(signalResult).toHaveProperty('name', 'ExecHookUser');
        expect(signalResult).toHaveProperty('hookType', 'execHook-test');
        expect(signalResult).toHaveProperty('processedAt');
        expect(signalResult).toHaveProperty('data');
        expect(signalResult.data).toContain('Processed by hook1: ExecHookUser-execHook-test');
        
        // Validate initial context structure
        const initialContext = result.initialContext;
        expect(initialContext).toHaveProperty('testType', 'execHook');
        expect(initialContext).toHaveProperty('user');
        expect(initialContext.user).toHaveProperty('name', 'ExecHookUser');
        expect(initialContext.user).toHaveProperty('id');
        expect(initialContext.user.id).toMatch(/^user-\d+$/);
        expect(initialContext).toHaveProperty('startTime');
        expect(initialContext).toHaveProperty('status', 'initialized');
        expect(initialContext).toHaveProperty('operations');
        expect(Array.isArray(initialContext.operations)).toBe(true);
        expect(initialContext.operations).toHaveLength(0);
        expect(initialContext).toHaveProperty('metrics');
        expect(initialContext.metrics).toHaveProperty('hookCount', 0);
        expect(initialContext.metrics).toHaveProperty('totalProcessingTime', 0);
        
        // Validate merged context structure
        const mergedContext = result.mergedContext;
        expect(mergedContext).toHaveProperty('testType', 'execHook');
        expect(mergedContext).toHaveProperty('status', 'hook-completed');
        expect(mergedContext).toHaveProperty('hookResult');
        expect(mergedContext).toHaveProperty('completedAt');
        expect(mergedContext).toHaveProperty('metrics');
        expect(mergedContext.metrics).toHaveProperty('hookCount', 1);
        expect(mergedContext.metrics).toHaveProperty('totalProcessingTime', 2000);
        
        // Validate final context structure (should include operations)
        const finalContext = result.finalContext;
        expect(finalContext).toHaveProperty('testType', 'execHook');
        expect(finalContext).toHaveProperty('status', 'hook-completed');
        expect(finalContext).toHaveProperty('hookResult');
        expect(finalContext).toHaveProperty('operations');
        expect(Array.isArray(finalContext.operations)).toBe(true);
        expect(finalContext.operations).toContain('execHook-executed');
        expect(finalContext.operations).toContain('context-merged');
        expect(finalContext.operations).toHaveLength(2);
        
        // Validate that hook result is properly embedded in final context
        expect(finalContext.hookResult).toHaveProperty('hook', 'hook1');
        expect(finalContext.hookResult).toHaveProperty('name', 'ExecHookUser');
        expect(finalContext.hookResult).toHaveProperty('hookType', 'execHook-test');
        
        // Validate user data is preserved
        expect(finalContext).toHaveProperty('user');
        expect(finalContext.user).toHaveProperty('name', 'ExecHookUser');
        expect(finalContext.user).toHaveProperty('id');
        
        // Validate timestamps
        expect(finalContext).toHaveProperty('startTime');
        expect(finalContext).toHaveProperty('completedAt');
        expect(new Date(finalContext.completedAt).getTime()).toBeGreaterThan(new Date(finalContext.startTime).getTime());
      });

      it('should return the evolved context', async () => {
        const response = await handle.result();
        expect(response).toBeDefined();
        expect(typeof response).toBe('object');
        
        // Cast response to any to access properties since we know the structure
        const result = response as any;

        // Validate the response structure
        expect(result).toHaveProperty('message');
        expect(result).toHaveProperty('hookResults');
        expect(result).toHaveProperty('finalContext');
        
        // Validate message
        expect(typeof result.message).toBe('string');
        expect(result.message).toContain('Hello, HotMesh!');
        expect(result.message).toContain('Hooks completed successfully');
        
        // Validate hook results
        expect(result.hookResults).toBeDefined();
        expect(typeof result.hookResults).toBe('object');
        expect(result.hookResults).toHaveProperty('hook1');
        expect(result.hookResults).toHaveProperty('hook2');
        
        // Validate hook1 result
        expect(result.hookResults.hook1).toBeDefined();
        expect(typeof result.hookResults.hook1).toBe('object');
        expect(result.hookResults.hook1).toHaveProperty('hookType');
        expect(result.hookResults.hook1).toHaveProperty('name');
        expect(result.hookResults.hook1).toHaveProperty('message');
        expect(result.hookResults.hook1).toHaveProperty('timestamp');
        expect(result.hookResults.hook1.hookType).toBe('hook1');
        expect(result.hookResults.hook1.name).toBe('HotMesh');
        
        // Validate hook2 result
        expect(result.hookResults.hook2).toBeDefined();
        expect(typeof result.hookResults.hook2).toBe('object');
        expect(result.hookResults.hook2).toHaveProperty('hookType');
        expect(result.hookResults.hook2).toHaveProperty('name');
        expect(result.hookResults.hook2).toHaveProperty('message');
        expect(result.hookResults.hook2).toHaveProperty('timestamp');
        expect(result.hookResults.hook2).toHaveProperty('processingDetails');
        expect(result.hookResults.hook2.hookType).toBe('hook2');
        expect(result.hookResults.hook2.name).toBe('HotMesh');
        expect(result.hookResults.hook2.processingDetails.type).toBe('advanced');
        
        // Validate final context
        expect(result.finalContext).toBeDefined();
        expect(typeof result.finalContext).toBe('object');
        expect(result.finalContext).toHaveProperty('user');
        expect(result.finalContext).toHaveProperty('hookResults');
        expect(result.finalContext).toHaveProperty('operations');
        expect(result.finalContext).toHaveProperty('metrics');
        
        // Validate context user data
        expect(result.finalContext.user).toBeDefined();
        expect(result.finalContext.user.name).toBe('HotMesh');
        expect(result.finalContext.user.language).toBe('en');
        expect(result.finalContext.user).toHaveProperty('lastUpdated');
        expect(result.finalContext.user.processedBy).toBe('example-workflow');
        
        // Validate context operations
        expect(Array.isArray(result.finalContext.operations)).toBe(true);
        expect(result.finalContext.operations).toContain('hook1-executed');
        expect(result.finalContext.operations).toContain('hook2-executed');
        
        // Validate context metrics
        expect(result.finalContext.metrics).toBeDefined();
        expect(result.finalContext.metrics.totalHooks).toBe(2);
        expect(result.finalContext.metrics.count).toBe(5);
        
        // Validate hook status in context
        expect(result.finalContext).toHaveProperty('hookResults');
        expect(result.finalContext.hookResults).toHaveProperty('hook1');
        expect(result.finalContext.hookResults).toHaveProperty('hook2');
      }, 20_000);
    });
  });
});
