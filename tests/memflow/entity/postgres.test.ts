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

describe('MEMFLOW | entity | `get, set, merge` | Postgres', () => {
  const prefix = 'entity-';
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
          taskQueue: 'entityqueue',
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
          taskQueue: 'entityqueue',
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
          taskQueue: 'entityqueue',
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
          taskQueue: 'entityqueue',
          workflow: workflows.hook2,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create testExecChildWithEntity worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'entityqueue',
          workflow: workflows.testExecChildWithEntity,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create createProduct worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'entityqueue',
          workflow: workflows.createProduct,
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
          taskQueue: 'entityqueue',
          workflow: workflows.testInfiniteLoopProtection,
        });
        await worker.run();

        // Start the test workflow
        const testHandle = await client.workflow.start({
          namespace,
          args: ['TestUser'],
          taskQueue: 'entityqueue',
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
          taskQueue: 'entityqueue',
          workflow: workflows.testExecHook,
        });
        await worker.run();

        // Start the testExecHook workflow
        const testHandle = await client.workflow.start({
          entity: 'user',
          namespace,
          args: ['ExecHookUser'],
          taskQueue: 'entityqueue',
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
        expect(result).toHaveProperty('initialEntity');
        expect(result).toHaveProperty('mergedEntity');
        expect(result).toHaveProperty('finalEntity');
        
        // Validate the signal result structure (hook1 response)
        const signalResult = result.signalResult;
        expect(signalResult).toHaveProperty('hook', 'hook1');
        expect(signalResult).toHaveProperty('name', 'ExecHookUser');
        expect(signalResult).toHaveProperty('hookType', 'execHook-test');
        expect(signalResult).toHaveProperty('processedAt');
        expect(signalResult).toHaveProperty('data');
        expect(signalResult.data).toContain('Processed by hook1: ExecHookUser-execHook-test');
        
        // Validate initial entity structure
        const initialEntity = result.initialEntity;
        expect(initialEntity).toHaveProperty('testType', 'execHook');
        expect(initialEntity).toHaveProperty('user');
        expect(initialEntity.user).toHaveProperty('name', 'ExecHookUser');
        expect(initialEntity.user).toHaveProperty('id');
        expect(initialEntity.user.id).toMatch(/^user-\d+$/);
        expect(initialEntity).toHaveProperty('startTime');
        expect(initialEntity).toHaveProperty('status', 'initialized');
        expect(initialEntity).toHaveProperty('operations');
        expect(Array.isArray(initialEntity.operations)).toBe(true);
        expect(initialEntity.operations).toHaveLength(0);
        expect(initialEntity).toHaveProperty('metrics');
        expect(initialEntity.metrics).toHaveProperty('hookCount', 0);
        expect(initialEntity.metrics).toHaveProperty('totalProcessingTime', 0);
        
        // Validate merged entity structure
        const mergedEntity = result.mergedEntity;
        expect(mergedEntity).toHaveProperty('testType', 'execHook');
        expect(mergedEntity).toHaveProperty('status', 'hook-completed');
        expect(mergedEntity).toHaveProperty('hookResult');
        expect(mergedEntity).toHaveProperty('completedAt');
        expect(mergedEntity).toHaveProperty('metrics');
        expect(mergedEntity.metrics).toHaveProperty('hookCount', 1);
        expect(mergedEntity.metrics).toHaveProperty('totalProcessingTime', 2000);
        
        // Validate final entity structure (should include operations)
        const finalEntity = result.finalEntity;
        expect(finalEntity).toHaveProperty('testType', 'execHook');
        expect(finalEntity).toHaveProperty('status', 'hook-completed');
        expect(finalEntity).toHaveProperty('hookResult');
        expect(finalEntity).toHaveProperty('operations');
        expect(Array.isArray(finalEntity.operations)).toBe(true);
        expect(finalEntity.operations).toContain('execHook-executed');
        expect(finalEntity.operations).toContain('entity-merged');
        expect(finalEntity.operations).toHaveLength(2);
        
        // Validate that hook result is properly embedded in final entity
        expect(finalEntity.hookResult).toHaveProperty('hook', 'hook1');
        expect(finalEntity.hookResult).toHaveProperty('name', 'ExecHookUser');
        expect(finalEntity.hookResult).toHaveProperty('hookType', 'execHook-test');
        
        // Validate user data is preserved
        expect(finalEntity).toHaveProperty('user');
        expect(finalEntity.user).toHaveProperty('name', 'ExecHookUser');
        expect(finalEntity.user).toHaveProperty('id');
        
        // Validate timestamps
        expect(finalEntity).toHaveProperty('startTime');
        expect(finalEntity).toHaveProperty('completedAt');
        expect(new Date(finalEntity.completedAt).getTime()).toBeGreaterThan(new Date(finalEntity.startTime).getTime());
      });

      it('should test execChild functionality with entity parameter', async () => {
        // Start the testExecChildWithEntity workflow
        const testHandle = await client.workflow.start({
          entity: 'user', // Parent entity type
          namespace,
          args: ['ExecChildUser'],
          taskQueue: 'entityqueue',
          workflowName: 'testExecChildWithEntity',
          workflowId: prefix + 'exec-child-entity-test-' + guid(),
          expire: 30,
        });

        // Wait for the result
        const response = await testHandle.result();
        expect(response).toBeDefined();
        
        // Cast response to any to access properties since we know the structure
        const result = response as any;
        
        // Validate basic response structure
        expect(result).toHaveProperty('success', true);
        expect(result).toHaveProperty('message', 'ExecChild with entity functionality working correctly');
        expect(result).toHaveProperty('userEntity');
        expect(result).toHaveProperty('productResult');
        expect(result).toHaveProperty('entityType', 'user');
        expect(result).toHaveProperty('childEntityType', 'product');
        
        // Validate user entity structure
        const userEntity = result.userEntity;
        expect(userEntity).toHaveProperty('entityType', 'user');
        expect(userEntity).toHaveProperty('name', 'ExecChildUser');
        expect(userEntity).toHaveProperty('id');
        expect(userEntity.id).toMatch(/^user-\d+$/);
        expect(userEntity).toHaveProperty('status', 'product-created');
        expect(userEntity).toHaveProperty('startTime');
        expect(userEntity).toHaveProperty('completedAt');
        expect(userEntity).toHaveProperty('operations');
        expect(Array.isArray(userEntity.operations)).toBe(true);
        expect(userEntity.operations).toContain('execChild-called');
        expect(userEntity.operations).toContain('product-created');
        expect(userEntity).toHaveProperty('createdEntities');
        expect(Array.isArray(userEntity.createdEntities)).toBe(true);
        expect(userEntity.createdEntities).toHaveLength(1);
        
        // Validate product result structure
        const productResult = result.productResult;
        expect(productResult).toHaveProperty('success', true);
        expect(productResult).toHaveProperty('message');
        expect(productResult.message).toContain('Product Laptop created successfully by ExecChildUser');
        expect(productResult).toHaveProperty('product');
        expect(productResult).toHaveProperty('entityType', 'product');
        expect(productResult).toHaveProperty('id');
        
        // Validate the created product entity
        const product = productResult.product;
        expect(product).toHaveProperty('entityType', 'product');
        expect(product).toHaveProperty('name', 'Laptop');
        expect(product).toHaveProperty('price', 999.99);
        expect(product).toHaveProperty('id');
        expect(product.id).toMatch(/^product-\d+$/);
        expect(product).toHaveProperty('createdBy', 'ExecChildUser');
        expect(product).toHaveProperty('createdAt');
        expect(product).toHaveProperty('status', 'created');
        expect(product).toHaveProperty('operations');
        expect(Array.isArray(product.operations)).toBe(true);
        expect(product.operations).toContain('product-initialized');
        expect(product.operations).toContain('inventory-updated');
        expect(product).toHaveProperty('metadata');
        expect(product.metadata).toHaveProperty('category', 'electronics');
        expect(product.metadata).toHaveProperty('inStock', true);
        expect(product).toHaveProperty('processedAt');
        
        // Validate that the user entity references the created product
        expect(userEntity.createdEntities[0]).toBe(productResult.id);
        
        // Validate timestamps
        expect(new Date(userEntity.completedAt).getTime()).toBeGreaterThan(new Date(userEntity.startTime).getTime());
        expect(new Date(product.processedAt).getTime()).toBeGreaterThan(new Date(product.createdAt).getTime());
      });

      it('should return the evolved entity', async () => {
        const response = await handle.result();
        expect(response).toBeDefined();
        expect(typeof response).toBe('object');
        
        // Cast response to any to access properties since we know the structure
        const result = response as any;

        // Validate the response structure
        expect(result).toHaveProperty('message');
        expect(result).toHaveProperty('hookResults');
        expect(result).toHaveProperty('finalEntity');
        
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
        
        // Validate final entity
        expect(result.finalEntity).toBeDefined();
        expect(typeof result.finalEntity).toBe('object');
        expect(result.finalEntity).toHaveProperty('user');
        expect(result.finalEntity).toHaveProperty('hookResults');
        expect(result.finalEntity).toHaveProperty('operations');
        expect(result.finalEntity).toHaveProperty('metrics');
        
        // Validate entity user data
        expect(result.finalEntity.user).toBeDefined();
        expect(result.finalEntity.user.name).toBe('HotMesh');
        expect(result.finalEntity.user.language).toBe('en');
        expect(result.finalEntity.user).toHaveProperty('lastUpdated');
        expect(result.finalEntity.user.processedBy).toBe('example-workflow');
        
        // Validate entity operations
        expect(Array.isArray(result.finalEntity.operations)).toBe(true);
        expect(result.finalEntity.operations).toContain('hook1-executed');
        expect(result.finalEntity.operations).toContain('hook2-executed');
        
        // Validate entity metrics
        expect(result.finalEntity.metrics).toBeDefined();
        expect(result.finalEntity.metrics.totalHooks).toBe(2);
        expect(result.finalEntity.metrics.count).toBe(5);
        
        // Validate hook status in entity
        expect(result.finalEntity).toHaveProperty('hookResults');
        expect(result.finalEntity.hookResults).toHaveProperty('hook1');
        expect(result.finalEntity.hookResults).toHaveProperty('hook2');
      }, 20_000);
    });
  });
});
