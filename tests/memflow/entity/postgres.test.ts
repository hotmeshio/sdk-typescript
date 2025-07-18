import { Client as Postgres } from 'pg';

import { MemFlow, } from '../../../services/memflow';
import { Entity } from '../../../services/memflow/entity';
import { WorkflowHandleService } from '../../../services/memflow/handle';
import { ClientService } from '../../../services/memflow/client';
import { guid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { ProvidersConfig } from '../../../types/provider';
import {
  dropTables,
  postgres_options as postgres_options_base,
} from '../../$setup/postgres';

const postgres_options = {
  ...postgres_options_base,
  max: 50,
  max_connections: 50,
};

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
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;
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

        //NOTE: expire is important for this unit test as all workflows essentially
        //      soft-delete after completion (after 1 second). Setting to 120
        //      allows a unit test below to run without error (it would get a
        //      job not found error otherwise, as the soft-delete is after 1 second)
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

      it('should create createTestEntity worker', async () => {
        const worker = await Worker.create({
          connection: {
            class: Postgres,
            options: postgres_options,
          },
          namespace,
          taskQueue: 'entityqueue',
          workflow: workflows.createTestEntity,
        });
        await worker.run();
        expect(worker).toBeDefined();
      });

      it('should create testExecHook worker', async () => {
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
        expect(worker).toBeDefined();
      });

      it('should create testInfiniteLoopProtection worker', async () => {
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
        expect(worker).toBeDefined();
      });

      it('should test infinite loop protection', async () => {
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

      
      it('should test execHook functionality', async () => {
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
    });
  });

  describe('Entity Static Find Methods', () => {
    let hotMeshClient: any;
    let testEntities: any[] = [];

    beforeAll(async () => {
      // Get the hotMeshClient from the existing client
      hotMeshClient = await client.getHotMeshClient('', namespace);
      
      // Create multiple test entities with different properties
      const entityData = [
        {
          id: 'user-1',
          entity: 'user',
          name: 'John Doe',
          age: 25,
          status: 'active',
          department: 'engineering',
          salary: 75000,
          tags: ['developer', 'frontend'],
          createdAt: '2024-01-01T10:00:00Z'
        },
        {
          id: 'user-2', 
          entity: 'user',
          name: 'Jane Smith',
          age: 30,
          status: 'active',
          department: 'engineering',
          salary: 85000,
          tags: ['developer', 'backend'],
          createdAt: '2024-01-02T10:00:00Z'
        },
        {
          id: 'user-3',
          entity: 'user', 
          name: 'Bob Johnson',
          age: 35,
          status: 'inactive',
          department: 'marketing',
          salary: 65000,
          tags: ['manager', 'marketing'],
          createdAt: '2024-01-03T10:00:00Z'
        },
        {
          id: 'product-1',
          entity: 'product',
          name: 'Widget A',
          price: 99.99,
          category: 'electronics',
          inStock: true,
          tags: ['gadget', 'popular'],
          createdAt: '2024-01-04T10:00:00Z'
        },
        {
          id: 'product-2',
          entity: 'product',
          name: 'Widget B',
          price: 149.99,
          category: 'electronics',
          inStock: false,
          tags: ['gadget', 'premium'],
          createdAt: '2024-01-05T10:00:00Z'
        }
      ];

      // Create workflow instances for each entity to populate the database
      for (const data of entityData) {
        const handle = await client.workflow.start({
          namespace,
          entity: data.entity,
          args: [data.name, JSON.stringify(data)],
          taskQueue: 'entityqueue',
          workflowName: 'createTestEntity',
          workflowId: prefix + data.id,
          expire: 300,
          search: {
            data: {
              name: data.name,
              entity: data.entity,
              status: data.status || 'active',
              department: data.department || '',
              category: data.category || '',
              age: (data.age || 0).toString(),
              price: (data.price || 0).toString()
            }
          }
        });
        testEntities.push({ handle, data });
      }

      // Wait for all workflows to complete
      await Promise.all(testEntities.map(({ handle }) => handle.result()));
    });

    describe('Entity.find', () => {
      it('should find entities with complex conditions', async () => {
        const results = await Entity.find(
          'user',
          {
            status: 'active',
            department: 'engineering'
          },
          hotMeshClient,
          { limit: 10, offset: 0 }
        );

        expect(Array.isArray(results)).toBe(true);
        expect(results.length).toBeGreaterThan(0);
        
        // Validate that results match our conditions
        results.forEach(result => {
          expect(result).toHaveProperty('context');
          const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
          expect(context.status).toBe('active');
          expect(context.department).toBe('engineering');
        });
      });

      it('should find entities with numeric conditions', async () => {
        const results = await Entity.find(
          'user',
          {
            age: 30
          },
          hotMeshClient
        );

        expect(Array.isArray(results)).toBe(true);
        expect(results.length).toBeGreaterThan(0);
        
        results.forEach(result => {
          expect(result).toHaveProperty('context');
          const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
          expect(context.age).toBe(30);
        });
      });

      it('should return empty array when no entities match conditions', async () => {
        const results = await Entity.find(
          'user',
          {
            status: 'nonexistent'
          },
          hotMeshClient
        );

        expect(Array.isArray(results)).toBe(true);
        expect(results.length).toBe(0);
      });
    });

    describe('Entity.findById', () => {
      it('should find entity by specific ID', async () => {
        const entityId = prefix + 'user-1';
        const result = await Entity.findById('user', entityId, hotMeshClient);

        expect(result).toBeDefined();
        expect(result).toHaveProperty('key');
        expect(result.key).toBe(`hmsh:${namespace}:j:${entityId}`);
        expect(result).toHaveProperty('context');
        
        const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
        expect(context.name).toBe('John Doe');
        expect(context.age).toBe(25);
      });

      it('should return null for non-existent ID', async () => {
        const result = await Entity.findById('user', 'non-existent-id', hotMeshClient);
        expect(result).toBeNull();
      });
    });

         describe('Entity.findByCondition', () => {
       it('should find entities with equality condition', async () => {
         const results = await Entity.findByCondition(
           'user',
           'status',
           'active',
           '=',
           hotMeshClient,
           { limit: 5 }
         );

         expect(Array.isArray(results)).toBe(true);
         expect(results.length).toBeGreaterThan(0);
         
         results.forEach(result => {
           expect(result).toHaveProperty('context');
           const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
           expect(context.status).toBe('active');
         });
       });

       it('should find entities with greater than condition', async () => {
         const results = await Entity.findByCondition(
           'user',
           'age',
           28,
           '>',
           hotMeshClient
         );

         expect(Array.isArray(results)).toBe(true);
         expect(results.length).toBeGreaterThan(0);
         
         results.forEach(result => {
           expect(result).toHaveProperty('context');
           const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
           expect(context.age).toBeGreaterThan(28);
         });
       });

       it('should find entities with LIKE condition', async () => {
         const results = await Entity.findByCondition(
           'user',
           'name',
           'John%',
           'LIKE',
           hotMeshClient
         );

         expect(Array.isArray(results)).toBe(true);
         expect(results.length).toBeGreaterThan(0);
         
         results.forEach(result => {
           expect(result).toHaveProperty('context');
           const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
           expect(context.name).toMatch(/^John/);
         });
       });

       it('should find entities with IN condition', async () => {
         const results = await Entity.findByCondition(
           'user',
           'department',
           ['engineering', 'marketing'],
           'IN',
           hotMeshClient
         );

         expect(Array.isArray(results)).toBe(true);
         expect(results.length).toBeGreaterThan(0);
         
         results.forEach(result => {
           expect(result).toHaveProperty('context');
           const context = typeof result.context === 'string' ? JSON.parse(result.context) : result.context;
           expect(['engineering', 'marketing']).toContain(context.department);
         });
       });

       it('should handle pagination with limit and offset', async () => {
         const page1 = await Entity.findByCondition(
           'user',
           'status',
           'active',
           '=',
           hotMeshClient,
           { limit: 1, offset: 0 }
         );

         const page2 = await Entity.findByCondition(
           'user',
           'status',
           'active',
           '=',
           hotMeshClient,
           { limit: 1, offset: 1 }
         );

         expect(page1.length).toBe(1);
         expect(page2.length).toBeLessThanOrEqual(1);
         
         if (page2.length > 0) {
           expect(page1[0].key).not.toBe(page2[0].key);
         }
       });
     });

    describe('Cross-entity queries', () => {
      it('should find entities across different entity types', async () => {
        // Test querying different entity types
        const users = await Entity.find(
          'user',
          { status: 'active' },
          hotMeshClient
        );

        const products = await Entity.find(
          'product',
          { category: 'electronics' },
          hotMeshClient
        );

        expect(Array.isArray(users)).toBe(true);
        expect(Array.isArray(products)).toBe(true);
        expect(users.length).toBeGreaterThan(0);
        expect(products.length).toBeGreaterThan(0);

        // Verify entity types are different
        if (users.length > 0 && products.length > 0) {
          const userContext = typeof users[0].context === 'string' ? JSON.parse(users[0].context) : users[0].context;
          const productContext = typeof products[0].context === 'string' ? JSON.parse(products[0].context) : products[0].context;
          
          expect(userContext.entity).toBe('user');
          expect(productContext.entity).toBe('product');
        }
      });
    });

    describe('Entity Index Creation', () => {
      it('should create gin index for array field', async () => {
        // Create a workflow with array data to test GIN index
        const handle = await client.workflow.start({
          namespace,
          entity: 'user',
          args: ['Test User', JSON.stringify({
            id: 'user-array',
            entity: 'user',
            name: 'Array Test User',
            tags: ['test', 'gin', 'index']
          })],
          taskQueue: 'entityqueue',
          workflowName: 'createTestEntity',
          workflowId: prefix + 'user-array',
          expire: 300
        });
        await handle.result();

        // Create GIN index on the array field
        await Entity.createIndex('user', 'tags', hotMeshClient);
      });

      it('should handle creating same index again gracefully', async () => {
        await Entity.createIndex('user', 'tags', hotMeshClient);
        // Should not throw error due to IF NOT EXISTS
      });

      it('should handle errors gracefully', async () => {
        try {
          // Try to create index on non-existent entity
          await Entity.createIndex('nonexistent', 'field', hotMeshClient);
          fail('Should have thrown error');
        } catch (error) {
          expect(error).toBeDefined();
        }
      });
    });
  });
});
