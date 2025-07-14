import { MemFlow } from '../../../../services/memflow';

export async function example(name: string, language: string = 'en'): Promise<any> {
  // Get single entity instance to use throughout the workflow
  const entity = await MemFlow.workflow.entity();
  
  // Initialize entity with user data
  const initialEntity = await entity.set({
    user: { name, language },
    hookResults: {},
    operations: [],
    metrics: { count: 0 }
  });

  // Trigger both hooks internally and wait for their completion
  // NOTE: taskQueue is required to prevent infinite loops - without it, 
  // the hook would target the same workflow topic as the current workflow
  // 
  // ALTERNATIVE APPROACH: You can use execHook for hook + signal in one call:
  // const hookId1 = await MemFlow.workflow.execHook({
  //   taskQueue: 'entityqueue',
  //   workflowName: 'hook1',
  //   args: [name, 'hook1'],
  //   signalId: 'hook1-complete',
  //   await: false // fire-and-forget
  // });
  
  const [hookAck1, hookAck2] = await Promise.all([
    MemFlow.workflow.hook({
      taskQueue: 'entityqueue', // Required to prevent infinite loop
      workflowName: 'hook1',
      args: [name, 'hook1']
    }),
    MemFlow.workflow.hook({
      taskQueue: 'entityqueue', // Required to prevent infinite loop
      workflowName: 'hook2',
      args: [name, 'hook2']
    })
  ]);

  // Wait for both hooks to complete and send signals
  const [result1, result2] = await Promise.all([
    MemFlow.workflow.waitFor<any>('hook1-complete'),
    MemFlow.workflow.waitFor<any>('hook2-complete')
  ]);

  // Merge hook results into entity
  const mergedEntity = await entity.merge({
    hookResults: {
      hook1: result1,
      hook2: result2
    }
  });

  // Update user data with additional information
  await entity.merge({
    user: {
      lastUpdated: new Date().toISOString(),
      processedBy: 'example-workflow'
    }
  });

  // Test array operations
  await entity.append('operations', 'hook1-executed');
  await entity.append('operations', 'hook2-executed');
  await entity.prepend('operations', 'workflow-started');

  // Test numeric operations
  await entity.increment('metrics.count', 5);
  await entity.increment('metrics.totalHooks', 2);

  // Test boolean operations
  await entity.merge({ settings: { enabled: false } });
  await entity.toggle('settings.enabled');

  // Test conditional set
  await entity.setIfNotExists('metadata.created', new Date().toISOString());
  await entity.setIfNotExists('metadata.version', '1.0.0');

  // Get the final entity to return
  const finalEntity = await entity.get();
  return {
    message: `Hello, ${name}! Hooks completed successfully.`,
    hookResults: {
      hook1: result1,
      hook2: result2
    },
    finalEntity
  };
}

/**
 * Hook function 1 - processes data and signals completion
 * This function handles both direct calls and execHook calls with signal injection
 */
export async function hook1(name: string, hookType: string, ...rest: any[]): Promise<any> {
  
  // Simulate some processing work
  await MemFlow.workflow.sleepFor('2 seconds');
  
  const result = {
    hook: 'hook1',
    name,
    hookType,
    processedAt: new Date().toISOString(),
    data: `Processed by hook1: ${name}-${hookType}`,
    message: `${hookType} completed for ${name}`,
    timestamp: new Date().toISOString()
  };
  
  // Check if last argument is a signal object (from execHook)
  const lastArg = rest[rest.length - 1];
  if (lastArg && typeof lastArg === 'object' && lastArg.signal) {
    await MemFlow.workflow.signal(lastArg.signal, result);
  } else {
    // Traditional behavior - emit the standard hook1-complete signal
    await MemFlow.workflow.signal('hook1-complete', result);
  }
  
  return result;
}

/**
 * Hook function 2 - Updates entity through external hook call
 */
export async function hook2(name: string, hookType: string): Promise<void> {
  //update shared entity (the hook shares same memory as the main workflow)
  const entity = await MemFlow.workflow.entity();
  const user = await entity.merge({ user: { age: 30 } });

  // Simulate different processing time
  await MemFlow.workflow.sleepFor('1 second');

  const result = {
    hookType,
    name,
    message: `${hookType} completed advanced processing for ${name}`,
    timestamp: new Date().toISOString(),
    processingDetails: {
      duration: '3 seconds',
      type: 'advanced'
    }
  };

  // Signal completion to parent workflow
  await MemFlow.workflow.signal('hook2-complete', result);
}

/**
 * Test function to demonstrate infinite loop protection
 * This function will throw an error due to missing taskQueue/entity parameters
 */
export async function testInfiniteLoopProtection(name: string): Promise<any> {
  
  try {
    // This should throw an error due to missing taskQueue/entity
    await MemFlow.workflow.hook({
      workflowName: 'hook1',
      args: [name, 'test-hook']
    });
    
    return { error: 'Expected error was not thrown!' };
  } catch (error) {
    return { 
      success: true, 
      message: 'Infinite loop protection working correctly',
      error: error.message 
    };
  }
}

/**
 * Test execHook functionality with entity management
 * This function tests the execHook functionality by executing a hook and awaiting the signal.
 * It also demonstrates entity management by setting initial data and merging hook results.
 */
export async function testExecHook(name: string): Promise<any> {
  // Create entity and set initial data
  const entity = await MemFlow.workflow.entity();
  
  const initialData = {
    testType: 'execHook',
    user: { name, id: `user-${Date.now()}` },
    startTime: new Date().toISOString(),
    status: 'initialized',
    operations: [],
    metrics: { hookCount: 0, totalProcessingTime: 0 }
  };
  
  await entity.set(initialData);
  
  // Execute hook and await signal result
  // The signalId will be injected as the last argument to hook1
  const hookResult = await MemFlow.workflow.execHook({
    taskQueue: 'entityqueue',
    workflowName: 'hook1',
    args: [name, 'execHook-test'],
    signalId: 'execHook-test-complete'
  });
  
  
  // Merge hook result into entity
  const mergedEntity = await entity.merge({
    status: 'hook-completed',
    hookResult: hookResult,
    completedAt: new Date().toISOString(),
    metrics: {
      hookCount: 1,
      totalProcessingTime: 2000 // 2 seconds from hook1's sleepFor
    }
  });
  
  // Add operation to track what was done
  await entity.append('operations', 'execHook-executed');
  await entity.append('operations', 'entity-merged');
  
  // Get final entity state
  const finalEntity = await entity.get();

  return {
    success: true,
    message: 'ExecHook functionality working correctly',
    signalResult: hookResult,
    initialEntity: initialData,
    mergedEntity: mergedEntity,
    finalEntity: finalEntity
  };
}

/**
 * Test execChild functionality with entity parameter
 * This function tests a 'user' entity creating a 'product' entity using execChild
 */
export async function testExecChildWithEntity(userName: string): Promise<any> {
  // Create user entity and set initial data
  const userEntity = await MemFlow.workflow.entity();
  
  const initialUserData = {
    entityType: 'user',
    name: userName,
    id: `user-${Date.now()}`,
    startTime: new Date().toISOString(),
    status: 'creating-product',
    operations: [],
    createdEntities: []
  };
  
  await userEntity.set(initialUserData);
  
  // User entity creates a product entity via execChild
  const productResult = await MemFlow.workflow.execChild<any>({
    entity: 'product', // This is the key - different entity type
    args: [userName, 'Laptop', 999.99],
    taskQueue: 'entityqueue',
    workflowName: 'createProduct'
  });
  
  // Update user entity with the created product information
  await userEntity.merge({
    status: 'product-created',
    completedAt: new Date().toISOString()
  });
  
  await userEntity.append('operations', 'execChild-called');
  await userEntity.append('operations', 'product-created');
  await userEntity.append('createdEntities', productResult.id);
  
  // Get final user entity state
  const finalUserEntity = await userEntity.get();

  return {
    success: true,
    message: 'ExecChild with entity functionality working correctly',
    userEntity: finalUserEntity,
    productResult: productResult,
    entityType: 'user',
    childEntityType: 'product'
  };
}

/**
 * Test function to create entities with test data for querying
 * This function creates entities with the provided test data for Entity.find() tests
 */
export async function createTestEntity(name: string, dataJson: string): Promise<any> {
  const data = JSON.parse(dataJson);
  
  // Create entity and set the provided data
  const entity = await MemFlow.workflow.entity();
  
  // Set the entity data
  await entity.set(data);
  
  // Add some additional metadata
  await entity.merge({
    createdAt: new Date().toISOString(),
    testEntity: true,
    processed: true
  });
  
  // Get the final entity to return
  const finalEntity = await entity.get();
  
  return {
    success: true,
    message: `Test entity created for ${name}`,
    entity: finalEntity
  };
}

/**
 * Child workflow that creates a product entity
 * This workflow runs as a 'product' entity type
 */
export async function createProduct(createdBy: string, productName: string, price: number): Promise<any> {
  // Create product entity
  const productEntity = await MemFlow.workflow.entity();
  
  const productData = {
    entityType: 'product',
    name: productName,
    price: price,
    id: `product-${Date.now()}`,
    createdBy: createdBy,
    createdAt: new Date().toISOString(),
    status: 'active',
    operations: [],
    metadata: {
      category: 'electronics',
      inStock: true
    }
  };
  
  await productEntity.set<typeof productData>(productData);
  
  // Simulate some product creation processing
  await MemFlow.workflow.sleepFor('1 second');
  
  await productEntity.append('operations', 'product-initialized');
  await productEntity.append('operations', 'inventory-updated');
  
  // Update status to completed
  await productEntity.merge({
    status: 'created',
    processedAt: new Date().toISOString()
  });
  
  // Get final product entity state
  const finalProductEntity = await productEntity.get<typeof productData>();

  return {
    success: true,
    message: `Product ${productName} created successfully by ${createdBy}`,
    product: finalProductEntity,
    entityType: 'product',
    id: finalProductEntity.id
  };
}
