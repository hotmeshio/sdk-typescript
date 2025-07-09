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
