import { MemFlow } from '../../../../services/memflow';
import * as activities from './activities';
import { MemFlowProxyError } from '../../../../modules/errors';

// Set up proxied activities with NO retries for testing
const { processData, validateData, recordResult } = MemFlow.workflow.proxyActivities<typeof activities>({
  activities,
  retryPolicy: {
    maximumAttempts: 1, // No retries
    backoffCoefficient: 1,
    maximumInterval: '1 second',
    throwOnError: true // Ensure errors are thrown, not returned
  }
});

// Separate isolated proxy for error testing to avoid contamination
const { alwaysFailValidation } = MemFlow.workflow.proxyActivities<typeof activities>({
  activities,
  retryPolicy: {
    maximumAttempts: 1, // No retries
    backoffCoefficient: 1,
    maximumInterval: '1 second',
    throwOnError: true // Ensure errors are thrown, not returned
  }
});

// Workflow that demonstrates durable execution with activities
export async function example(name: string): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  // Set initial state - idempotent operation
  await entity.set({
    name,
    status: 'started',
    operations: [],
    results: []
  });

  try {
    // Validate input - will retry automatically
    await validateData(name);
    await entity.append('operations', 'validated');

    // Process data - durable activity execution
    const processedData = await processData(name);
    await entity.merge({ 
      status: 'processing',
      lastProcessed: processedData 
    });
    await entity.append('operations', 'processed');

    // Record result - demonstrates activity retry
    await recordResult(processedData);
    await entity.append('operations', 'recorded');

    // Final state update - atomic operation
    await entity.merge({ 
      status: 'completed',
      finalResult: processedData
    });

    // Return the full entity state
    return await entity.get();
  } catch (err) {
    // Check if this is a HotMesh interruption
    if (MemFlow.workflow.didInterrupt(err)) {
      // Rethrow interruptions for the workflow engine to handle
      throw err;
    }
    // Handle actual errors
    await entity.merge({ 
      status: 'failed',
      error: err.message
    });
    throw err;
  }
}

// Workflow that demonstrates error handling with activities
export async function errorExample(): Promise<any> {
  // This will always fail - no arguments to contaminate
  return await alwaysFailValidation();
}

// Workflow that demonstrates durable interceptor execution
export async function durableInterceptorExample(name: string): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  
  try {
    // Set basic workflow state
    await entity.set({
      name,
      status: 'processing',
      operations: [],
      executionCount: 1 // This will always be 1 due to replay behavior - that's correct!
    });

    // Call a proxy activity to demonstrate the interruption pattern
    const proxyResult = await processData(name);
    
    // Update final state
    await entity.append('operations', 'proxy-activity-called');
    await entity.merge({ 
      proxyResult,
      status: 'completed'
    });

    return await entity.get();
  } catch (err) {
    // CRITICAL: Always check for interruptions and rethrow them
    if (MemFlow.workflow.didInterrupt(err)) {
      throw err;
    }
    
    // Handle actual errors
    await entity.merge({ 
      status: 'failed',
      error: err.message
    });
    throw err;
  }
} 