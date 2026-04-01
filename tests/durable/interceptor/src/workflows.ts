import { Durable } from '../../../../services/durable';
import * as activities from './activities';
import { DurableProxyError } from '../../../../modules/errors';

// Set up proxied activities with NO retries for testing
// Using default (workflow-derived) activity queue
const { processData, validateData, recordResult } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
  retryPolicy: {
    maximumAttempts: 1, // No retries
    backoffCoefficient: 1,
    maximumInterval: '1 second',
    throwOnError: true // Ensure errors are thrown, not returned
  }
});

// Separate isolated proxy for error testing to avoid contamination
const { alwaysFailValidation } = Durable.workflow.proxyActivities<typeof activities>({
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
  const entity = await Durable.workflow.entity();
  
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
    if (Durable.workflow.didInterrupt(err)) {
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
  const entity = await Durable.workflow.entity();
  
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
    if (Durable.workflow.didInterrupt(err)) {
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

// Workflow that calls securedActivity — the activity interceptor injects the principal.
// Note: NO argumentMetadata in config — the interceptor provides it.
export async function securedWorkflow(userId: string): Promise<any> {
  const entity = await Durable.workflow.entity();

  try {
    await entity.set({ userId, status: 'started', operations: [] });

    // No argumentMetadata here — the interceptor will inject it
    const { securedActivity } = Durable.workflow.proxyActivities<typeof activities>({
      activities,
      retryPolicy: { maximumAttempts: 1, throwOnError: true },
    });

    const result1 = await securedActivity('read-secrets');
    await entity.append('operations', 'read-secrets');

    const result2 = await securedActivity('write-config');
    await entity.append('operations', 'write-config');

    await entity.merge({
      status: 'completed',
      result1,
      result2,
    });

    return await entity.get();
  } catch (err) {
    if (Durable.workflow.didInterrupt(err)) {
      throw err;
    }
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}

// Workflow that demonstrates argumentMetadata propagation to activities
export async function metadataExample(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();

  try {
    await entity.set({
      name,
      status: 'started',
      operations: [],
    });

    // Proxy with argumentMetadata
    const { metadataAwareActivity } = Durable.workflow.proxyActivities<typeof activities>({
      activities,
      argumentMetadata: { tenantId: 'acme', traceId: 'trace-123', custom: { nested: true } },
      retryPolicy: {
        maximumAttempts: 1,
        throwOnError: true,
      },
    });

    const result = await metadataAwareActivity(name);
    await entity.append('operations', 'metadata-activity-called');
    await entity.merge({
      status: 'completed',
      activityResult: result,
    });

    return await entity.get();
  } catch (err) {
    if (Durable.workflow.didInterrupt(err)) {
      throw err;
    }
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}

// Workflow that calls multiple activities with different metadata
export async function multiMetadataExample(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();

  try {
    await entity.set({
      name,
      status: 'started',
      operations: [],
    });

    // First proxy group with metadata A
    const groupA = Durable.workflow.proxyActivities<typeof activities>({
      activities,
      argumentMetadata: { group: 'A', priority: 'high' },
      retryPolicy: { maximumAttempts: 1, throwOnError: true },
    });

    // Second proxy group with metadata B
    const groupB = Durable.workflow.proxyActivities<typeof activities>({
      activities,
      argumentMetadata: { group: 'B', priority: 'low' },
      retryPolicy: { maximumAttempts: 1, throwOnError: true },
    });

    const resultA = await groupA.metadataAwareActivity(`${name}-A`);
    await entity.append('operations', 'group-a-called');

    const resultB = await groupB.metadataAwareActivity(`${name}-B`);
    await entity.append('operations', 'group-b-called');

    await entity.merge({
      status: 'completed',
      resultA,
      resultB,
    });

    return await entity.get();
  } catch (err) {
    if (Durable.workflow.didInterrupt(err)) {
      throw err;
    }
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}

// Workflow that calls activities WITHOUT argumentMetadata (fallback test)
export async function noMetadataExample(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();

  try {
    await entity.set({ name, status: 'started', operations: [] });

    const { metadataAwareActivity } = Durable.workflow.proxyActivities<typeof activities>({
      activities,
      retryPolicy: { maximumAttempts: 1, throwOnError: true },
    });

    const result = await metadataAwareActivity(name);
    await entity.append('operations', 'no-metadata-called');
    await entity.merge({ status: 'completed', activityResult: result });

    return await entity.get();
  } catch (err) {
    if (Durable.workflow.didInterrupt(err)) {
      throw err;
    }
    await entity.merge({ status: 'failed', error: err.message });
    throw err;
  }
}

// NEW: Workflow that demonstrates interceptor with proxy activities
export async function interceptorWithActivities(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  
  try {
    // Set initial state
    await entity.set({
      name,
      status: 'started',
      operations: [],
      interceptorCalls: []
    });

    // The interceptor will call proxy activities before and after this workflow
    // The activities called by the interceptor use the global queue
    await entity.append('operations', 'workflow-executed');
    await entity.merge({ 
      status: 'completed',
      result: `Workflow completed: ${name}`
    });

    return await entity.get();
  } catch (err) {
    if (Durable.workflow.didInterrupt(err)) {
      throw err;
    }
    
    await entity.merge({ 
      status: 'failed',
      error: err.message
    });
    throw err;
  }
} 