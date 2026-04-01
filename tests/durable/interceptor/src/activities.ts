// Activities for testing workflow interceptors
export async function processData(data: string): Promise<string> {
  return `Processed: ${data}`;
}

export async function validateData(data: string): Promise<boolean> {
  // Ensure empty string validation fails immediately
  if (!data.trim()) {
    throw Object.assign(new Error('Data validation failed'), {
      name: 'ValidationError',
      isValidationError: true // Add explicit flag
    });
  }
  return true;
}

// Dedicated error activity that always fails - isolated from other tests
export async function alwaysFailValidation(): Promise<boolean> {
  throw Object.assign(new Error('Validation always fails'), {
    name: 'ValidationError',
    isValidationError: true
  });
}

export async function recordResult(result: string): Promise<void> {
  // Simulate some async work
  await new Promise(resolve => setTimeout(resolve, 100));
}

// NEW: Activities for testing interceptor proxy activity calls
export async function auditLog(workflowId: string, action: string): Promise<string> {
  return `Audit: ${action} for workflow ${workflowId}`;
}

export async function metricsCollect(workflowId: string, metric: string, value: number): Promise<void> {
  // Simulate metrics collection
  await new Promise(resolve => setTimeout(resolve, 50));
}

export async function interceptorActivity(message: string): Promise<string> {
  return `Interceptor processed: ${message}`;
}

import { Durable } from '../../../../services/durable';

// Activity that reads metadata from Durable.activity.getContext()
export async function metadataAwareActivity(data: string): Promise<Record<string, any>> {
  const ctx = Durable.activity.getContext();
  return {
    data,
    activityName: ctx.activityName,
    argumentMetadata: ctx.argumentMetadata,
    workflowId: ctx.workflowId,
  };
}

// Simulates a DB lookup for a user profile (used by the security interceptor)
export async function lookupUserProfile(userId: string): Promise<Record<string, any>> {
  // In production, this would be a real DB call
  return {
    userId,
    role: 'admin',
    tenantId: 'acme-corp',
    permissions: ['read', 'write', 'delete'],
    displayName: `User ${userId}`,
  };
}

// Activity that reads the injected principal from argumentMetadata
// and returns what it sees — proving the interceptor-injected metadata arrived
export async function securedActivity(action: string): Promise<Record<string, any>> {
  const ctx = Durable.activity.getContext();
  const principal = ctx.argumentMetadata?.principal;
  return {
    action,
    principal,
    authorized: !!principal?.userId,
  };
}