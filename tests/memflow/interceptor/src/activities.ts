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