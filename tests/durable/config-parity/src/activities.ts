let callCount = 0;

export function resetCallCount() {
  callCount = 0;
}

export function getCallCount() {
  return callCount;
}

export async function failThenSucceed(): Promise<string> {
  callCount++;
  if (callCount < 3) {
    throw new Error(`Attempt ${callCount} failed`);
  }
  return 'success';
}

export async function fastActivity(): Promise<string> {
  return 'fast';
}

export async function slowActivity(): Promise<string> {
  // Sleeps for 30s — should exceed a short startToCloseTimeout
  await new Promise((resolve) => setTimeout(resolve, 30_000));
  return 'slow';
}

export async function validateOrderV2(orderId: string): Promise<string> {
  return `v2-validated-${orderId}`;
}

export async function validateOrder(orderId: string): Promise<string> {
  return `v1-validated-${orderId}`;
}

export async function hookTaskV2(): Promise<string> {
  return 'hook-v2-result';
}

export async function hookTaskV1(): Promise<string> {
  return 'hook-v1-result';
}

export async function chargePayment(orderId: string): Promise<string> {
  return `charged-${orderId}`;
}

export async function refundPayment(orderId: string): Promise<string> {
  return `refunded-${orderId}`;
}

export async function processBatch(cursor: number): Promise<{ nextCursor: number | null; processed: number }> {
  // Simulate batch processing: 3 batches of 10 items each
  const processed = 10;
  const nextCursor = cursor < 3 ? cursor + 1 : null;
  return { nextCursor, processed };
}

export async function securedSignalActivity(value: string): Promise<string> {
  return `processed-${value}`;
}

export async function securedChainStep1(input: string): Promise<string> {
  return `step1-${input}`;
}

export async function securedChainStep2(input: string): Promise<string> {
  return `step2-${input}`;
}

export async function securedChainStep3(input: string): Promise<string> {
  return `step3-${input}`;
}
