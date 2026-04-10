let callCount = 0;

export function resetCallCount() {
  callCount = 0;
}

export function getCallCount() {
  return callCount;
}

export default async function failThenSucceed(): Promise<string> {
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

export async function processBatch(cursor: number): Promise<{ nextCursor: number | null; processed: number }> {
  // Simulate batch processing: 3 batches of 10 items each
  const processed = 10;
  const nextCursor = cursor < 3 ? cursor + 1 : null;
  return { nextCursor, processed };
}
