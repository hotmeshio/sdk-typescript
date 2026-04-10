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
