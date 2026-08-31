import { state } from './state';

// Always fails. Each execution increments the shared counter so the test
// can confirm the static retry policy drove multiple attempts before the
// framework gave up and surfaced the terminal error to the workflow.
export async function unreliableActivity(orderId: string): Promise<string> {
  state.attempts = state.attempts + 1;
  throw new Error(`activity-failed-for-${orderId}`);
}
