import { state } from './state';

export async function count(limit: number): Promise<number> {
  state.counter = state.counter + 1;
  if (state.counter < limit) {
    throw new Error('retry');
  } else {
    return state.counter;
  }
}
