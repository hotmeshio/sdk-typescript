import { state } from './state';

export async function count(): Promise<number> {
  state.counter = state.counter + 1;
  if (state.counter < 5) {
    throw new Error('retry');
  } else {
    return state.counter;
  }
}
