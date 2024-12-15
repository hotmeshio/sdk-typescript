import { asyncLocalStorage, deterministicRandom } from './common';

/**
 * Returns a deterministic random number between 0 and 1. The number is derived from the
 * current execution index, ensuring deterministic replay.
 * @returns {number} A deterministic pseudo-random number between 0 and 1.
 */
export function random(): number {
  const store = asyncLocalStorage.getStore();
  const COUNTER = store.get('counter');
  const seed = COUNTER.counter = COUNTER.counter + 1;
  return deterministicRandom(seed);
}
