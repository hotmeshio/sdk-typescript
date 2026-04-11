import { asyncLocalStorage, deterministicRandom } from './common';

/**
 * Returns a deterministic UUID v4 string. The value is derived from the
 * current execution counter, producing the **same UUID** on every replay.
 *
 * Use this instead of `crypto.randomUUID()` inside workflow functions.
 *
 * ```typescript
 * const id = Durable.workflow.uuid4();
 * // e.g. "a3b8f042-1e9c-4d5a-b6e7-3f2c8a9d0e1b"
 * ```
 *
 * @returns {string} A deterministic UUID v4 string.
 */
export function uuid4(): string {
  const store = asyncLocalStorage.getStore();
  const COUNTER = store.get('counter');

  // Generate 16 deterministic pseudo-random bytes
  const bytes: number[] = [];
  for (let i = 0; i < 16; i++) {
    const seed = COUNTER.counter = COUNTER.counter + 1;
    bytes.push(Math.floor(deterministicRandom(seed) * 256));
  }

  // Set version (4) and variant (RFC 4122)
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = bytes.map(b => b.toString(16).padStart(2, '0'));
  return [
    hex.slice(0, 4).join(''),
    hex.slice(4, 6).join(''),
    hex.slice(6, 8).join(''),
    hex.slice(8, 10).join(''),
    hex.slice(10, 16).join(''),
  ].join('-');
}
