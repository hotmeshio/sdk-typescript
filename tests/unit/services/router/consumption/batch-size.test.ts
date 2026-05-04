import { describe, it, expect, vi, afterEach } from 'vitest';

/**
 * Proves the HMSH_BATCH_SIZE env var controls the Postgres batch size.
 *
 * Before the fix, batchSize was hardcoded to 10. After the fix, it reads
 * from process.env.HMSH_BATCH_SIZE via the HMSH_BATCH_SIZE constant
 * (modules/enums.ts), which is re-exported through the router config barrel.
 *
 * This test dynamically imports the enum module to pick up the env var
 * at parse time, matching how the production code loads it.
 */

describe('HMSH_BATCH_SIZE env var', () => {
  const originalEnv = process.env.HMSH_BATCH_SIZE;

  afterEach(() => {
    // Restore original env
    if (originalEnv === undefined) {
      delete process.env.HMSH_BATCH_SIZE;
    } else {
      process.env.HMSH_BATCH_SIZE = originalEnv;
    }
    vi.resetModules();
  });

  it('should default to 10 when env var is not set', async () => {
    delete process.env.HMSH_BATCH_SIZE;
    vi.resetModules();
    const enums = await import('../../../../../modules/enums');
    expect(enums.HMSH_BATCH_SIZE).toBe(10);
  });

  it('should respect HMSH_BATCH_SIZE env var', async () => {
    process.env.HMSH_BATCH_SIZE = '3';
    vi.resetModules();
    const enums = await import('../../../../../modules/enums');
    expect(enums.HMSH_BATCH_SIZE).toBe(3);
  });

  it('should allow HMSH_BATCH_SIZE=1 to disable parallelism', async () => {
    process.env.HMSH_BATCH_SIZE = '1';
    vi.resetModules();
    const enums = await import('../../../../../modules/enums');
    expect(enums.HMSH_BATCH_SIZE).toBe(1);
  });
});
