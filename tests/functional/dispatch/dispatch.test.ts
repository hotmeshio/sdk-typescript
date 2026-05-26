import { describe, it, expect } from 'vitest';

import { DuplicateJobError } from '../../../modules/errors';
import { processStreamMessage } from '../../../services/engine/dispatch';
import { StreamDataType, StreamStatus } from '../../../types/stream';

/**
 * Simulates the crash-recovery scenario where a child workflow
 * already exists when the engine replays an AWAIT message.
 *
 * Without the fix: DuplicateJobError propagates, the stream message
 * is nack'd, and the engine retries indefinitely (poison loop).
 *
 * With the fix: DuplicateJobError is caught in dispatchAwait,
 * the message is acknowledged, and the child delivers its result
 * via the normal RESULT path.
 */
describe('dispatch | AWAIT duplicate child handling', () => {
  it('should not throw when Trigger.process raises DuplicateJobError', async () => {
    const logs: Array<{ event: string; data: any }> = [];

    // Mock engine instance with a Trigger that throws DuplicateJobError
    const mockInstance = {
      logger: {
        debug: (event: string, data?: any) => logs.push({ event, data }),
        info: (event: string, data?: any) => logs.push({ event, data }),
        error: (event: string, data?: any) => logs.push({ event, data }),
      },
      initActivity: async () => ({
        process: async () => {
          throw new DuplicateJobError('child-job-123');
        },
      }),
    };

    const streamData = {
      metadata: {
        jid: 'parent-job-456',
        gid: 'guid-abc',
        dad: ',0,5,0',
        aid: 'collator',
        topic: 'child.workflow',
        await: true,
      },
      type: StreamDataType.AWAIT,
      status: StreamStatus.SUCCESS,
      code: 200,
      data: {},
    };

    // This should NOT throw — the DuplicateJobError should be caught
    await expect(
      processStreamMessage(mockInstance as any, streamData as any),
    ).resolves.toBeUndefined();

    // Verify the info log was emitted
    const dupLog = logs.find((l) => l.event === 'dispatch-await-child-exists');
    expect(dupLog).toBeDefined();
    expect(dupLog?.data?.childJobId).toBe('child-job-123');
    expect(dupLog?.data?.parentJobId).toBe('parent-job-456');
  });

  it('should still throw non-DuplicateJobError errors', async () => {
    const mockInstance = {
      logger: {
        debug: () => {},
        info: () => {},
        error: () => {},
      },
      initActivity: async () => ({
        process: async () => {
          throw new Error('unexpected failure');
        },
      }),
    };

    const streamData = {
      metadata: {
        jid: 'parent-job-789',
        gid: 'guid-def',
        dad: ',0,0',
        aid: 'trigger',
        topic: 'child.workflow',
        await: true,
      },
      type: StreamDataType.AWAIT,
      status: StreamStatus.SUCCESS,
      code: 200,
      data: {},
    };

    await expect(
      processStreamMessage(mockInstance as any, streamData as any),
    ).rejects.toThrow('unexpected failure');
  });
});
