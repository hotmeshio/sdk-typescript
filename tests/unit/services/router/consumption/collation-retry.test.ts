import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ConsumptionManager } from '../../../../../services/router/consumption';
import { ErrorHandler } from '../../../../../services/router/error-handling';
import { CollationError } from '../../../../../modules/errors';
import { CollationFaultType } from '../../../../../types/collator';
import { HMSH_POISON_MESSAGE_THRESHOLD } from '../../../../../modules/enums';
import { processEvent } from '../../../../../services/activities/activity/process';
import { StreamStatus } from '../../../../../types/stream';

/**
 * Proves the collation duplicate handling:
 *
 * The Postgres collateLeg2Entry is a single atomic CTE with row-level
 * locks. Concurrent writes are serialized by the database. When
 * verifySyntheticInteger throws INACTIVE, the GUID ledger value is
 * correct — the activity was genuinely already processed by a prior
 * delivery of the same message.
 *
 * The correct behavior is silent ack: log and return, don't rethrow.
 * The message is a legitimate duplicate and the work is already done.
 */

// ── processEvent mocks ───────────────────────────────────────��──────

function createMockProcessContext(overrides: Record<string, any> = {}) {
  return {
    config: { subtype: 'test-activity' },
    context: { metadata: { jid: 'j1' }, data: {} },
    metadata: { aid: 'a1' },
    engine: { appId: 'test-app' },
    logger: {
      info: vi.fn(),
      debug: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
    },
    status: StreamStatus.SUCCESS,
    code: 200,
    data: {},
    leg: 2,
    adjacencyList: [],
    adjacentIndex: 0,
    setLeg: vi.fn(),
    verifyReentry: vi.fn(),
    bindActivityError: vi.fn(),
    bindActivityData: vi.fn(),
    bindJobError: vi.fn(),
    filterAdjacent: vi.fn().mockResolvedValue([]),
    mapJobData: vi.fn(),
    executeStepProtocol: vi.fn().mockResolvedValue(true),
    ...overrides,
  } as any;
}

// ── consumeOne mocks ────────────────────────────────────────────────

function createMockStream() {
  return {
    mintKey: vi.fn().mockReturnValue('hmsh:test:x:'),
    publishMessages: vi.fn().mockResolvedValue(['msg-1']),
    ackAndDelete: vi.fn().mockResolvedValue(undefined),
    getProviderSpecificFeatures: vi.fn().mockReturnValue({
      supportsNotifications: false,
      supportsRetry: false,
    }),
    createConsumerGroup: vi.fn().mockResolvedValue(undefined),
  };
}

function createMockLogger() {
  return { info: vi.fn(), debug: vi.fn(), error: vi.fn(), warn: vi.fn() };
}

function createMockLifecycle() {
  return {
    isStopped: vi.fn().mockReturnValue(false),
    getShouldConsume: vi.fn().mockReturnValue(true),
    isReadonly: vi.fn().mockReturnValue(false),
    startConsuming: vi.fn().mockResolvedValue(undefined),
    setIsUsingNotifications: vi.fn(),
  };
}

function createMockRouter() {
  return {
    errorCount: 0, counts: {}, hasReachedMaxBackoff: false,
    throttle: 0, reclaimDelay: 60000, reclaimCount: 0,
  };
}

function makeInput(overrides: Record<string, any> = {}) {
  return {
    metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: 'worker-topic' },
    data: {},
    ...overrides,
  };
}

function createManager(stream: any, logger: any, retry?: any) {
  return new ConsumptionManager(
    stream, logger,
    { acquire: vi.fn().mockResolvedValue(undefined), release: vi.fn() } as any,
    new ErrorHandler(),
    createMockLifecycle() as any,
    60000, 10, 'test-app', 'WORKER', createMockRouter(), retry,
  );
}

// ═════════════════════════════════════════════════════════════════════
// Layer 1: processEvent — INACTIVE is silently acked (correct behavior)
// ═════════════════════════════════════════════════════════════════════

describe('processEvent | CollationError INACTIVE silent ack', () => {
  const inactiveError = new CollationError(
    889000001010001, 2, 'enter', CollationFaultType.INACTIVE,
  );

  it('should silently ack CollationError INACTIVE (legitimate duplicate)', async () => {
    // The Postgres atomic CTE serializes concurrent writes via row locks.
    // INACTIVE means the activity was genuinely already processed.
    // processEvent should catch it and return — not rethrow.
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(inactiveError),
    });

    await expect(processEvent(ctx)).resolves.toBeUndefined();
    expect(ctx.logger.warn).toHaveBeenCalledWith(
      'process-event-inactive-error',
      expect.objectContaining({ jid: 'j1', aid: 'a1' }),
    );
  });

  it('should silently ack all CollationError fault types', async () => {
    const duplicateError = new CollationError(
      889000001010001, 2, 'enter', CollationFaultType.DUPLICATE,
    );
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(duplicateError),
    });

    await expect(processEvent(ctx)).resolves.toBeUndefined();
    expect(ctx.logger.warn).toHaveBeenCalledWith(
      'process-event-duplicate-error',
      expect.objectContaining({ jid: 'j1', aid: 'a1' }),
    );
  });
});

// ═════════════════════════════════════════════════════════════════════
// Layer 1b: processEvent — FORBIDDEN is rethrown (not silently acked)
// ═════════════════════════════════════════════════════════════════════

describe('processEvent | CollationError FORBIDDEN rethrown for retry', () => {
  const forbiddenError = new CollationError(
    1000000000001, 2, 'enter', CollationFaultType.FORBIDDEN,
  );

  it('should rethrow FORBIDDEN so the stream message is retried', async () => {
    // FORBIDDEN = Leg1 not complete; signal arrived between
    // registerHook (standalone) and Leg1 transaction commit.
    // The GUID marker was already committed by notarizeLeg2Entry,
    // so the message MUST be retried — silent ack would lose the signal.
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(forbiddenError),
    });

    await expect(processEvent(ctx)).rejects.toThrow(CollationError);
    expect(ctx.logger.warn).toHaveBeenCalledWith(
      'process-event-forbidden-retry',
      expect.objectContaining({
        jid: 'j1',
        aid: 'a1',
        message: 'Leg1 not committed yet; rethrowing for stream retry',
      }),
    );
  });

  it('FORBIDDEN should NOT delete the hook signal (error propagates past deleteWebHookSignal)', async () => {
    // When processEvent rethrows FORBIDDEN, control never reaches
    // processWebHookEvent's deleteWebHookSignal call. The hook signal
    // is preserved for the retry attempt.
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(forbiddenError),
    });

    const thrown = await processEvent(ctx).catch((e: Error) => e);
    expect(thrown).toBeInstanceOf(CollationError);
    expect((thrown as CollationError).fault).toBe(CollationFaultType.FORBIDDEN);
    // bindActivityData should NOT have been called (error before it)
    expect(ctx.bindActivityData).not.toHaveBeenCalled();
    expect(ctx.mapJobData).not.toHaveBeenCalled();
    expect(ctx.executeStepProtocol).not.toHaveBeenCalled();
  });

  it('INACTIVE should still be silently acked (not rethrown)', async () => {
    // Regression guard: INACTIVE is a legitimate duplicate.
    const inactiveError = new CollationError(
      889000001010001, 2, 'enter', CollationFaultType.INACTIVE,
    );
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(inactiveError),
    });

    await expect(processEvent(ctx)).resolves.toBeUndefined();
  });

  it('DUPLICATE should still be silently acked (not rethrown)', async () => {
    const duplicateError = new CollationError(
      889000001010001, 2, 'enter', CollationFaultType.DUPLICATE,
    );
    const ctx = createMockProcessContext({
      verifyReentry: vi.fn().mockRejectedValue(duplicateError),
    });

    await expect(processEvent(ctx)).resolves.toBeUndefined();
  });
});

// ═════════════════════════════════════════════════════════════════════
// Layer 2: consumeOne — duplicate messages are acked without retry
// ═════════════════════════════════════════════════════════════════════

describe('ConsumptionManager | Duplicate message handling', () => {
  let stream: any;
  let logger: any;

  beforeEach(() => {
    vi.resetAllMocks();
    stream = createMockStream();
    logger = createMockLogger();
  });

  it('callback returning undefined (silent ack) → message acked, NOT republished', async () => {
    // When processEvent silently catches a CollationError, the callback
    // resolves to undefined. consumeOne should ack and NOT republish.
    const manager = createManager(stream, logger);
    const input = makeInput();
    const callback = vi.fn().mockResolvedValue(undefined);

    await manager.consumeOne(
      'hmsh:test-app:x:worker-topic', 'WORKER', 'msg-100', input, callback,
    );

    expect(stream.ackAndDelete).toHaveBeenCalled();
    // No retry, no republish — duplicate is correctly dropped
    expect(stream.publishMessages).not.toHaveBeenCalled();
  });

  it('poison circuit breaker still works for genuine poison messages', async () => {
    const manager = createManager(stream, logger);
    const input = makeInput({ _retryAttempt: HMSH_POISON_MESSAGE_THRESHOLD });
    const callback = vi.fn();

    await manager.consumeOne(
      'hmsh:test-app:x:worker-topic', 'WORKER', 'msg-poison', input, callback,
    );

    expect(callback).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(
      'stream-poison-message-detected',
      expect.objectContaining({
        retryAttempt: HMSH_POISON_MESSAGE_THRESHOLD,
        poisonThreshold: HMSH_POISON_MESSAGE_THRESHOLD,
      }),
    );
    expect(stream.ackAndDelete).toHaveBeenCalled();
  });
});
