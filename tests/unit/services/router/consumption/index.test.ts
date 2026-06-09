import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ConsumptionManager } from '../../../../../services/router/consumption';
import { ErrorHandler } from '../../../../../services/router/error-handling';
import { DuressManager } from '../../../../../services/router/duress';
import { StreamDataType } from '../../../../../types/stream';

/**
 * Proves the poison loop: when the ENGINE group's callback throws,
 * the error must NOT be republished to the engine stream (which would
 * cause infinite reprocessing). Instead it should be acked and discarded.
 *
 * Without the fix, `publishResponse` is called unconditionally, sending
 * the error back to the engine stream where it re-enters processStreamMessage,
 * fails again, and loops forever.
 */

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
  return {
    info: vi.fn(),
    debug: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
  };
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
    errorCount: 0,
    counts: {},
    hasReachedMaxBackoff: false,
    throttle: 0,
    reclaimDelay: 60000,
    reclaimCount: 0,
  };
}

describe('ConsumptionManager | Engine Poison Loop Prevention', () => {
  let stream: any;
  let logger: any;
  let manager: any;

  beforeEach(() => {
    vi.resetAllMocks();
    stream = createMockStream();
    logger = createMockLogger();
    const throttleManager = { acquire: vi.fn().mockResolvedValue(undefined), release: vi.fn() };
    const errorHandler = new ErrorHandler();
    const lifecycleManager = createMockLifecycle();
    const router = createMockRouter();

    manager = new ConsumptionManager(
      stream,
      logger,
      throttleManager as any,
      errorHandler,
      lifecycleManager as any,
      60000, // reclaimDelay
      10,    // reclaimCount
      'test-app',
      'ENGINE', // role
      router,
      undefined, // no retry policy
    );
  });

  it('should NOT republish errors to engine stream when ENGINE callback throws', async () => {
    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: '.a1' },
      data: {},
    };

    // Callback that throws (simulates processStreamMessage failing
    // because activity schema not found — code 598 marks it as
    // infrastructure/unprocessable, distinct from application errors)
    const schemaError = new Error('Activity schema not found for "a1" (topic: .a1) in app test-app');
    (schemaError as any).code = 598;
    const callback = vi.fn().mockRejectedValue(schemaError);

    await manager.consumeOne(
      'hmsh:test-app:x:',
      'ENGINE',
      'msg-123',
      input,
      callback,
    );

    // The message MUST be acked (not left dangling)
    expect(stream.ackAndDelete).toHaveBeenCalledWith(
      'hmsh:test-app:x:',
      'ENGINE',
      ['msg-123'],
    );

    // The error MUST NOT be published back to the engine stream
    // (publishing creates the poison loop)
    expect(stream.publishMessages).not.toHaveBeenCalled();
  });

  it('should still publish errors for WORKER group (normal error flow)', async () => {
    // Recreate with WORKER role
    const errorHandler = new ErrorHandler();
    const workerManager = new ConsumptionManager(
      stream,
      logger,
      { acquire: vi.fn().mockResolvedValue(undefined), release: vi.fn() } as any,
      errorHandler,
      createMockLifecycle() as any,
      60000,
      10,
      'test-app',
      'WORKER',
      createMockRouter(),
      undefined,
    );

    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: 'worker-topic' },
      data: {},
    };

    const callback = vi.fn().mockRejectedValue(
      new Error('some worker error'),
    );

    await workerManager.consumeOne(
      'hmsh:test-app:x:worker-topic',
      'WORKER',
      'msg-456',
      input,
      callback,
    );

    // Worker errors SHOULD be published back to engine
    expect(stream.publishMessages).toHaveBeenCalled();
  });
});

describe('ConsumptionManager | Duress Instrumentation', () => {
  let stream: any;
  let logger: any;
  let duressManager: DuressManager;
  let throttleManager: any;

  beforeEach(() => {
    vi.resetAllMocks();
    stream = createMockStream();
    logger = createMockLogger();
    duressManager = new DuressManager();
    throttleManager = {
      customSleep: vi.fn().mockResolvedValue(undefined),
      isPaused: vi.fn().mockReturnValue(false),
      setDuressFloor: vi.fn(),
    };
  });

  function createEngineManager(dm?: DuressManager) {
    return new ConsumptionManager(
      stream,
      logger,
      throttleManager as any,
      new ErrorHandler(),
      createMockLifecycle() as any,
      60000,
      10,
      'test-app',
      'ENGINE',
      createMockRouter(),
      undefined, // no retry
      dm,        // duressManager
    );
  }

  it('records latency after processing a message (engine with duressManager)', async () => {
    const recordSpy = vi.spyOn(duressManager, 'recordLatency');
    const manager = createEngineManager(duressManager);

    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: '.a1' },
      data: {},
      type: StreamDataType.TRANSITION,
    };
    const callback = vi.fn().mockResolvedValue({
      metadata: { guid: 'g2', aid: 'a1', jid: 'j1', gid: 'gid1' },
      data: {},
      status: 'success',
      code: 200,
    });

    await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', 'msg-1', input, callback);

    expect(recordSpy).toHaveBeenCalledTimes(1);
    expect(recordSpy).toHaveBeenCalledWith(
      StreamDataType.TRANSITION,
      expect.any(Number),
    );
    // Duration should be non-negative
    const duration = recordSpy.mock.calls[0][1];
    expect(duration).toBeGreaterThanOrEqual(0);
  });

  it('does NOT record latency for worker routers (no duressManager)', async () => {
    // Create without duressManager (simulates worker router)
    const manager = createEngineManager(undefined);

    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: 'worker-topic' },
      data: {},
      type: StreamDataType.RESPONSE,
    };
    const callback = vi.fn().mockResolvedValue({
      metadata: { guid: 'g2', aid: 'a1', jid: 'j1', gid: 'gid1' },
      data: {},
      status: 'success',
      code: 200,
    });

    // Should complete without error — no duress tracking
    await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', 'msg-2', input, callback);

    // No way to assert duressManager wasn't called since it doesn't exist,
    // but we confirm no crash and the message was acked normally
    expect(stream.ackAndDelete).toHaveBeenCalled();
  });

  it('triggers evaluate and setDuressFloor after EVAL_INTERVAL messages', async () => {
    const evaluateSpy = vi.spyOn(duressManager, 'evaluate');
    const manager = createEngineManager(duressManager);

    const makeInput = (i: number) => ({
      metadata: { guid: `g${i}`, aid: 'a1', jid: `j${i}`, gid: `gid${i}`, topic: '.a1' },
      data: {},
      type: StreamDataType.TRANSITION,
    });
    const callback = vi.fn().mockResolvedValue({
      metadata: { guid: 'out', aid: 'a1', jid: 'j1', gid: 'gid1' },
      data: {},
      status: 'success',
      code: 200,
    });

    // Process 9 messages — should NOT trigger evaluate yet
    for (let i = 0; i < 9; i++) {
      await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', `msg-${i}`, makeInput(i), callback);
    }
    expect(evaluateSpy).not.toHaveBeenCalled();

    // 10th message triggers evaluation (HMSH_DURESS_EVAL_INTERVAL default = 10)
    await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', 'msg-9', makeInput(9), callback);
    expect(evaluateSpy).toHaveBeenCalledTimes(1);
  });

  it('sets duress floor on ThrottleManager when evaluation detects duress', async () => {
    // Pre-load duressManager with very high latency. The 10 near-zero
    // consumeOne calls will decay the EMA (alpha=0.3 → factor 0.7^10 ≈ 0.028),
    // so we need an initial value high enough to stay above 200ms (mild threshold)
    // after decay: 100_000 * 0.028 ≈ 2825ms → moderate duress.
    for (let i = 0; i < 5; i++) {
      duressManager.recordLatency(StreamDataType.TRANSITION, 100_000);
    }

    const manager = createEngineManager(duressManager);

    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: '.a1' },
      data: {},
      type: StreamDataType.TRANSITION,
    };
    // Callback that takes ~0ms (the pre-loaded EMA is what matters)
    const callback = vi.fn().mockResolvedValue({
      metadata: { guid: 'out', aid: 'a1', jid: 'j1', gid: 'gid1' },
      data: {},
      status: 'success',
      code: 200,
    });

    // Process 10 messages to trigger evaluation
    for (let i = 0; i < 10; i++) {
      await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', `msg-${i}`, input, callback);
    }

    // ThrottleManager should have received a non-zero duress floor
    expect(throttleManager.setDuressFloor).toHaveBeenCalled();
    const floorValue = throttleManager.setDuressFloor.mock.calls[0][0];
    expect(floorValue).toBeGreaterThan(0);
  });

  it('fires onDuressChange callback when broadcast is warranted', async () => {
    // Pre-load with very high latency (same reasoning as above —
    // must survive 10 near-zero samples and remain in duress)
    for (let i = 0; i < 5; i++) {
      duressManager.recordLatency(StreamDataType.TRANSITION, 100_000);
    }

    const manager = createEngineManager(duressManager);
    const duressCallback = vi.fn();
    manager.setDuressCallback(duressCallback);

    const input = {
      metadata: { guid: 'g1', aid: 'a1', jid: 'j1', gid: 'gid1', topic: '.a1' },
      data: {},
      type: StreamDataType.TRANSITION,
    };
    const callback = vi.fn().mockResolvedValue({
      metadata: { guid: 'out', aid: 'a1', jid: 'j1', gid: 'gid1' },
      data: {},
      status: 'success',
      code: 200,
    });

    // Process 10 messages to trigger evaluation
    for (let i = 0; i < 10; i++) {
      await manager.consumeOne('hmsh:test-app:x:', 'ENGINE', `msg-${i}`, input, callback);
    }

    // Duress callback should have fired with a snapshot
    expect(duressCallback).toHaveBeenCalledTimes(1);
    const snapshot = duressCallback.mock.calls[0][0];
    expect(snapshot.level).not.toBe('healthy');
    expect(snapshot.throttle_ms).toBeGreaterThan(0);
    expect(snapshot.per_type).toHaveProperty(StreamDataType.TRANSITION);
  });
});
