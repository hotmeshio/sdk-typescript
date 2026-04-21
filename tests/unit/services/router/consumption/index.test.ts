import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ConsumptionManager } from '../../../../../services/router/consumption';
import { ErrorHandler } from '../../../../../services/router/error-handling';

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
