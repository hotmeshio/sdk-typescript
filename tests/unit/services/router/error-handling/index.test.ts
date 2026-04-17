import { describe, it, expect, vi } from 'vitest';
import { ErrorHandler } from '../../../../../services/router/error-handling';
import { StreamData, StreamDataResponse } from '../../../../../types/stream';

function makeInput(overrides: Record<string, any> = {}): StreamData {
  return {
    metadata: { guid: 'g1', aid: 'a1', jid: 'j1', topic: 'worker-topic' },
    data: {},
    ...overrides,
  } as StreamData;
}

function makeErrorOutput(
  overrides: Record<string, any> = {},
): StreamDataResponse {
  return {
    status: 'error',
    code: 500,
    data: { message: 'Activity schema not found' },
    metadata: { guid: 'g2', aid: 'a1' },
    ...overrides,
  } as StreamDataResponse;
}

describe('ErrorHandler', () => {
  const handler = new ErrorHandler();

  describe('handleRetry – non-retry path propagates _retryAttempt', () => {
    it('should set _retryAttempt = 1 when input has no _retryAttempt', async () => {
      const input = makeInput();
      const output = makeErrorOutput();
      const publishMessage = vi.fn().mockResolvedValue('msg-id');

      await handler.handleRetry(input, output, publishMessage);

      expect(publishMessage).toHaveBeenCalledOnce();
      const [topic, published] = publishMessage.mock.calls[0];
      expect(topic).toBeNull();
      expect(published._retryAttempt).toBe(1);
    });

    it('should increment _retryAttempt from input value', async () => {
      const input = makeInput({ _retryAttempt: 3 });
      const output = makeErrorOutput();
      const publishMessage = vi.fn().mockResolvedValue('msg-id');

      await handler.handleRetry(input, output, publishMessage);

      const [topic, published] = publishMessage.mock.calls[0];
      expect(topic).toBeNull();
      expect(published._retryAttempt).toBe(4);
    });

    it('should preserve error structure alongside _retryAttempt', async () => {
      const input = makeInput({ _retryAttempt: 0 });
      const output = makeErrorOutput({ code: 404, data: { message: 'Not found', stack: 'stack trace' } });
      const publishMessage = vi.fn().mockResolvedValue('msg-id');

      await handler.handleRetry(input, output, publishMessage);

      const [, published] = publishMessage.mock.calls[0];
      expect(published.status).toBe('error');
      expect(published.data.message).toBe('Not found');
      expect(published._retryAttempt).toBe(1);
    });
  });

  describe('handleRetry – retry path still propagates _retryAttempt', () => {
    it('should increment _retryAttempt on retried messages', async () => {
      const input = makeInput({
        _retryAttempt: 2,
        policies: { retry: { '500': [3] } },
      });
      const output = makeErrorOutput();
      const publishMessage = vi.fn().mockResolvedValue('msg-id');

      await handler.handleRetry(input, output, publishMessage);

      const [topic, published] = publishMessage.mock.calls[0];
      expect(topic).toBe('worker-topic');
      expect(published._retryAttempt).toBe(3);
    });
  });

  describe('structureUnhandledError – propagates _retryAttempt', () => {
    it('should propagate _retryAttempt when present on input', () => {
      const input = makeInput({ _retryAttempt: 4 });
      const result = handler.structureUnhandledError(
        input,
        new Error('test error'),
      );

      expect(result.status).toBe('error');
      expect((result as any)._retryAttempt).toBe(4);
    });

    it('should not add _retryAttempt when absent from input', () => {
      const input = makeInput();
      const result = handler.structureUnhandledError(
        input,
        new Error('test error'),
      );

      expect(result.status).toBe('error');
      expect((result as any)._retryAttempt).toBeUndefined();
    });

    it('should propagate _retryAttempt = 0', () => {
      const input = makeInput({ _retryAttempt: 0 });
      const result = handler.structureUnhandledError(
        input,
        new Error('test error'),
      );

      expect((result as any)._retryAttempt).toBe(0);
    });
  });

  describe('poison message circuit breaker convergence', () => {
    it('should reach poison threshold via non-retry error path', async () => {
      const THRESHOLD = 5;
      let currentMessage = makeInput() as any;

      for (let i = 0; i < THRESHOLD; i++) {
        const output = makeErrorOutput();
        const publishMessage = vi.fn().mockResolvedValue('msg-id');

        await handler.handleRetry(currentMessage, output, publishMessage);

        const [, published] = publishMessage.mock.calls[0];
        expect(published._retryAttempt).toBe(i + 1);

        // Simulate: engine picks up the published error, wraps it via
        // structureUnhandledError (like consumeOne does on exception),
        // then goes back through handleRetry.
        const asInput = {
          ...published,
          metadata: { ...published.metadata, guid: `g-${i}` },
        };
        currentMessage = asInput;
      }

      // After THRESHOLD iterations, _retryAttempt should be >= THRESHOLD
      expect((currentMessage as any)._retryAttempt).toBeGreaterThanOrEqual(
        THRESHOLD,
      );
    });
  });
});
