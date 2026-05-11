import { guid, sleepFor } from '../../../modules/utils';
import {
  HMSH_CODE_UNACKED,
  HMSH_CODE_UNKNOWN,
  HMSH_STATUS_UNKNOWN,
  HMSH_MAX_RETRIES,
} from '../config';
import {
  StreamData,
  StreamDataResponse,
  StreamError,
  StreamStatus,
  StreamDataType,
  RetryPolicy,
} from '../../../types/stream';

export class ErrorHandler {
  shouldRetry(
    input: StreamData,
    output: StreamDataResponse,
    retry?: RetryPolicy,
  ): [boolean, number] {
    const tryCount = input.metadata.try || 0;
    
    // Priority 1: Use structured retry policy (from stream columns or config)
    if (retry) {
      const maxAttempts = retry.maximumAttempts || 3;
      const backoffCoeff = retry.backoffCoefficient || 10;
      const maxInterval = typeof retry.maximumInterval === 'string'
        ? parseInt(retry.maximumInterval)
        : (retry.maximumInterval || 120);
      const initialIntervalS = (retry as any).initialInterval || 1;

      if ((tryCount + 1) < maxAttempts) {
        // Exponential backoff: min(initialInterval * coefficient^(try+1), maxInterval)
        const backoffSeconds = Math.min(
          initialIntervalS * Math.pow(backoffCoeff, tryCount + 1),
          maxInterval
        );
        return [true, backoffSeconds * 1000];
      }
      return [false, 0];
    }
    
    // Priority 2: Use message-level policies (existing behavior)
    const policies = input.policies?.retry;
    const errorCode = output.code.toString();
    const policy = policies?.[errorCode];
    const maxRetries = policy?.[0];
    const cappedTryCount = Math.min(tryCount, HMSH_MAX_RETRIES);
    
    if (maxRetries > cappedTryCount) {
      // 10ms, 100ms, or 1000ms delays between system retries
      return [true, Math.pow(10, cappedTryCount + 1)];
    }
    
    return [false, 0];
  }

  structureUnhandledError(input: StreamData, err: Error): StreamDataResponse {
    const error: Partial<StreamError> = {};
    if (typeof err.message === 'string') {
      error.message = err.message;
    } else {
      error.message = HMSH_STATUS_UNKNOWN;
    }
    if (typeof err.stack === 'string') {
      error.stack = err.stack;
    }
    if (typeof err.name === 'string') {
      error.name = err.name;
    }
    const result = {
      status: 'error',
      code: (err as any).code || HMSH_CODE_UNKNOWN,
      metadata: { ...input.metadata, guid: guid() },
      data: error as StreamError,
    } as StreamDataResponse;
    if ((input as any)._retryAttempt != null) {
      (result as any)._retryAttempt = (input as any)._retryAttempt;
    }
    return result;
  }

  structureUnacknowledgedError(input: StreamData): StreamDataResponse {
    const message = 'stream message max delivery count exceeded';
    const code = HMSH_CODE_UNACKED;
    const data: StreamError = { message, code };
    const output: StreamDataResponse = {
      metadata: { ...input.metadata, guid: guid() },
      status: StreamStatus.ERROR,
      code,
      data,
    };
    //send unacknowleded errors to the engine (it has no topic)
    delete output.metadata.topic;
    return output;
  }

  structureError(
    input: StreamData,
    output: StreamDataResponse,
  ): StreamDataResponse {
    const message = output.data?.message
      ? output.data?.message.toString()
      : HMSH_STATUS_UNKNOWN;
    const statusCode = output.code || output.data?.code;
    const code = isNaN(statusCode as number)
      ? HMSH_CODE_UNKNOWN
      : parseInt(statusCode.toString());
    const stack = output.data?.stack
      ? output.data?.stack.toString()
      : undefined;
    const data: StreamError = { message, code, stack };
    if (typeof output.data?.error === 'object') {
      data.error = { ...output.data.error };
    }
    return {
      status: StreamStatus.ERROR,
      code,
      stack,
      metadata: { ...input.metadata, guid: guid() },
      data,
    } as StreamDataResponse;
  }

  async handleRetry(
    input: StreamData,
    output: StreamDataResponse,
    publishMessage: (
      topic: string,
      streamData: StreamData | StreamDataResponse,
    ) => Promise<string>,
    retry?: RetryPolicy,
    onRetryScheduled?: (topic: string, delayMs: number) => void,
  ): Promise<string> {
    const [shouldRetry, timeout] = this.shouldRetry(input, output, retry);
    if (shouldRetry) {
      // Only sleep if no retry (legacy behavior for backward compatibility)
      // With retry, use visibility timeout instead of in-memory sleep
      if (!retry) {
        await sleepFor(timeout);
      }

      // Create new message with incremented try count
      const newMessage: any = {
        data: input.data,
        metadata: { ...input.metadata, try: (input.metadata.try || 0) + 1 },
        policies: input.policies,
      };

      // Propagate retry config to new message (for immutable pattern)
      if ((input as any)._streamRetryConfig) {
        newMessage._streamRetryConfig = (input as any)._streamRetryConfig;
      }

      // Add visibility delay for production-ready retry with retry
      if (retry && timeout > 0) {
        newMessage._visibilityDelayMs = timeout;
      }

      // Track retry attempt count in database
      const currentAttempt = (input as any)._retryAttempt || 0;
      newMessage._retryAttempt = currentAttempt + 1;

      const messageId = (await publishMessage(input.metadata.topic, newMessage)) as string;

      // Schedule a targeted NOTIFY so the consumer wakes up when
      // the visibility-delayed message becomes visible
      if (retry && timeout > 0 && onRetryScheduled) {
        onRetryScheduled(input.metadata.topic, timeout);
      }

      return messageId;
    } else {
      const structuredError = this.structureError(input, output);
      (structuredError as any)._retryAttempt =
        ((input as any)._retryAttempt || 0) + 1;
      return (await publishMessage(null, structuredError)) as string;
    }
  }
}
