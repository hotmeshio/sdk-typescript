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
    retryPolicy?: RetryPolicy,
  ): [boolean, number] {
    const tryCount = input.metadata.try || 0;
    
    // Priority 1: Use structured retry policy (from stream columns or config)
    if (retryPolicy) {
      const maxAttempts = retryPolicy.maximumAttempts || 3;
      const backoffCoeff = retryPolicy.backoffCoefficient || 10;
      const maxInterval = typeof retryPolicy.maximumInterval === 'string'
        ? parseInt(retryPolicy.maximumInterval)
        : (retryPolicy.maximumInterval || 120);
      
      if (tryCount < maxAttempts) {
        // Exponential backoff: min(coefficient^try, maxInterval)
        const backoffSeconds = Math.min(
          Math.pow(backoffCoeff, tryCount),
          maxInterval
        );
        return [true, backoffSeconds * 1000]; // Convert to milliseconds
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
    return {
      status: 'error',
      code: HMSH_CODE_UNKNOWN,
      metadata: { ...input.metadata, guid: guid() },
      data: error as StreamError,
    } as StreamDataResponse;
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
    retryPolicy?: RetryPolicy,
  ): Promise<string> {
    const [shouldRetry, timeout] = this.shouldRetry(input, output, retryPolicy);
    if (shouldRetry) {
      await sleepFor(timeout);
      
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
      
      return (await publishMessage(input.metadata.topic, newMessage)) as string;
    } else {
      const structuredError = this.structureError(input, output);
      return (await publishMessage(null, structuredError)) as string;
    }
  }
}
