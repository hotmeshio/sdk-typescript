import {
  HMSH_BLOCK_TIME_MS,
  HMSH_MAX_RETRIES,
  HMSH_MAX_TIMEOUT_MS,
  HMSH_GRADUATED_INTERVAL_MS,
  HMSH_CODE_UNACKED,
  HMSH_CODE_UNKNOWN,
  HMSH_STATUS_UNKNOWN,
  HMSH_XCLAIM_COUNT,
  HMSH_XCLAIM_DELAY_MS,
  HMSH_XPENDING_COUNT,
  MAX_DELAY,
  MAX_STREAM_BACKOFF,
  INITIAL_STREAM_BACKOFF,
  MAX_STREAM_RETRIES,
} from '../../modules/enums';
import { KeyType } from '../../modules/key';
import { guid, sleepFor } from '../../modules/utils';
import { ILogger } from '../logger';
import { StreamService } from '../stream';
import { TelemetryService } from '../telemetry';
import {
  RouterConfig,
  StreamData,
  StreamDataResponse,
  StreamDataType,
  StreamError,
  StreamMessage,
  StreamRole,
  StreamStatus,
} from '../../types/stream';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

class Router<S extends StreamService<ProviderClient, ProviderTransaction>> {
  static instances: Set<Router<any>> = new Set();
  appId: string;
  guid: string;
  hasReachedMaxBackoff: boolean | undefined;
  role: StreamRole;
  topic: string | undefined;
  stream: S;
  reclaimDelay: number;
  reclaimCount: number;
  logger: ILogger;
  throttle = 0;
  errorCount = 0;
  counts: { [key: string]: number } = {};
  currentTimerId: NodeJS.Timeout | null = null;
  shouldConsume: boolean;
  sleepPromiseResolve: (() => void) | null = null;
  innerPromiseResolve: (() => void) | null = null;
  isSleeping = false;
  sleepTimout: NodeJS.Timeout | null = null;
  readonly: boolean;

  constructor(config: RouterConfig, stream: S, logger: ILogger) {
    this.appId = config.appId;
    this.guid = config.guid;
    this.role = config.role;
    this.topic = config.topic;
    this.stream = stream;
    this.throttle = config.throttle;
    this.reclaimDelay = config.reclaimDelay || HMSH_XCLAIM_DELAY_MS;
    this.reclaimCount = config.reclaimCount || HMSH_XCLAIM_COUNT;
    this.logger = logger;
    this.readonly = config.readonly || false;
    this.resetThrottleState();
  }

  private resetThrottleState() {
    this.sleepPromiseResolve = null;
    this.innerPromiseResolve = null;
    this.isSleeping = false;
    this.sleepTimout = null;
  }

  async createGroup(stream: string, group: string) {
    try {
      await this.stream.createConsumerGroup(stream, group);
    } catch (err) {
      this.logger.debug('router-stream-group-exists', { stream, group });
    }
  }

  async publishMessage(
    topic: string,
    streamData: StreamData | StreamDataResponse,
    transaction?: ProviderTransaction,
  ): Promise<string | ProviderTransaction> {
    const code = streamData?.code || '200';
    this.counts[code] = (this.counts[code] || 0) + 1;

    const stream = this.stream.mintKey(KeyType.STREAMS, { topic });
    const responses = await this.stream.publishMessages(
      stream,
      [JSON.stringify(streamData)],
      { transaction },
    );
    return responses[0];
  }

  /**
   * An adjustable throttle that will interrupt a sleeping
   * router if the throttle is reduced and the sleep time
   * has elapsed. If the throttle is increased, or if
   * the sleep time has not elapsed, the router will continue
   * to sleep until the new termination point. This
   * allows for dynamic, elastic throttling with smooth
   * acceleration and deceleration.
   */
  public async customSleep(): Promise<void> {
    if (this.throttle === 0) return;
    if (this.isSleeping) return;
    this.isSleeping = true;
    const startTime = Date.now(); //anchor the origin

    await new Promise<void>(async (outerResolve) => {
      this.sleepPromiseResolve = outerResolve;
      let elapsedTime = Date.now() - startTime;
      while (elapsedTime < this.throttle) {
        await new Promise<void>((innerResolve) => {
          this.innerPromiseResolve = innerResolve;
          this.sleepTimout = setTimeout(
            innerResolve,
            this.throttle - elapsedTime,
          );
        });
        elapsedTime = Date.now() - startTime;
      }
      this.resetThrottleState();
      outerResolve();
    });
  }

  async consumeMessages(
    stream: string,
    group: string,
    consumer: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    // exit early if readonly
    if (this.readonly) {
      this.logger.info(`router-stream-readonly`, { group, consumer, stream });
      return;
    }
    this.logger.info(`router-stream-starting`, { group, consumer, stream });
    Router.instances.add(this);
    this.shouldConsume = true;
    await this.createGroup(stream, group);
    
    let lastCheckedPendingMessagesAt = Date.now();
  
    // Track if we've hit maximum backoff. Initially false.
    // When true, we use the fallback mode: single attempt, no backoff, then sleep 3s if empty.
    if (typeof this.hasReachedMaxBackoff === 'undefined') {
      this.hasReachedMaxBackoff = false;
    }
  
    const sleepFor = (ms: number) =>
      new Promise<void>((resolve) => setTimeout(resolve, ms));
  
    const consume = async () => {
      await this.customSleep(); // always respect the global throttle
      if (this.isStopped(group, consumer, stream)) {
        return;
      } else if (this.isPaused()) {
        setImmediate(consume.bind(this));
        return;
      }
  
      // Randomizer that asymptotes at 150% of `HMSH_BLOCK_TIME_MS`
      const streamDuration =
        HMSH_BLOCK_TIME_MS + Math.round(HMSH_BLOCK_TIME_MS * Math.random());
  
      try {
        let messages: StreamMessage[] = [];
  
        if (!this.hasReachedMaxBackoff) {
          // Normal mode: try with backoff and finite retries
          messages = await this.stream.consumeMessages(stream, group, consumer, {
            blockTimeout: streamDuration,
            enableBackoff: true,
            initialBackoff: INITIAL_STREAM_BACKOFF,
            maxBackoff: MAX_STREAM_BACKOFF,
            maxRetries: MAX_STREAM_RETRIES,
          });
        } else {
          // Fallback mode: just try once, no backoff
          messages = await this.stream.consumeMessages(stream, group, consumer, {
            blockTimeout: streamDuration,
            enableBackoff: false,
            maxRetries: 1,
          });
        }
  
        if (this.isStopped(group, consumer, stream)) {
          return;
        } else if (this.isPaused()) {
          setImmediate(consume.bind(this));
          return;
        }
  
        if (messages.length > 0) {
          // Reset if we were in fallback mode
          this.hasReachedMaxBackoff = false;
  
          // Process messages
          for (const message of messages) {
            await this.consumeOne(stream, group, message.id, message.data, callback);
          }
  
          // Check for pending messages if supported and enough time has passed
          const now = Date.now();
          if (
            this.stream.getProviderSpecificFeatures().supportsRetry &&
            now - lastCheckedPendingMessagesAt > this.reclaimDelay
          ) {
            lastCheckedPendingMessagesAt = now;
            const pendingMessages = await this.stream.retryMessages(stream, group, {
              consumerName: consumer,
              minIdleTime: this.reclaimDelay,
              limit: HMSH_XPENDING_COUNT,
            });
  
            // Process reclaimed messages
            for (const message of pendingMessages) {
              await this.consumeOne(stream, group, message.id, message.data, callback);
            }
          }

          // If we got messages, just continue as normal
          setImmediate(consume.bind(this));
        } else {
          // No messages found
          if (!this.hasReachedMaxBackoff) {
            // We were in normal mode and got no messages after maxRetries
            // Switch to fallback mode
            this.hasReachedMaxBackoff = true;
          } else {
            // We are already in fallback mode, still no messages
            // Sleep for MAX_STREAM_BACKOFF ms before trying again
            await sleepFor(MAX_STREAM_BACKOFF);
          }
  
          // Try again after sleeping
          setImmediate(consume.bind(this));
        }
  
      } catch (error) {
        if (this.shouldConsume && process.env.NODE_ENV !== 'test') {
          this.logger.error(`router-stream-error`, {
            error,
            stream,
            group,
            consumer,
          });
          this.errorCount++;
          const timeout = Math.min(
            HMSH_GRADUATED_INTERVAL_MS * 2 ** this.errorCount,
            HMSH_MAX_TIMEOUT_MS,
          );
          setTimeout(consume.bind(this), timeout);
        }
      }
    };
  
    consume.call(this);
  }

  isStreamMessage(result: any): boolean {
    return Array.isArray(result) && Array.isArray(result[0]);
  }

  isPaused(): boolean {
    return this.throttle === MAX_DELAY;
  }

  isStopped(group: string, consumer: string, stream: string): boolean {
    if (!this.shouldConsume) {
      this.logger.info(`router-stream-stopped`, {
        group,
        consumer,
        stream,
      });
    }
    return !this.shouldConsume;
  }

  async consumeOne(
    stream: string,
    group: string,
    id: string,
    input: StreamData,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ) {
    this.logger.debug(`stream-read-one`, { group, stream, id });
    let output: StreamDataResponse | void;
    let telemetry: TelemetryService;
    try {
      telemetry = new TelemetryService(this.appId);
      telemetry.startStreamSpan(input, this.role);
      output = await this.execStreamLeg(input, stream, id, callback.bind(this));
      if (output?.status === StreamStatus.ERROR) {
        telemetry.setStreamError(
          `Function Status Code ${output.code || HMSH_CODE_UNKNOWN}`,
        );
      }
      this.errorCount = 0;
    } catch (err) {
      this.logger.error(`stream-read-one-error`, { group, stream, id, err });
      telemetry.setStreamError(err.message);
    }
    const messageId = await this.publishResponse(input, output);
    telemetry.setStreamAttributes({ 'app.worker.mid': messageId });
    await this.ackAndDelete(stream, group, id);
    telemetry.endStreamSpan();
    this.logger.debug(`stream-read-one-end`, { group, stream, id });
  }

  async execStreamLeg(
    input: StreamData,
    stream: string,
    id: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ) {
    let output: StreamDataResponse | void;
    try {
      output = await callback(input);
    } catch (error) {
      this.logger.error(`stream-call-function-error`, {
        error,
        input: input,
        stack: error.stack,
        message: error.message,
        name: error.name,
        stream,
        id,
      });
      output = this.structureUnhandledError(input, error);
    }
    return output as StreamDataResponse;
  }

  async ackAndDelete(stream: string, group: string, id: string) {
    await this.stream.ackAndDelete(stream, group, [id]);
  }

  async publishResponse(
    input: StreamData,
    output: StreamDataResponse | void,
  ): Promise<string> {
    if (output && typeof output === 'object') {
      if (output.status === 'error') {
        const [shouldRetry, timeout] = this.shouldRetry(input, output);
        if (shouldRetry) {
          await sleepFor(timeout);
          return (await this.publishMessage(input.metadata.topic, {
            data: input.data,
            //note: retain guid (this is a retry attempt)
            metadata: { ...input.metadata, try: (input.metadata.try || 0) + 1 },
            policies: input.policies,
          })) as string;
        } else {
          output = this.structureError(input, output);
        }
      } else if (typeof output.metadata !== 'object') {
        output.metadata = { ...input.metadata, guid: guid() };
      } else {
        output.metadata.guid = guid();
      }
      output.type = StreamDataType.RESPONSE;
      return (await this.publishMessage(
        null,
        output as StreamDataResponse,
      )) as string;
    }
  }

  shouldRetry(
    input: StreamData,
    output: StreamDataResponse,
  ): [boolean, number] {
    //const isUnhandledEngineError = output.code === 500;
    const policies = input.policies?.retry;
    const errorCode = output.code.toString();
    const policy = policies?.[errorCode];
    const maxRetries = policy?.[0];
    const tryCount = Math.min(input.metadata.try || 0, HMSH_MAX_RETRIES);
    //only possible values for maxRetries are 1, 2, 3
    //only possible values for tryCount are 0, 1, 2
    if (maxRetries > tryCount) {
      // 10ms, 100ms, or 1000ms delays between system retries
      return [true, Math.pow(10, tryCount + 1)];
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

  structureUnacknowledgedError(input: StreamData) {
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

  static async stopConsuming() {
    for (const instance of [...Router.instances]) {
      instance.stopConsuming();
    }
    await sleepFor(HMSH_BLOCK_TIME_MS * 2);
  }

  async stopConsuming() {
    this.shouldConsume = false;
    this.logger.info(
      `router-stream-stopping`,
      this.topic ? { topic: this.topic } : undefined,
    );
    this.cancelThrottle();
  }

  cancelThrottle() {
    if (this.sleepTimout) {
      clearTimeout(this.sleepTimout);
    }
    this.resetThrottleState();
  }

  public setThrottle(delayInMillis: number): void {
    if (
      !Number.isInteger(delayInMillis) ||
      delayInMillis < 0 ||
      delayInMillis > MAX_DELAY
    ) {
      throw new Error(
        `Throttle must be a non-negative integer and not exceed ${MAX_DELAY} ms; send -1 to throttle indefinitely`,
      );
    }
    const wasDecreased = delayInMillis < this.throttle;
    this.throttle = delayInMillis;

    // If the throttle was decreased, and we're in the middle of a sleep cycle, adjust immediately
    if (wasDecreased) {
      if (this.sleepTimout) {
        clearTimeout(this.sleepTimout);
      }
      if (this.innerPromiseResolve) {
        this.innerPromiseResolve();
      }
    }
  }
}

export { Router };
