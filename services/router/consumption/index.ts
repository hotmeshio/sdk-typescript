import { guid } from '../../../modules/utils';
import { ILogger } from '../../logger';
import { StreamService } from '../../stream';
import { ThrottleManager } from '../throttling';
import { ErrorHandler } from '../error-handling';
import { RouterTelemetry } from '../telemetry';
import { LifecycleManager } from '../lifecycle';
import {
  HMSH_BLOCK_TIME_MS,
  HMSH_GRADUATED_INTERVAL_MS,
  HMSH_MAX_TIMEOUT_MS,
  HMSH_XPENDING_COUNT,
  MAX_STREAM_BACKOFF,
  INITIAL_STREAM_BACKOFF,
  MAX_STREAM_RETRIES,
} from '../config';
import {
  StreamData,
  StreamDataResponse,
  StreamDataType,
  StreamMessage,
  StreamStatus,
} from '../../../types/stream';
import { ProviderClient, ProviderTransaction } from '../../../types/provider';
import { KeyType } from '../../../modules/key';

export class ConsumptionManager<
  S extends StreamService<ProviderClient, ProviderTransaction>,
> {
  private stream: S;
  private logger: ILogger;
  private throttleManager: ThrottleManager;
  private errorHandler: ErrorHandler;
  private lifecycleManager: LifecycleManager<S>;
  private reclaimDelay: number;
  private reclaimCount: number;
  private appId: string;
  private role: any;
  private errorCount = 0;
  private counts: { [key: string]: number } = {};
  private hasReachedMaxBackoff: boolean | undefined;
  private router: any;

  constructor(
    stream: S,
    logger: ILogger,
    throttleManager: ThrottleManager,
    errorHandler: ErrorHandler,
    lifecycleManager: LifecycleManager<S>,
    reclaimDelay: number,
    reclaimCount: number,
    appId: string,
    role: any,
    router: any,
  ) {
    this.stream = stream;
    this.logger = logger;
    this.throttleManager = throttleManager;
    this.errorHandler = errorHandler;
    this.lifecycleManager = lifecycleManager;
    this.reclaimDelay = reclaimDelay;
    this.reclaimCount = reclaimCount;
    this.appId = appId;
    this.role = role;
    this.router = router;
  }

  async createGroup(stream: string, group: string): Promise<void> {
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

    // Extract retry policy from child workflow (590) and activity (591) message data
    // ONLY if values differ from YAML defaults (10, 3/5, 120)
    // If they're defaults, let old retry mechanism (policies.retry) handle it
    const codeNum = typeof code === 'number' ? code : parseInt(code, 10);
    if ((codeNum === 590 || codeNum === 591) && streamData.data) {
      const data = streamData.data as any;
      const backoff = data.backoffCoefficient;
      const attempts = data.maximumAttempts;
      const maxInterval = typeof data.maximumInterval === 'string' 
        ? parseInt(data.maximumInterval) 
        : data.maximumInterval;
      
      // Only extract if values are NOT the YAML defaults
      // YAML defaults: backoffCoefficient=10, maximumAttempts=3 or 5, maximumInterval=120
      const hasNonDefaultBackoff = backoff != null && backoff !== 10;
      const hasNonDefaultAttempts = attempts != null && attempts !== 3 && attempts !== 5;
      const hasNonDefaultInterval = maxInterval != null && maxInterval !== 120;
      
      if (hasNonDefaultBackoff || hasNonDefaultAttempts || hasNonDefaultInterval) {
        // Has custom values from config - add _streamRetryConfig
        (streamData as any)._streamRetryConfig = {
          max_retry_attempts: attempts,
          backoff_coefficient: backoff,
          maximum_interval_seconds: maxInterval,
        };
      }
    }

    const stream = this.stream.mintKey(KeyType.STREAMS, { topic });
    const responses = await this.stream.publishMessages(
      stream,
      [JSON.stringify(streamData)],
      { transaction },
    );
    return responses[0];
  }

  async consumeMessages(
    stream: string,
    group: string,
    consumer: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    // exit early if readonly
    if (this.lifecycleManager.isReadonly()) {
      this.logger.info(`router-stream-readonly`, { group, consumer, stream });
      return;
    }
    this.logger.info(`router-stream-starting`, { group, consumer, stream });
    await this.lifecycleManager.startConsuming(this.router);
    await this.createGroup(stream, group);

    // Check if notifications are supported
    const features = this.stream.getProviderSpecificFeatures();
    const supportsNotifications = features.supportsNotifications;

    if (supportsNotifications) {
      this.logger.info(`router-stream-using-notifications`, {
        group,
        consumer,
        stream,
      });
      this.lifecycleManager.setIsUsingNotifications(true);
      return this.consumeWithNotifications(stream, group, consumer, callback);
    } else {
      this.logger.info(`router-stream-using-polling`, {
        group,
        consumer,
        stream,
      });
      this.lifecycleManager.setIsUsingNotifications(false);
      return this.consumeWithPolling(stream, group, consumer, callback);
    }
  }

  private async consumeWithNotifications(
    stream: string,
    group: string,
    consumer: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    let lastCheckedPendingMessagesAt = Date.now();

    // Set up notification-based consumption
    const notificationCallback = async (messages: StreamMessage[]) => {
      if (this.lifecycleManager.isStopped(group, consumer, stream)) {
        return;
      }

      await this.throttleManager.customSleep(); // respect throttle

      if (
        this.lifecycleManager.isStopped(group, consumer, stream) ||
        this.throttleManager.isPaused()
      ) {
        return;
      }

      // Process messages - use parallel processing for PostgreSQL
      const features = this.stream.getProviderSpecificFeatures();
      const isPostgres = features.supportsNotifications; // Only PostgreSQL supports notifications currently

      if (isPostgres && messages.length > 1) {
        // Parallel processing for PostgreSQL batches
        this.logger.debug('postgres-stream-parallel-processing', {
          streamName: stream,
          groupName: group,
          messageCount: messages.length,
        });

        const processingStart = Date.now();
        const processingPromises = messages.map(async (message) => {
          if (this.lifecycleManager.isStopped(group, consumer, stream)) {
            return;
          }
          return this.consumeOne(
            stream,
            group,
            message.id,
            message.data,
            callback,
          );
        });

        // Process all messages in parallel
        await Promise.allSettled(processingPromises);

        this.logger.debug('postgres-stream-parallel-processing-complete', {
          streamName: stream,
          groupName: group,
          messageCount: messages.length,
          processingDuration: Date.now() - processingStart,
        });
      } else {
        // Sequential processing for other providers or single messages
        for (const message of messages) {
          if (this.lifecycleManager.isStopped(group, consumer, stream)) {
            return;
          }
          await this.consumeOne(
            stream,
            group,
            message.id,
            message.data,
            callback,
          );
        }
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

        // Process reclaimed messages (also parallel for PostgreSQL)
        if (isPostgres && pendingMessages.length > 1) {
          const reclaimPromises = pendingMessages.map(async (message) => {
            if (this.lifecycleManager.isStopped(group, consumer, stream)) {
              return;
            }
            return this.consumeOne(
              stream,
              group,
              message.id,
              message.data,
              callback,
            );
          });
          await Promise.allSettled(reclaimPromises);
        } else {
          for (const message of pendingMessages) {
            if (this.lifecycleManager.isStopped(group, consumer, stream)) {
              return;
            }
            await this.consumeOne(
              stream,
              group,
              message.id,
              message.data,
              callback,
            );
          }
        }
      }
    };

    try {
      // Start notification-based consumption - this should return immediately after setup
      await this.stream.consumeMessages(stream, group, consumer, {
        enableNotifications: true,
        notificationCallback,
        blockTimeout: HMSH_BLOCK_TIME_MS,
      });

      // Don't block here - let the worker initialization complete
      // The notification system will handle message processing asynchronously
    } catch (error) {
      this.logger.error(`router-stream-notification-error`, {
        error,
        stream,
        group,
        consumer,
      });

      // Fall back to polling if notifications fail
      this.logger.info(`router-stream-fallback-to-polling`, {
        group,
        consumer,
        stream,
      });
      this.lifecycleManager.setIsUsingNotifications(false);
      return this.consumeWithPolling(stream, group, consumer, callback);
    }
  }

  private async consumeWithPolling(
    stream: string,
    group: string,
    consumer: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    let lastCheckedPendingMessagesAt = Date.now();

    // Track if we've hit maximum backoff. Initially false.
    // When true, we use the fallback mode: single attempt, no backoff, then sleep 3s if empty.
    if (typeof this.hasReachedMaxBackoff === 'undefined') {
      this.hasReachedMaxBackoff = false;
    }

    const sleepFor = (ms: number) =>
      new Promise<void>((resolve) => setTimeout(resolve, ms));

    const consume = async () => {
      await this.throttleManager.customSleep(); // always respect the global throttle
      if (this.lifecycleManager.isStopped(group, consumer, stream)) {
        return;
      } else if (this.throttleManager.isPaused()) {
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
          const features = this.stream.getProviderSpecificFeatures();
          const isPostgres = features.supportsNotifications; // Only PostgreSQL supports notifications
          const batchSize = isPostgres ? 10 : 1; // Use batch size of 10 for PostgreSQL, 1 for others

          messages = await this.stream.consumeMessages(
            stream,
            group,
            consumer,
            {
              blockTimeout: streamDuration,
              batchSize,
              enableBackoff: true,
              initialBackoff: INITIAL_STREAM_BACKOFF,
              maxBackoff: MAX_STREAM_BACKOFF,
              maxRetries: MAX_STREAM_RETRIES,
            },
          );
        } else {
          // Fallback mode: just try once, no backoff
          const features = this.stream.getProviderSpecificFeatures();
          const isPostgres = features.supportsNotifications; // Only PostgreSQL supports notifications
          const batchSize = isPostgres ? 10 : 1; // Use batch size of 10 for PostgreSQL, 1 for others

          messages = await this.stream.consumeMessages(
            stream,
            group,
            consumer,
            {
              blockTimeout: streamDuration,
              batchSize,
              enableBackoff: false,
              maxRetries: 1,
            },
          );
        }

        if (this.lifecycleManager.isStopped(group, consumer, stream)) {
          return;
        } else if (this.throttleManager.isPaused()) {
          setImmediate(consume.bind(this));
          return;
        }

        if (messages.length > 0) {
          // Reset if we were in fallback mode
          this.hasReachedMaxBackoff = false;

          // Process messages - use parallel processing for PostgreSQL
          const features = this.stream.getProviderSpecificFeatures();
          const isPostgres = features.supportsNotifications; // Only PostgreSQL supports notifications currently

          if (isPostgres && messages.length > 1) {
            // Parallel processing for PostgreSQL batches
            this.logger.debug('postgres-stream-parallel-processing-polling', {
              streamName: stream,
              groupName: group,
              messageCount: messages.length,
            });

            const processingStart = Date.now();
            const processingPromises = messages.map(async (message) => {
              if (this.lifecycleManager.isStopped(group, consumer, stream)) {
                return;
              }
              return this.consumeOne(
                stream,
                group,
                message.id,
                message.data,
                callback,
              );
            });

            // Process all messages in parallel
            await Promise.allSettled(processingPromises);

            this.logger.debug(
              'postgres-stream-parallel-processing-polling-complete',
              {
                streamName: stream,
                groupName: group,
                messageCount: messages.length,
                processingDuration: Date.now() - processingStart,
              },
            );
          } else {
            // Sequential processing for other providers or single messages
            for (const message of messages) {
              if (this.lifecycleManager.isStopped(group, consumer, stream)) {
                return;
              }
              await this.consumeOne(
                stream,
                group,
                message.id,
                message.data,
                callback,
              );
            }
          }

          // Check for pending messages if supported and enough time has passed
          const now = Date.now();
          if (
            this.stream.getProviderSpecificFeatures().supportsRetry &&
            now - lastCheckedPendingMessagesAt > this.reclaimDelay
          ) {
            lastCheckedPendingMessagesAt = now;
            const pendingMessages = await this.stream.retryMessages(
              stream,
              group,
              {
                consumerName: consumer,
                minIdleTime: this.reclaimDelay,
                limit: HMSH_XPENDING_COUNT,
              },
            );

            // Process reclaimed messages (also parallel for PostgreSQL)
            if (isPostgres && pendingMessages.length > 1) {
              const reclaimPromises = pendingMessages.map(async (message) => {
                if (this.lifecycleManager.isStopped(group, consumer, stream)) {
                  return;
                }
                return this.consumeOne(
                  stream,
                  group,
                  message.id,
                  message.data,
                  callback,
                );
              });
              await Promise.allSettled(reclaimPromises);
            } else {
              for (const message of pendingMessages) {
                if (this.lifecycleManager.isStopped(group, consumer, stream)) {
                  return;
                }
                await this.consumeOne(
                  stream,
                  group,
                  message.id,
                  message.data,
                  callback,
                );
              }
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
        if (
          this.lifecycleManager.getShouldConsume() &&
          process.env.NODE_ENV !== 'test'
        ) {
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

  async consumeOne(
    stream: string,
    group: string,
    id: string,
    input: StreamData,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    this.logger.debug(`stream-read-one`, { group, stream, id });
    let output: StreamDataResponse | void;
    const telemetry = new RouterTelemetry(this.appId);
    try {
      telemetry.startStreamSpan(input, this.role);
      output = await this.execStreamLeg(input, stream, id, callback.bind(this));
      telemetry.setStreamErrorFromOutput(output);
      this.errorCount = 0;
    } catch (err) {
      this.logger.error(`stream-read-one-error`, { group, stream, id, err });
      telemetry.setStreamErrorFromException(err);
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
  ): Promise<StreamDataResponse> {
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
      output = this.errorHandler.structureUnhandledError(input, error);
    }
    return output as StreamDataResponse;
  }

  async ackAndDelete(stream: string, group: string, id: string): Promise<void> {
    await this.stream.ackAndDelete(stream, group, [id]);
  }

  // Add batch acknowledgment method for PostgreSQL optimization
  async ackAndDeleteBatch(
    stream: string,
    group: string,
    ids: string[],
  ): Promise<void> {
    if (ids.length === 0) return;

    const features = this.stream.getProviderSpecificFeatures();
    const isPostgres = features.supportsNotifications; // Only PostgreSQL supports notifications

    if (isPostgres && ids.length > 1) {
      // Batch acknowledgment for PostgreSQL
      await this.stream.ackAndDelete(stream, group, ids);
    } else {
      // Individual acknowledgments for other providers
      for (const id of ids) {
        await this.stream.ackAndDelete(stream, group, [id]);
      }
    }
  }

  async publishResponse(
    input: StreamData,
    output: StreamDataResponse | void,
  ): Promise<string> {
    if (output && typeof output === 'object') {
      if (output.status === 'error') {
        // Extract retry policy from stream message config
        const retryPolicy = (input as any)._streamRetryConfig 
          ? {
              maximumAttempts: (input as any)._streamRetryConfig.max_retry_attempts,
              backoffCoefficient: (input as any)._streamRetryConfig.backoff_coefficient,
              maximumInterval: (input as any)._streamRetryConfig.maximum_interval_seconds,
            }
          : undefined;
        
        return await this.errorHandler.handleRetry(
          input,
          output,
          this.publishMessage.bind(this),
          retryPolicy,
        );
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

  isStreamMessage(result: any): boolean {
    return Array.isArray(result) && Array.isArray(result[0]);
  }
}
