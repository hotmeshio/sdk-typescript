import { guid } from '../../../modules/utils';
import {
  LeaseExpiredError,
  ErrorCategory,
  classifyError,
} from '../../../modules/errors';
import { ILogger } from '../../logger';
import { StreamService } from '../../stream';
import { ThrottleManager } from '../throttling';
import { ErrorHandler } from '../error-handling';
import { RouterTelemetry } from '../telemetry';
import { LifecycleManager } from '../lifecycle';
import { DuressManager, DuressSnapshot } from '../duress';
import {
  HMSH_BLOCK_TIME_MS,
  HMSH_GRADUATED_INTERVAL_MS,
  HMSH_MAX_TIMEOUT_MS,
  HMSH_XPENDING_COUNT,
  HMSH_BATCH_SIZE,
  HMSH_RESERVATION_TIMEOUT_S,
  HMSH_RESERVATION_TIMEOUT_MAX_S,
  HMSH_BATCH_SIZE_MIN,
  MAX_STREAM_BACKOFF,
  INITIAL_STREAM_BACKOFF,
  MAX_STREAM_RETRIES,
  HMSH_POISON_MESSAGE_THRESHOLD,
  HMSH_DURESS_EVAL_INTERVAL,
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
  /**
   * Consumption stats are written directly to the parent Router so
   * they are visible in quorum rollcall profiles.
   */
  private get errorCount(): number { return this.router.errorCount; }
  private set errorCount(v: number) { this.router.errorCount = v; }
  private get counts(): { [key: string]: number } { return this.router.counts; }
  private get hasReachedMaxBackoff(): boolean | undefined { return this.router.hasReachedMaxBackoff; }
  private set hasReachedMaxBackoff(v: boolean | undefined) { this.router.hasReachedMaxBackoff = v; }
  private router: any;
  private retry: import('../../../types/stream').RetryPolicy | undefined;
  private duressManager?: DuressManager;
  private onDuressChange?: (snapshot: DuressSnapshot) => void;
  private messagesSinceLastEval = 0;

  // Adaptive consumption pressure — scales reservation timeout AND batch
  // size based on stream depth. Under load: timeout grows (prevents
  // duplicate re-reservation) and batch size shrinks (reduces in-memory
  // blocking, lets other consumers share the stream). When idle, both
  // restore toward configured defaults.
  private adaptiveReservationTimeout = HMSH_RESERVATION_TIMEOUT_S;
  private adaptiveBatchSize = HMSH_BATCH_SIZE;
  private lastDepthCheckAt = 0;
  private static readonly DEPTH_CHECK_INTERVAL_MS = 10_000;
  private static readonly DEPTH_SCALE_UP_THRESHOLD = 100;
  private static readonly DEPTH_SCALE_DOWN_THRESHOLD = 10;
  // Buffer between the activity deadline (N) and the reclaim interval
  // (N+5). The function gets the full configured timeout; the extra 5s
  // ensures the deadline fires before a reclaimant can pick up the message.
  private static readonly LEASE_BUFFER_S = 5;

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
    retry?: import('../../../types/stream').RetryPolicy,
    duressManager?: DuressManager,
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
    this.retry = retry;
    this.duressManager = duressManager;
  }

  setDuressCallback(callback: (snapshot: DuressSnapshot) => void): void {
    this.onDuressChange = callback;
  }

  /**
   * Adjusts reservation timeout based on stream depth. Called periodically
   * from the consume loop. When depth is high:
   *   - reservation timeout grows (prevents duplicate re-reservation)
   *   - batch size shrinks (reduces in-memory blocking, shares the stream)
   * When depth drops, both restore toward configured defaults.
   */
  private async adjustConsumptionPressure(stream: string): Promise<void> {
    const now = Date.now();
    if (now - this.lastDepthCheckAt < ConsumptionManager.DEPTH_CHECK_INTERVAL_MS) {
      return;
    }
    this.lastDepthCheckAt = now;

    try {
      const depth = await this.stream.getStreamDepth(stream);
      const prevTimeout = this.adaptiveReservationTimeout;
      const prevBatch = this.adaptiveBatchSize;

      if (depth > ConsumptionManager.DEPTH_SCALE_UP_THRESHOLD) {
        // Scale up timeout, scale down batch size
        this.adaptiveReservationTimeout = Math.min(
          this.adaptiveReservationTimeout * 2,
          HMSH_RESERVATION_TIMEOUT_MAX_S,
        );
        this.adaptiveBatchSize = Math.max(
          Math.floor(this.adaptiveBatchSize / 2),
          HMSH_BATCH_SIZE_MIN,
        );
      } else if (depth < ConsumptionManager.DEPTH_SCALE_DOWN_THRESHOLD) {
        // Scale down timeout, scale up batch size
        this.adaptiveReservationTimeout = Math.max(
          Math.floor(this.adaptiveReservationTimeout / 2),
          HMSH_RESERVATION_TIMEOUT_S,
        );
        this.adaptiveBatchSize = Math.min(
          this.adaptiveBatchSize * 2,
          HMSH_BATCH_SIZE,
        );
      }

      if (this.adaptiveReservationTimeout !== prevTimeout) {
        this.stream.reservationTimeout =
          this.adaptiveReservationTimeout + ConsumptionManager.LEASE_BUFFER_S;
        this.logger.info('stream-reservation-timeout-adjusted', {
          stream,
          depth,
          previousTimeoutS: prevTimeout,
          newTimeoutS: this.adaptiveReservationTimeout,
          configuredDefaultS: HMSH_RESERVATION_TIMEOUT_S,
        });
      }
      if (this.adaptiveBatchSize !== prevBatch) {
        this.logger.info('stream-batch-size-adjusted', {
          stream,
          depth,
          previousBatchSize: prevBatch,
          newBatchSize: this.adaptiveBatchSize,
          configuredDefaultBatchSize: HMSH_BATCH_SIZE,
        });
      }
    } catch {
      // Stream depth check is best-effort; don't fail the consume loop
    }
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
      this.logger.debug(`router-stream-using-notifications`, {
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

      // Adapt reservation timeout based on stream depth
      await this.adjustConsumptionPressure(stream);

      await this.throttleManager.customSleep(); // respect throttle

      if (
        this.lifecycleManager.isStopped(group, consumer, stream) ||
        this.throttleManager.isPaused()
      ) {
        return;
      }

      // Process messages - use parallel processing for PostgreSQL
      const features = this.stream.getProviderSpecificFeatures();
      const isPostgres = features.supportsParallelProcessing;

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
        reservationTimeout: HMSH_RESERVATION_TIMEOUT_S + ConsumptionManager.LEASE_BUFFER_S,
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

        // Adapt reservation timeout based on stream depth
        await this.adjustConsumptionPressure(stream);

        if (!this.hasReachedMaxBackoff) {
          // Normal mode: try with backoff and finite retries
          const features = this.stream.getProviderSpecificFeatures();
          const isPostgres = features.supportsParallelProcessing;
          const batchSize = isPostgres ? this.adaptiveBatchSize : 1;

          messages = await this.stream.consumeMessages(
            stream,
            group,
            consumer,
            {
              blockTimeout: streamDuration,
              batchSize,
              reservationTimeout: this.adaptiveReservationTimeout + ConsumptionManager.LEASE_BUFFER_S,
              enableBackoff: true,
              initialBackoff: INITIAL_STREAM_BACKOFF,
              maxBackoff: MAX_STREAM_BACKOFF,
              maxRetries: MAX_STREAM_RETRIES,
            },
          );
        } else {
          // Fallback mode: just try once, no backoff
          const features = this.stream.getProviderSpecificFeatures();
          const isPostgres = features.supportsParallelProcessing;
          const batchSize = isPostgres ? this.adaptiveBatchSize : 1;

          messages = await this.stream.consumeMessages(
            stream,
            group,
            consumer,
            {
              blockTimeout: streamDuration,
              batchSize,
              reservationTimeout: this.adaptiveReservationTimeout + ConsumptionManager.LEASE_BUFFER_S,
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
          const isPostgres = features.supportsParallelProcessing;

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

    // Poison message circuit breaker. This is a SAFETY NET that sits above
    // the normal retry mechanism (ErrorHandler.handleRetry / shouldRetry).
    //
    // Normal retry flow: handleRetry() checks metadata.try against the
    // configured retry.maximumAttempts (or _streamRetryConfig) and
    // applies exponential backoff + visibility delays. That mechanism is
    // the primary retry budget and is what developers configure via
    // HotMesh.init({ workers: [{ retry: { maximumAttempts, ... } }] }).
    //
    // This check catches messages that have somehow exceeded the normal
    // budget — e.g., when no retry is configured, when the retry
    // logic is bypassed by an infrastructure error, or when a message
    // re-enters the stream through a path that doesn't increment
    // metadata.try. The threshold is the HIGHER of the configured retry
    // budget and the system-wide HMSH_POISON_MESSAGE_THRESHOLD, so it
    // never interferes with legitimate developer-configured retries.
    const retryAttempt = (input as any)._retryAttempt || 0;
    const configuredMax =
      (input as any)._streamRetryConfig?.max_retry_attempts
      ?? this.retry?.maximumAttempts
      ?? 0;
    const poisonThreshold = Math.max(configuredMax, HMSH_POISON_MESSAGE_THRESHOLD);
    if (retryAttempt >= poisonThreshold) {
      this.logger.error(`stream-poison-message-detected`, {
        group,
        stream,
        id,
        retryAttempt,
        poisonThreshold,
        configuredMaxAttempts: configuredMax,
        systemThreshold: HMSH_POISON_MESSAGE_THRESHOLD,
        topic: input.metadata?.topic,
        activityId: input.metadata?.aid,
        jobId: input.metadata?.jid,
        metadata: input.metadata,
      });
      const errorOutput = this.errorHandler.structureUnhandledError(
        input,
        new Error(
          `Poison message detected: retry attempt ${retryAttempt} reached ` +
          `threshold ${poisonThreshold} (configured: ${configuredMax}, ` +
          `system: ${HMSH_POISON_MESSAGE_THRESHOLD}). Discarding message ` +
          `for activity "${input.metadata?.aid || 'unknown'}" ` +
          `(topic: ${input.metadata?.topic || 'unknown'}, ` +
          `job: ${input.metadata?.jid || 'unknown'}).`,
        ),
      );
      try {
        await this.publishMessage(null, errorOutput as StreamData);
      } catch (publishErr) {
        this.logger.error(`stream-poison-message-publish-error`, {
          error: publishErr,
          stream,
          id,
          retryAttempt,
          poisonThreshold,
        });
      }
      // Mark as dead-lettered if the provider supports it; otherwise just ack
      if (this.stream.deadLetterMessages) {
        await this.stream.deadLetterMessages(stream, group, [id]);
      } else {
        await this.ackAndDelete(stream, group, id);
      }
      return;
    }

    // Lease deadline: the full configured reservation timeout (N).
    // The reclaim interval is N+5s, so the deadline always fires
    // before a reclaimant can pick up the message. This preserves
    // the user's contract — if they set 30s, the function gets 30s.
    const deadlineMs = this.adaptiveReservationTimeout * 1000;

    let output: StreamDataResponse | void;
    const telemetry = new RouterTelemetry(this.appId);
    const processingStart = Date.now();
    try {
      telemetry.startStreamSpan(input, this.role);

      let deadlineTimer: ReturnType<typeof setTimeout>;
      const deadlinePromise = new Promise<never>((_, reject) => {
        deadlineTimer = setTimeout(
          () =>
            reject(
              new LeaseExpiredError(deadlineMs, this.adaptiveReservationTimeout),
            ),
          deadlineMs,
        );
      });

      try {
        output = await Promise.race([
          this.execStreamLeg(input, stream, id, callback.bind(this)),
          deadlinePromise,
        ]);
      } finally {
        clearTimeout(deadlineTimer);
      }

      telemetry.setStreamErrorFromOutput(output);
      this.errorCount = 0;
    } catch (err) {
      const category = classifyError(err);

      if (err instanceof LeaseExpiredError) {
        // FATAL: lease expired — do NOT ack. The message remains in the
        // stream for a reclaimant to pick up cleanly. Any partial writes
        // from this consumer are idempotent via collation.
        this.logger.error('stream-lease-expired', {
          category,
          group,
          stream,
          id,
          deadlineMs,
          reservationTimeoutS: this.adaptiveReservationTimeout,
          topic: input.metadata?.topic,
          activityId: input.metadata?.aid,
          jobId: input.metadata?.jid,
        });
        telemetry.setStreamErrorFromException(err);
        telemetry.endStreamSpan();
        return; // NO ack — leave for reclaimant
      }

      this.logger.error(`stream-read-one-error`, {
        category,
        group,
        stream,
        id,
        err,
      });
      telemetry.setStreamErrorFromException(err);
      output = this.errorHandler.structureUnhandledError(
        input,
        err instanceof Error ? err : new Error(String(err)),
      );
    }

    // Record processing latency for duress detection (engine routers only).
    // This measures the actual time spent in execStreamLeg — the causal
    // signal. The prior depth-based mechanism (adjustConsumptionPressure)
    // responds to queue backlog; this responds to *why* the backlog exists.
    // Evaluation is amortized over HMSH_DURESS_EVAL_INTERVAL messages to
    // avoid per-message overhead.
    if (this.duressManager && input.type) {
      const processingDuration = Date.now() - processingStart;
      this.duressManager.recordLatency(
        input.type as StreamDataType,
        processingDuration,
      );
      if (++this.messagesSinceLastEval >= HMSH_DURESS_EVAL_INTERVAL) {
        this.messagesSinceLastEval = 0;
        const snapshot = this.duressManager.evaluate();
        this.throttleManager.setDuressFloor(snapshot.throttle_ms);
        if (snapshot.level !== 'healthy') {
          this.logger.info('stream-duress-detected', {
            stream,
            level: snapshot.level,
            score_ms: snapshot.score_ms,
            throttle_ms: snapshot.throttle_ms,
            per_type: snapshot.per_type,
          });
        }
        if (this.duressManager.shouldBroadcast() && this.onDuressChange) {
          this.duressManager.markBroadcast();
          this.onDuressChange(snapshot);
        }
      }
    }

    try {
      // When the ENGINE encounters an infrastructure error (schema not found,
      // subscription missing — code 598), the message is permanently unprocessable.
      // Do NOT republish it — that creates an infinite poison loop. Only suppress
      // these specific infrastructure errors; application-level errors (retries,
      // duplicates, workflow failures) must still flow through normally.
      if (group === 'ENGINE' && output?.code === 598) {
        this.logger.error(`stream-engine-dispatch-fatal`, {
          category: ErrorCategory.FATAL,
          stream, id, group,
          aid: (input as any).metadata?.aid,
          jid: (input as any).metadata?.jid,
          message: output.data?.message,
        });
      } else {
        const messageId = await this.publishResponse(input, output);
        telemetry.setStreamAttributes({ 'app.worker.mid': messageId });
      }
    } catch (publishErr) {
      // If publishResponse fails, still ack the message to prevent
      // infinite reprocessing. Log the error for debugging.
      this.logger.error(`stream-publish-response-error`, {
        category: classifyError(publishErr),
        group, stream, id, error: publishErr,
      });
      this.errorCount++;
    } finally {
      await this.ackAndDelete(stream, group, id);
      telemetry.endStreamSpan();
      this.logger.debug(`stream-read-one-end`, { group, stream, id });
    }
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
        category: classifyError(error),
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
    const isPostgres = features.supportsParallelProcessing;

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
        // Extract retry policy with priority:
        // 1. Use message-level _streamRetryConfig (from database columns or previous retry)
        // 2. Fall back to router-level retry (from worker config)
        const streamRetryConfig = (input as any)._streamRetryConfig;
        const retry = streamRetryConfig
          ? {
              maximumAttempts: streamRetryConfig.max_retry_attempts,
              backoffCoefficient: streamRetryConfig.backoff_coefficient,
              maximumInterval: streamRetryConfig.maximum_interval_seconds,
              initialInterval: streamRetryConfig.initialInterval ?? (input as any).data?.initialInterval ?? 1,
            }
          : this.retry;
        
        return await this.errorHandler.handleRetry(
          input,
          output,
          this.publishMessage.bind(this),
          retry,
          (topic: string, delayMs: number) => {
            // Schedule a targeted NOTIFY so the consumer wakes up
            // when the visibility-delayed retry message becomes visible
            if (typeof (this.stream as any).scheduleStreamNotify === 'function') {
              const streamKey = this.stream.mintKey(KeyType.STREAMS, { topic });
              (this.stream as any).scheduleStreamNotify(streamKey, delayMs);
            }
          },
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
