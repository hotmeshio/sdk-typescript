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
  HMSH_XPENDING_COUNT } from '../../modules/enums';
import { KeyType } from '../../modules/key';
import { XSleepFor, guid, sleepFor } from '../../modules/utils';
import { ILogger } from '../logger';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { TelemetryService } from '../telemetry';
import { RedisClient, RedisMulti } from '../../types/redis';
import {
  ReclaimedMessageType,
  StreamConfig,
  StreamData,
  StreamDataResponse,
  StreamDataType,
  StreamError,
  StreamRole,
  StreamStatus
} from '../../types/stream';

class Router {
  static instances: Set<Router> = new Set();
  appId: string;
  guid: string;
  role: StreamRole;
  topic: string | undefined;
  store: StoreService<RedisClient, RedisMulti>;
  stream: StreamService<RedisClient, RedisMulti>;
  reclaimDelay: number;
  reclaimCount: number;
  logger: ILogger;
  throttle = 0;
  errorCount = 0;
  currentTimerId: NodeJS.Timeout | null = null;
  shouldConsume: boolean;

  constructor(config: StreamConfig, stream: StreamService<RedisClient, RedisMulti>, store: StoreService<RedisClient, RedisMulti>, logger: ILogger) {
    this.appId = config.appId;
    this.guid = config.guid;
    this.role = config.role;
    this.topic = config.topic;
    this.stream = stream;
    this.store = store;
    this.reclaimDelay = config.reclaimDelay || HMSH_XCLAIM_DELAY_MS;
    this.reclaimCount = config.reclaimCount || HMSH_XCLAIM_COUNT;
    this.logger = logger;
  }

  async createGroup(stream: string, group: string) {
    try {
      await this.store.xgroup('CREATE', stream, group, '$', 'MKSTREAM');
    } catch (err) {
      this.logger.debug('consumer-group-exists', { stream, group });
    }
  }

  async publishMessage(topic: string, streamData: StreamData|StreamDataResponse, multi?: RedisMulti): Promise<string | RedisMulti> {
    const stream = this.store.mintKey(KeyType.STREAMS, { appId: this.store.appId, topic });
    return await this.store.xadd(stream, '*', 'message', JSON.stringify(streamData), multi);
  }

  async consumeMessages(stream: string, group: string, consumer: string, callback: (streamData: StreamData) => Promise<StreamDataResponse|void>): Promise<void> {
    this.logger.info(`stream-consumer-starting`, { group, consumer, stream });
    Router.instances.add(this);
    this.shouldConsume = true;
    await this.createGroup(stream, group);
    let lastCheckedPendingMessagesAt = Date.now();

    async function consume() {
      let sleep = XSleepFor(this.throttle);
      this.currentTimerId = sleep.timerId;
      await sleep.promise;
      if (!this.shouldConsume) {
        this.logger.info(`stream-consumer-stopping`, { group, consumer, stream });
        return;
      }

      try {
        //randomizer that asymptotes at 150% of `HMSH_BLOCK_TIME_MS`
        const streamDuration = HMSH_BLOCK_TIME_MS + Math.round((HMSH_BLOCK_TIME_MS * Math.random()));
        const result = await this.stream.xreadgroup('GROUP', group, consumer, 'BLOCK', streamDuration, 'STREAMS', stream, '>');
        if (this.isStreamMessage(result)) {
          const [[, messages]] = result;
          for (const [id, message] of messages) {
            await this.consumeOne(stream, group, id, message, callback);
          }
        }

        // Check for pending messages (note: Redis 6.2 simplifies)
        const now = Date.now();
        if (now - lastCheckedPendingMessagesAt > this.reclaimDelay) {
          lastCheckedPendingMessagesAt = now;
          const pendingMessages = await this.claimUnacknowledged(stream, group, consumer);
          for (const [id, message] of pendingMessages) {
            await this.consumeOne(stream, group, id, message, callback);
          }
        }
        setImmediate(consume.bind(this));
      } catch (err) {
        if (this.shouldConsume && process.env.NODE_ENV !== 'test') {
        this.logger.error(`stream-consume-message-error`, { err, stream, group, consumer });
          this.errorCount++;
          const timeout = Math.min(HMSH_GRADUATED_INTERVAL_MS * (2 ** this.errorCount), HMSH_MAX_TIMEOUT_MS);
          setTimeout(consume.bind(this), timeout);
        }
      }
    }
    consume.call(this);
  }

  isStreamMessage(result: any): boolean {
    return Array.isArray(result) && Array.isArray(result[0])
  }

  async consumeOne(stream: string, group: string, id: string, message: string[], callback: (streamData: StreamData) => Promise<StreamDataResponse|void>) {
    this.logger.debug(`stream-consume-one`, { group, stream, id });
    const [err, input] = this.parseStreamData(message[1]);
    let output: StreamDataResponse | void;
    let telemetry: TelemetryService;
    try {
      telemetry = new TelemetryService(this.appId);
      telemetry.startStreamSpan(input, this.role);  
      output = await this.execStreamLeg(input, stream, id, callback.bind(this));
      if (output?.status === StreamStatus.ERROR) {
        telemetry.setStreamError(`Function Status Code ${ output.code || HMSH_CODE_UNKNOWN }`);
      }
      this.errorCount = 0;
    } catch (err) {
      this.logger.error(`stream-consume-one-error`, { group, stream, id, err });
      telemetry.setStreamError(err.message);
    }
    const messageId = await this.publishResponse(input, output);
    telemetry.setStreamAttributes({ 'app.worker.mid': messageId });
    await this.ackAndDelete(stream, group, id);
    telemetry.endStreamSpan();
    this.logger.debug(`stream-consume-one-end`, { group, stream, id });
  }

  async execStreamLeg(input: StreamData, stream: string, id: string, callback: (streamData: StreamData) => Promise<StreamDataResponse|void>) {
    let output: StreamDataResponse | void;
    try {
      output = await callback(input);
    } catch (error) {
      this.logger.error(`stream-call-function-error`, { error });
      output = this.structureUnhandledError(input, error);
    }
    return output as StreamDataResponse;
  }

  async ackAndDelete(stream: string, group: string, id: string) {
    const multi = this.stream.getMulti();
    await this.stream.xack(stream, group, id, multi);
    await this.stream.xdel(stream, id, multi);
    await multi.exec();
  }

  async publishResponse(input: StreamData, output: StreamDataResponse | void): Promise<string> {
    if (output && typeof output === 'object') {
      if (output.status === 'error') {
        const [shouldRetry, timeout] = this.shouldRetry(input, output);
        if (shouldRetry) {
          await sleepFor(timeout);
          return await this.publishMessage(input.metadata.topic, { 
            data: input.data,
            //note: retain guid (this is a retry attempt)
            metadata: { ...input.metadata, try: (input.metadata.try || 0) + 1 },
            policies: input.policies,
          }) as string;
        } else {
          output = this.structureError(input, output);
        }
      } else if (typeof output.metadata !== 'object') {
        output.metadata = { ...input.metadata, guid: guid() };
      } else {
        output.metadata.guid = guid();
      }
      output.type = StreamDataType.RESPONSE;
      return await this.publishMessage(null, output as StreamDataResponse) as string;
    }
  }

  shouldRetry(input: StreamData, output: StreamDataResponse): [boolean, number] {
    const policies = input.policies?.retry;
    const errorCode = output.code.toString();
    const policy = policies?.[errorCode];
    const maxRetries = policy?.[0];
    const tryCount = Math.min(input.metadata.try || 0,  HMSH_MAX_RETRIES);
    //only possible values for maxRetries are 1, 2, 3
    //only possible values for tryCount are 0, 1, 2
    if (maxRetries > tryCount) {
      // 10ms, 100ms, or 1000ms delays between system retries
      return[true, Math.pow(10, tryCount + 1)];
    }
    return [false, 0];
  }

  structureUnhandledError(input: StreamData, err: Error): StreamDataResponse {
    let error: Partial<StreamError> = {};
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
      data: error as StreamError
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

  structureError(input: StreamData, output: StreamDataResponse): StreamDataResponse {
    const message = output.data?.message ? output.data?.message.toString() : HMSH_STATUS_UNKNOWN;
    const statusCode = output.code || output.data?.code;
    const code = isNaN(statusCode as number) ? HMSH_CODE_UNKNOWN : parseInt(statusCode.toString());
    const data: StreamError = { message, code };
    if (typeof output.data?.error === 'object') {
      data.error = { ...output.data.error };
    }
    return {
      status: StreamStatus.ERROR,
      code,
      metadata: { ...input.metadata, guid: guid() },
      data
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
    this.logger.info(`stream-consumer-stopping`, this.topic ? { topic: this.topic } : undefined);
    this.cancelThrottle();
  }

  cancelThrottle() {
    if (this.currentTimerId !== undefined) {
      clearTimeout(this.currentTimerId);
      this.currentTimerId = undefined;
    }
  }

  setThrottle(delayInMillis: number) {
    if (!Number.isInteger(delayInMillis) || delayInMillis < 0) {
      throw new Error('Throttle must be a non-negative integer');
    }
    this.throttle = delayInMillis;
    this.logger.info(`stream-throttle-reset`, { delay: this.throttle, topic: this.topic });
  }

  async claimUnacknowledged(stream: string, group: string, consumer: string, idleTimeMs = this.reclaimDelay, limit = HMSH_XPENDING_COUNT): Promise<[string, [string, string]][]> {
    let pendingMessages = [];
    const pendingMessagesInfo = await this.stream.xpending(stream, group, '-', '+', limit); //[[ '1688768134881-0', 'testConsumer1', 1017, 1 ]]
    for (const pendingMessageInfo of pendingMessagesInfo) {
      if (Array.isArray(pendingMessageInfo)) {
        const [id, , elapsedTimeMs, deliveryCount] = pendingMessageInfo;
        if (elapsedTimeMs > idleTimeMs) {
          const reclaimedMessage = await this.stream.xclaim(stream, group, consumer, idleTimeMs, id);
          if (reclaimedMessage.length) {
            if (deliveryCount <= this.reclaimCount) {
              pendingMessages = pendingMessages.concat(reclaimedMessage);
            } else {
              await this.expireUnacknowledged(reclaimedMessage, stream, group, consumer, id, deliveryCount);
            }
          }
        }
      }
    }
    return pendingMessages;
  }

  async expireUnacknowledged(reclaimedMessage: ReclaimedMessageType, stream: string, group: string, consumer: string, id: string, count: number) {
    //The stream activity was not processed within established limits. Possibilities Include:
    // 1) user error: the workers were not properly configured and are timing out
    // 2a) system error: JSON is corrupt
    //     i) bad/unwitting actor
    //     ii) corrupt hardware/network/transport/etc
    // 3b) system error: Redis unable to accept `xadd` request
    // 4c) system error: Redis unable to accept `xdel`/`xack` request
    this.logger.error('stream-message-max-delivery-count-exceeded', { id, stream, group, consumer, code: HMSH_CODE_UNACKED, count });
    const streamData = reclaimedMessage[0]?.[1]?.[1];

    //fatal risk point 1 of 3): json is corrupt
    const [err, input] = this.parseStreamData(streamData as string);
    if (err) {
      return this.logger.error('expire-unacknowledged-parse-fatal-error', { id, err })
    } else if(!input || !input.metadata) {
      return this.logger.error('expire-unacknowledged-parse-fatal-error', { id })
    }
    let telemetry: TelemetryService;
    let messageId: string;
    try {
      telemetry = new TelemetryService(this.appId);
      telemetry.startStreamSpan(input, StreamRole.SYSTEM);
      telemetry.setStreamError(`Stream Message Max Delivery Count Exceeded`);

      //fatal risk point 2 of 3): unable to publish error message (to notify the parent job)
      const output = this.structureUnacknowledgedError(input);
      messageId = await this.publishResponse(input, output);
      telemetry.setStreamAttributes({ 'app.worker.mid': messageId });

      //fatal risk point 3 of 3): unable to ack and delete stream message
      await this.ackAndDelete(stream, group, id);
    } catch (err) {
      if (messageId) {
        this.logger.error('expire-unacknowledged-pub-fatal-error', { id, err, ...input.metadata });
        telemetry.setStreamAttributes({ 'app.system.fatal': 'expire-unacknowledged-pub-fatal-error' });
      } else {
        this.logger.error('expire-unacknowledged-ack-fatal-error', { id, err, ...input.metadata });
        telemetry.setStreamAttributes({ 'app.system.fatal': 'expire-unacknowledged-ack-fatal-error' });
      }
    } finally {
      telemetry.endStreamSpan();
    }
  }

  parseStreamData(str: string): [undefined, StreamData] | [Error] {
    try {
      return [,JSON.parse(str)];
    } catch (e) {
      return [e as Error];
    }
  }
}

export { Router };