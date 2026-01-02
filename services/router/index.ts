import { ILogger } from '../logger';
import { StreamService } from '../stream';
import {
  RouterConfig,
  StreamData,
  StreamDataResponse,
  StreamRole,
} from '../../types/stream';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

// Import the new submodules
import { RouterConfigManager } from './config';
import { ThrottleManager } from './throttling';
import { ErrorHandler } from './error-handling';
import { RouterTelemetry } from './telemetry';
import { LifecycleManager, InstanceRegistry } from './lifecycle';
import { ConsumptionManager } from './consumption';

class Router<S extends StreamService<ProviderClient, ProviderTransaction>> {
  // Core properties
  appId: string;
  guid: string;
  role: StreamRole;
  topic: string | undefined;
  stream: S;
  reclaimDelay: number;
  reclaimCount: number;
  logger: ILogger;
  readonly: boolean;
  retryPolicy: import('../../types/stream').RetryPolicy | undefined;

  // Legacy properties for backward compatibility
  errorCount = 0;
  counts: { [key: string]: number } = {};
  hasReachedMaxBackoff: boolean | undefined;
  currentTimerId: NodeJS.Timeout | null = null;
  sleepPromiseResolve: (() => void) | null = null;
  innerPromiseResolve: (() => void) | null = null;
  isSleeping = false;
  sleepTimout: NodeJS.Timeout | null = null;
  private isUsingNotifications = false;

  // Submodule managers
  private throttleManager: ThrottleManager;
  private errorHandler: ErrorHandler;
  private lifecycleManager: LifecycleManager<S>;
  private consumptionManager: ConsumptionManager<S>;

  constructor(config: RouterConfig, stream: S, logger: ILogger) {
    // Apply configuration defaults
    const enhancedConfig = RouterConfigManager.setDefaults(config);

    this.appId = enhancedConfig.appId;
    this.guid = enhancedConfig.guid;
    this.role = enhancedConfig.role;
    this.topic = enhancedConfig.topic;
    this.stream = stream;
    this.reclaimDelay = enhancedConfig.reclaimDelay;
    this.reclaimCount = enhancedConfig.reclaimCount;
    this.logger = logger;
    this.readonly = enhancedConfig.readonly;
    this.retryPolicy = enhancedConfig.retryPolicy;

    // Initialize submodule managers
    this.throttleManager = new ThrottleManager(enhancedConfig.throttle);
    this.errorHandler = new ErrorHandler();
    this.lifecycleManager = new LifecycleManager(
      this.readonly,
      this.topic,
      this.logger,
      this.stream,
    );
    this.consumptionManager = new ConsumptionManager(
      this.stream,
      this.logger,
      this.throttleManager,
      this.errorHandler,
      this.lifecycleManager,
      this.reclaimDelay,
      this.reclaimCount,
      this.appId,
      this.role,
      this,
      this.retryPolicy,
    );

    this.resetThrottleState();
  }

  // Legacy compatibility methods
  get throttle(): number {
    return this.throttleManager.getThrottle();
  }

  get shouldConsume(): boolean {
    return this.lifecycleManager.getShouldConsume();
  }

  set shouldConsume(value: boolean) {
    this.lifecycleManager.setShouldConsume(value);
  }

  private resetThrottleState() {
    this.sleepPromiseResolve = null;
    this.innerPromiseResolve = null;
    this.isSleeping = false;
    this.sleepTimout = null;
  }

  // Delegated methods to submodules
  async createGroup(stream: string, group: string): Promise<void> {
    return this.consumptionManager.createGroup(stream, group);
  }

  async publishMessage(
    topic: string,
    streamData: StreamData | StreamDataResponse,
    transaction?: ProviderTransaction,
  ): Promise<string | ProviderTransaction> {
    return this.consumptionManager.publishMessage(
      topic,
      streamData,
      transaction,
    );
  }

  async customSleep(): Promise<void> {
    return this.throttleManager.customSleep();
  }

  async consumeMessages(
    stream: string,
    group: string,
    consumer: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    return this.consumptionManager.consumeMessages(
      stream,
      group,
      consumer,
      callback,
    );
  }

  isStreamMessage(result: any): boolean {
    return this.consumptionManager.isStreamMessage(result);
  }

  isPaused(): boolean {
    return this.throttleManager.isPaused();
  }

  isStopped(group: string, consumer: string, stream: string): boolean {
    return this.lifecycleManager.isStopped(group, consumer, stream);
  }

  async consumeOne(
    stream: string,
    group: string,
    id: string,
    input: StreamData,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<void> {
    return this.consumptionManager.consumeOne(
      stream,
      group,
      id,
      input,
      callback,
    );
  }

  async execStreamLeg(
    input: StreamData,
    stream: string,
    id: string,
    callback: (streamData: StreamData) => Promise<StreamDataResponse | void>,
  ): Promise<StreamDataResponse> {
    return this.consumptionManager.execStreamLeg(input, stream, id, callback);
  }

  async ackAndDelete(stream: string, group: string, id: string): Promise<void> {
    return this.consumptionManager.ackAndDelete(stream, group, id);
  }

  async publishResponse(
    input: StreamData,
    output: StreamDataResponse | void,
  ): Promise<string> {
    return this.consumptionManager.publishResponse(input, output);
  }

  shouldRetry(
    input: StreamData,
    output: StreamDataResponse,
  ): [boolean, number] {
    return this.errorHandler.shouldRetry(input, output);
  }

  structureUnhandledError(input: StreamData, err: Error): StreamDataResponse {
    return this.errorHandler.structureUnhandledError(input, err);
  }

  structureUnacknowledgedError(input: StreamData): StreamDataResponse {
    return this.errorHandler.structureUnacknowledgedError(input);
  }

  structureError(
    input: StreamData,
    output: StreamDataResponse,
  ): StreamDataResponse {
    return this.errorHandler.structureError(input, output);
  }

  // Static methods for instance management
  static async stopConsuming(): Promise<void> {
    return InstanceRegistry.stopAll();
  }

  async stopConsuming(): Promise<void> {
    return this.lifecycleManager.stopConsuming(this);
  }

  cancelThrottle(): void {
    this.throttleManager.cancelThrottle();
    this.resetThrottleState();
  }

  public setThrottle(delayInMillis: number): void {
    RouterConfigManager.validateThrottle(delayInMillis);
    this.throttleManager.setThrottle(delayInMillis);
  }

  // Static instances property for backward compatibility
  static get instances(): Set<Router<any>> {
    return InstanceRegistry.getInstances();
  }
}

export { Router };
