import { HMNS } from '../../modules/key';
import { guid } from '../../modules/utils';
import { RedisConnection } from '../connector/clients/redis';
import { RedisConnection as IORedisConnection } from '../connector/clients/ioredis';
import { ConnectorService } from '../connector';
import { EngineService } from '../engine';
import { LoggerService, ILogger } from '../logger';
import { QuorumService } from '../quorum';
import { Router } from '../router';
import { WorkerService } from '../worker';
import {
  JobState,
  JobData,
  JobOutput,
  JobStatus,
  JobInterruptOptions,
  ExtensionType,
} from '../../types/job';
import { HotMeshConfig, HotMeshManifest } from '../../types/hotmesh';
import { JobExport } from '../../types/exporter';
import {
  JobMessageCallback,
  QuorumMessage,
  QuorumMessageCallback,
  QuorumProfile,
  ThrottleMessage,
  ThrottleOptions,
} from '../../types/quorum';
import { StringAnyType, StringStringType } from '../../types/serializer';
import {
  JobStatsInput,
  GetStatsOptions,
  IdsResponse,
  StatsResponse,
} from '../../types/stats';
import {
  StreamCode,
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../types/stream';
import { MAX_DELAY } from '../../modules/enums';

/**
 * HotMesh transforms Redis into a durable service mesh.
 * Call the static `init` method to initialize a point of presence
 * and attach to the mesh. Connect an `engine` or link a `worker`.
 * This example shows the full lifecycle of a HotMesh engine instance,
 * including: initialization, deployment, activation and execution.
 * 
 * The system is self-cleaning and self-healing, with a built-in
 * quorum for consensus and a worker pool for distributed processing.
 * Workflows are automatically removed from the system once completed.
 * 
 * @example
 * ```typescript
 * import Redis from 'ioredis';
 * import { HotMesh } from '@hotmeshio/hotmesh';
 * 
 * const hotMesh = await HotMesh.init({
 *   appId: 'abc',
 *   engine: {
 *     redis: {
 *       class: Redis,
 *       options: { host, port, password, db }
 *     }
 *   }
 * });
 * 
 * await hotMesh.deploy(`
 * app:
 *   id: abc
 *   version: '1'
 *   graphs:
 *     - subscribes: abc.test
 *       activities:
 *         t1:
 *           type: trigger
 * `);
 *
 * await hotMesh.activate('1');
 *
 * await hotMesh.pubsub('abc.test');
 * 
 * await HotMesh.stop();
 * ```
 */
class HotMesh {
  namespace: string;
  appId: string;
  guid: string;
  engine: EngineService | null = null;
  quorum: QuorumService | null = null;
  workers: WorkerService[] = [];
  logger: ILogger;

  static disconnecting = false;

  /**
   * @private
   */
  verifyAndSetNamespace(namespace?: string) {
    if (!namespace) {
      this.namespace = HMNS;
    } else if (!namespace.match(/^[A-Za-z0-9-]+$/)) {
      throw new Error(`config.namespace [${namespace}] is invalid`);
    } else {
      this.namespace = namespace;
    }
  }

  /**
   * @private
   */
  verifyAndSetAppId(appId: string) {
    if (!appId?.match(/^[A-Za-z0-9-]+$/)) {
      throw new Error(`config.appId [${appId}] is invalid`);
    } else if (appId === 'a') {
      throw new Error(`config.appId [${appId}] is reserved`);
    } else {
      this.appId = appId;
    }
  }

  /**
   * Instance initializer
   * @example
   * ```typescript
   * const config: HotMeshConfig = {
   *   appId: 'myapp',
   *   engine: {
   *     redis: {
   *       class: Redis,
   *       options: { host: 'localhost', port: 6379 }
   *     },
   *   },
   *   workers [...]
   * };
   * const hotMesh = await HotMesh.init(config);
   * ```
   */
  static async init(config: HotMeshConfig) {
    const instance = new HotMesh();
    instance.guid = config.guid ?? guid();
    instance.verifyAndSetNamespace(config.namespace);
    instance.verifyAndSetAppId(config.appId);
    instance.logger = new LoggerService(
      config.appId,
      instance.guid,
      config.name || '',
      config.logLevel,
    );
    await instance.initEngine(config, instance.logger);
    await instance.initQuorum(config, instance.engine, instance.logger);
    await instance.doWork(config, instance.logger);
    return instance;
  }

  /**
   * returns a guid using the same core guid
   * generator used by the HotMesh (nanoid)
   */
  static guid(): string {
    return guid();
  }

  /**
   * @private
   */
  async initEngine(config: HotMeshConfig, logger: ILogger): Promise<void> {
    if (config.engine) {
      await ConnectorService.initRedisClients(
        config.engine.redis?.class,
        config.engine.redis?.options,
        config.engine,
      );
      this.engine = await EngineService.init(
        this.namespace,
        this.appId,
        this.guid,
        config,
        logger,
      );
    }
  }

  /**
   * @private
   */
  async initQuorum(
    config: HotMeshConfig,
    engine: EngineService,
    logger: ILogger,
  ): Promise<void> {
    if (engine) {
      this.quorum = await QuorumService.init(
        this.namespace,
        this.appId,
        this.guid,
        config,
        engine,
        logger,
      );
    }
  }

  /**
   * @private
   */
  constructor() { }

  /**
   * @private
   */
  async doWork(config: HotMeshConfig, logger: ILogger) {
    this.workers = await WorkerService.init(
      this.namespace,
      this.appId,
      this.guid,
      config,
      logger,
    );
  }

  // ************* PUB/SUB METHODS *************
  async pub(
    topic: string,
    data: JobData = {},
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    return await this.engine?.pub(topic, data, context, extended);
  }
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.sub(topic, callback);
  }
  async unsub(topic: string): Promise<void> {
    return await this.engine?.unsub(topic);
  }
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.psub(wild, callback);
  }
  async punsub(wild: string): Promise<void> {
    return await this.engine?.punsub(wild);
  }
  /**
   * One-time subscription in support of request/response exchanges
   */
  async pubsub(
    topic: string,
    data: JobData = {},
    context?: JobState | null,
    timeout?: number,
  ): Promise<JobOutput> {
    return await this.engine?.pubsub(topic, data, context, timeout);
  }
  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return (await this.engine.add(streamData)) as string;
  }

  // ************* QUORUM METHODS *************
  /**
   * Request a roll call from the quorum (engine and workers)
   */
  async rollCall(delay?: number): Promise<QuorumProfile[]> {
    return await this.quorum?.rollCall(delay);
  }
  /**
   * Send a throttle message to the quorum (engine and/or workers)
   */
  async throttle(options: ThrottleOptions): Promise<boolean> {
    let throttle: number;
    if (options.throttle === -1) {
      throttle = MAX_DELAY;
    } else {
      throttle = options.throttle;
    }
    if (!Number.isInteger(throttle) || throttle < 0 || throttle > MAX_DELAY) {
      throw new Error(
        `Throttle must be a non-negative integer and not exceed ${MAX_DELAY} ms; send -1 to throttle indefinitely`,
      );
    }
    const throttleMessage: ThrottleMessage = {
      type: 'throttle',
      throttle: throttle,
    };
    if (options.guid) {
      throttleMessage.guid = options.guid;
    }
    if (options.topic !== undefined) {
      throttleMessage.topic = options.topic;
    }
    await this.engine.store.setThrottleRate(throttleMessage);
    return await this.quorum?.pub(throttleMessage);
  }
  /**
   * Publish a message to the quorum (engine and/or workers)
   */
  async pubQuorum(quorumMessage: QuorumMessage) {
    return await this.quorum?.pub(quorumMessage);
  }
  /**
   * Subscribe to quorum events (engine and workers)
   */
  async subQuorum(callback: QuorumMessageCallback): Promise<void> {
    return await this.quorum?.sub(callback);
  }
  /**
   * Unsubscribe from quorum events (engine and workers)
   */
  async unsubQuorum(callback: QuorumMessageCallback): Promise<void> {
    return await this.quorum?.unsub(callback);
  }

  // ************* COMPILER METHODS *************
  async plan(path: string): Promise<HotMeshManifest> {
    return await this.engine?.plan(path);
  }
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return await this.engine?.deploy(pathOrYAML);
  }
  async activate(version: string, delay?: number): Promise<boolean> {
    //activation is a quorum operation
    return await this.quorum?.activate(version, delay);
  }

  // ************* REPORTER METHODS *************
  async export(jobId: string): Promise<JobExport> {
    return await this.engine?.export(jobId);
  }
  async getRaw(jobId: string): Promise<StringStringType> {
    return await this.engine?.getRaw(jobId);
  }
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return await this.engine?.getStats(topic, query);
  }
  async getStatus(jobId: string): Promise<JobStatus> {
    return this.engine?.getStatus(jobId);
  }
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return this.engine?.getState(topic, jobId);
  }
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return await this.engine?.getQueryState(jobId, fields);
  }
  /**
   * @private
   */
  async getIds(
    topic: string,
    query: JobStatsInput,
    queryFacets = [],
  ): Promise<IdsResponse> {
    return await this.engine?.getIds(topic, query, queryFacets);
  }
  /**
   * @private
   */
  async resolveQuery(
    topic: string,
    query: JobStatsInput,
  ): Promise<GetStatsOptions> {
    return await this.engine?.resolveQuery(topic, query);
  }

  // ****************** `INTERRUPT` ACTIVE JOBS *****************
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return await this.engine?.interrupt(topic, jobId, options);
  }

  // ****************** `SCRUB` CLEAN COMPLETED JOBS *****************
  async scrub(jobId: string) {
    await this.engine?.scrub(jobId);
  }

  // ****** `HOOK` ACTIVITY RE-ENTRY POINT ******
  async hook(
    topic: string,
    data: JobData,
    status?: StreamStatus,
    code?: StreamCode,
  ): Promise<string> {
    return await this.engine?.hook(topic, data, status, code);
  }
  /**
   * @private
   */
  async hookAll(
    hookTopic: string,
    data: JobData,
    query: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<string[]> {
    return await this.engine?.hookAll(hookTopic, data, query, queryFacets);
  }

  /**
   * Stop all points of presence, workers and engines
   */
  static async stop() {
    if (!this.disconnecting) {
      this.disconnecting = true;
      await Router.stopConsuming();
      await RedisConnection.disconnectAll();
      await IORedisConnection.disconnectAll();
    }
  }

  /**
   * Stop this point of presence, workers and engines
   */
  stop() {
    this.engine?.taskService.cancelCleanup();
    this.quorum?.stop();
    this.workers?.forEach((worker: WorkerService) => {
      worker.stop();
    });
  }

  /**
   * @private
   * @deprecated
   */
  async compress(terms: string[]): Promise<boolean> {
    return await this.engine?.compress(terms);
  }
}

export { HotMesh };
