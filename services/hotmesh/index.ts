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
 * HotMesh transforms Redis into indispensable middleware.
 * Call `HotMesh.init` to initialize a point of presence
 * and attach to the mesh.
 *
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
  /**
   * @private
   */
  engine: EngineService | null = null;
  /**
   * @private
   */
  quorum: QuorumService | null = null;
  /**
   * @private
   */
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
  constructor() {}

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
  /**
   * Starts a workflow
   * @example
   * ```typescript
   * await hotMesh.pub('a.b.c', { key: 'value' });
   * ```
   */
  async pub(
    topic: string,
    data: JobData = {},
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    return await this.engine?.pub(topic, data, context, extended);
  }
  /**
   * Subscribe (listen) to all output and interim emissions of a single
   * workflow topic
   * @example
   * ```typescript
   * await hotMesh.psub('a.b.c', (topic, message) => {
   *  console.log(message);
   * });
   * ```
   */
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.sub(topic, callback);
  }
  /**
   * Stop listening in on a single workflow topic
   */
  async unsub(topic: string): Promise<void> {
    return await this.engine?.unsub(topic);
  }
  /**
   * Listen to all output and interim emissions of a workflow topic
   * matching a wildcard pattern.
   * @example
   * ```typescript
   * await hotMesh.psub('a.b.c*', (topic, message) => {
   *  console.log(message);
   * });
   * ```
   */
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.psub(wild, callback);
  }
  /**
   * Patterned unsubscribe
   */
  async punsub(wild: string): Promise<void> {
    return await this.engine?.punsub(wild);
  }
  /**
   * Starts a workflow and awaits the response
   * @example
   * ```typescript
   * await hotMesh.pubsub('a.b.c', { key: 'value' });
   * ```
   */
  async pubsub(
    topic: string,
    data: JobData = {},
    context?: JobState | null,
    timeout?: number,
  ): Promise<JobOutput> {
    return await this.engine?.pubsub(topic, data, context, timeout);
  }
  /**
   * Add a transition message to the workstream, resuming leg 2 of a paused
   * reentrant activity (e.g., await, worker, hook)
   */
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
   * Sends a throttle message to the quorum (engine and/or workers)
   * to limit the rate of processing. Pass `-1` to throttle indefinitely.
   * The value must be a non-negative integer and not exceed `MAX_DELAY` ms.
   *
   * When throttling is set, the quorum will pause for the specified time
   * before processing the next message. Target specific engines and
   * workers by passing a `guid` and/or `topic`. Pass no arguments to
   * throttle the entire quorum.
   *
   * In this example, all processing has been paused indefinitely for
   * the entire quorum. This is equivalent to an emergency stop.
   *
   * HotMesh is a stateless sequence engine, so the throttle can be adjusted up
   * and down with no loss of data.
   *
   *
   * @example
   * ```typescript
   * await hotMesh.throttle({ throttle: -1 });
   * ```
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

  // ************* LIFECYCLE METHODS *************
  /**
   * Preview changes and provide an analysis of risk
   * prior to deployment
   * @private
   */
  async plan(path: string): Promise<HotMeshManifest> {
    return await this.engine?.plan(path);
  }
  /**
   * When the app YAML file is ready, the `deploy` function can be called.
   * This function is responsible for merging all referenced YAML source
   * files and writing the JSON output to the file system and to Redis. It
   * is also possible to embed the YAML in-line as a string.
   *
   * *The version will not be active until activation is explicitly called.*
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return await this.engine?.deploy(pathOrYAML);
  }
  /**
   * Once the app YAML file is deployed to Redis, the `activate` function can be
   * called to enable it for the entire quorum at the same moment.
   *
   * The approach is to establish the coordinated health of the system through series
   * of call/response exchanges. Once it is established that the quorum is healthy,
   * the quorum is instructed to run their engine in `no-cache` mode, ensuring
   * that the Redis backend is consulted for the active app version each time a
   * call is processed. This ensures that all engines are running the same version
   * of the app, switching over at the same moment and then enabling `cache` mode
   * to improve performance.
   *
   * *Add a delay for the quorum to reach consensus if traffic is busy, but
   * also consider throttling traffic flow to an acceptable level.*
   */
  async activate(version: string, delay?: number): Promise<boolean> {
    return await this.quorum?.activate(version, delay);
  }

  /**
   * Returns the job state as a JSON object, useful
   * for understanding dependency chains
   */
  async export(jobId: string): Promise<JobExport> {
    return await this.engine?.export(jobId);
  }
  /**
   * Returns all data (HGETALL) for a job.
   */
  async getRaw(jobId: string): Promise<StringStringType> {
    return await this.engine?.getRaw(jobId);
  }
  /**
   * Reporter-related method to get the status of a job
   * @private
   */
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return await this.engine?.getStats(topic, query);
  }
  /**
   * Returns the status of a job. This is a numeric
   * semaphore value that indicates the job's state.
   * Any non-positive value indicates a completed job.
   * Jobs with a value of `-1` are pending and will
   * automatically be scrubbed after a set period.
   * Jobs a value around -1billion have been interrupted
   * and will be scrubbed after a set period. Jobs with
   * a value of 0 completed normally. Jobs with a
   * positive value are still running.
   */
  async getStatus(jobId: string): Promise<JobStatus> {
    return this.engine?.getStatus(jobId);
  }
  /**
   * Returns the job state (data and metadata) for a job.
   */
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return this.engine?.getState(topic, jobId);
  }
  /**
   * Returns searchable/queryable data for a job. In this
   * example a literal field is also searched (the colon
   * is used to track job status and is a reserved field;
   * it can be read but not written).
   *
   * @example
   * ```typescript
   * const fields = ['fred', 'barney', '":"'];
   * const queryState = await hotMesh.getQueryState('123', fields);
   * //returns { fred: 'flintstone', barney: 'rubble', ':': '1' }
   * ```
   */
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

  /**
   * Interrupt an active job
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return await this.engine?.interrupt(topic, jobId, options);
  }

  /**
   * Immediately deletes (DEL) a completed job from the system.
   *
   * *Scrubbed jobs must be complete with a non-positive `status` value*
   */
  async scrub(jobId: string) {
    await this.engine?.scrub(jobId);
  }

  /**
   * Re/entry point for an active job. This is used to resume a paused job
   * and close the reentry point or leave it open for subsequent reentry.
   * Because `hooks` are public entry points, they include a `topic`
   * which is established in the app YAML file.
   *
   * When this method is called, a hook rule will be located to establish
   * the exact activity and activity dimension for reentry.
   */
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
