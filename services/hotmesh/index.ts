import { HMNS } from '../../modules/key';
import { guid } from '../../modules/utils';
import { ConnectorService } from '../connector/factory';
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
import { MAX_DELAY, DEFAULT_TASK_QUEUE } from '../../modules/enums';

/**
 * HotMesh is a distributed, reentrant process orchestration engine that transforms
 * Postgres into a resilient service mesh capable of running
 * fault-tolerant workflows across multiple services and systems.
 *
 * ## Core Concepts
 *
 * **Distributed Quorum Architecture**: HotMesh operates as a distributed quorum
 * where multiple engine and worker instances coordinate using CQRS principles.
 * Each member reads from assigned topic queues and writes results to other queues,
 * creating emergent workflow orchestration without a central controller.
 *
 * **Reentrant Process Engine**: Unlike traditional workflow engines, HotMesh
 * provides built-in retry logic, idempotency, and failure recovery. Your business
 * logic doesn't need to handle timeouts or retries - the engine manages all of that.
 *
 * ## Key Features
 *
 * - **Fault Tolerance**: Automatic retry, timeout, and failure recovery
 * - **Distributed Execution**: No single point of failure
 * - **YAML-Driven**: Model-driven development with declarative workflow definitions
 * - **OpenTelemetry**: Built-in observability and tracing
 * - **Durable State**: Workflow state persists across system restarts
 * - **Pattern Matching**: Pub/sub with wildcard pattern support
 * - **Throttling**: Dynamic flow control and backpressure management
 *
 * ## Architecture
 *
 * HotMesh consists of several specialized modules:
 * - **HotMesh**: Core orchestration engine (this class)
 * - **MemFlow**: Temporal.io-compatible workflow framework
 * - **MeshCall**: Durable function execution (Temporal-like clone)
 *
 * ## Lifecycle Overview
 *
 * 1. **Initialize**: Create HotMesh instance with provider configuration
 * 2. **Deploy**: Upload YAML workflow definitions to the backend
 * 3. **Activate**: Coordinate quorum to enable the workflow version
 * 4. **Execute**: Publish events to trigger workflow execution
 * 5. **Monitor**: Track progress via OpenTelemetry and built-in observability
 *
 * ## Basic Usage
 *
 * @example
 * ```typescript
 * import { HotMesh } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * // Initialize with Postgres backend
 * const hotMesh = await HotMesh.init({
 *   appId: 'my-app',
 *   engine: {
 *     connection: {
 *       class: Postgres,
 *       options: {
 *         connectionString: 'postgresql://user:pass@localhost:5432/db'
 *       }
 *     }
 *   }
 * });
 *
 * // Deploy workflow definition
 * await hotMesh.deploy(`
 * app:
 *   id: my-app
 *   version: '1'
 *   graphs:
 *     - subscribes: order.process
 *       activities:
 *         validate:
 *           type: worker
 *           topic: order.validate
 *         approve:
 *           type: hook
 *           topic: order.approve
 *         fulfill:
 *           type: worker
 *           topic: order.fulfill
 *       transitions:
 *         validate:
 *           - to: approve
 *         approve:
 *           - to: fulfill
 * `);
 *
 * // Activate the workflow version
 * await hotMesh.activate('1');
 *
 * // Execute workflow (fire-and-forget)
 * const jobId = await hotMesh.pub('order.process', {
 *   orderId: '12345',
 *   amount: 99.99
 * });
 *
 * // Execute workflow and wait for result
 * const result = await hotMesh.pubsub('order.process', {
 *   orderId: '12345',
 *   amount: 99.99
 * });
 * ```
 *
 * ## Postgres Backend Example
 *
 * @example
 * ```typescript
 * import { HotMesh } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const hotMesh = await HotMesh.init({
 *   appId: 'my-app',
 *   engine: {
 *     connection: {
 *       class: Postgres,
 *       options: {
 *         connectionString: 'postgresql://user:pass@localhost:5432/db'
 *       }
 *     }
 *   }
 * });
 * ```
 *
 * ## Advanced Features
 *
 * **Pattern Subscriptions**: Listen to multiple workflow topics
 * ```typescript
 * await hotMesh.psub('order.*', (topic, message) => {
 *   console.log(`Received ${topic}:`, message);
 * });
 * ```
 *
 * **Throttling**: Control processing rates
 * ```typescript
 * // Pause all processing for 5 seconds
 * await hotMesh.throttle({ throttle: 5000 });
 *
 * // Emergency stop (pause indefinitely)
 * await hotMesh.throttle({ throttle: -1 });
 * ```
 *
 * **Workflow Interruption**: Gracefully stop running workflows
 * ```typescript
 * await hotMesh.interrupt('order.process', jobId, {
 *   reason: 'User cancellation'
 * });
 * ```
 *
 * **State Inspection**: Query workflow state and progress
 * ```typescript
 * const state = await hotMesh.getState('order.process', jobId);
 * const status = await hotMesh.getStatus(jobId);
 * ```
 *
 * ## Distributed Coordination
 *
 * HotMesh automatically handles distributed coordination through its quorum system:
 *
 * ```typescript
 * // Check quorum health
 * const members = await hotMesh.rollCall();
 *
 * // Coordinate version activation across all instances
 * await hotMesh.activate('2', 1000); // 1 second delay for consensus
 * ```
 *
 * ## Integration with Higher-Level Modules
 *
 * For most use cases, consider using the higher-level modules:
 * - **MemFlow**: For Temporal.io-style workflows with TypeScript functions
 * - **MeshCall**: For durable function calls and RPC patterns
 *
 * ## Cleanup
 *
 * Always clean up resources when shutting down:
 * ```typescript
 * // Stop this instance
 * hotMesh.stop();
 *
 * // Stop all instances (typically in signal handlers)
 * await HotMesh.stop();
 * ```
 *
 * @see {@link https://docs.hotmesh.io/} - Complete documentation
 * @see {@link https://github.com/hotmeshio/samples-typescript} - Examples and tutorials
 * @see {@link https://zenodo.org/records/12168558} - White paper on the architecture
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
   * Instance initializer. Workers are configured
   * similarly to the engine, but as an array with
   * multiple worker objects.
   *
   * @example
   * ```typescript
   * const config: HotMeshConfig = {
   *   appId: 'myapp',
   *   engine: {
   *     connection: {
   *       class: Postgres,
   *       options: {
   *         connectionString: 'postgresql://usr:pwd@localhost:5432/db',
   *       }
   *     }
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
      //connections that are 'readonly' transfer
      //this property directly to the engine,
      //and ALWAYS take precendence.
      if (config.engine.connection.readonly) {
        config.engine.readonly = true;
      }

      // Initialize task queue for engine
      config.engine.taskQueue = this.initTaskQueue(
        config.engine.taskQueue,
        config.taskQueue,
      );

      await ConnectorService.initClients(config.engine);
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
    // Initialize task queues for workers
    if (config.workers) {
      for (const worker of config.workers) {
        worker.taskQueue = this.initTaskQueue(
          worker.taskQueue,
          config.taskQueue,
        );
      }
    }

    this.workers = await WorkerService.init(
      this.namespace,
      this.appId,
      this.guid,
      config,
      logger,
    );
  }

  /**
   * Initialize task queue with proper precedence:
   * 1. Use component-specific queue if set (engine/worker)
   * 2. Use global config queue if set
   * 3. Use default queue as fallback
   * @private
   */
  private initTaskQueue(componentQueue?: string, globalQueue?: string): string {
    // Component-specific queue takes precedence
    if (componentQueue) {
      return componentQueue;
    }

    // Global config queue is next
    if (globalQueue) {
      return globalQueue;
    }

    // Default queue as fallback
    return DEFAULT_TASK_QUEUE;
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
   * workflow topic. NOTE: Postgres does not support patterned
   * unsubscription, so this method is not supported for Postgres.
   *
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
   * Patterned unsubscribe. NOTE: Postgres does not support patterned
   * unsubscription, so this method is not supported for Postgres.
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
   * When the app YAML descriptor file is ready, the `deploy` function can be called.
   * This function is responsible for merging all referenced YAML source
   * files and writing the JSON output to the file system and to the provider backend. It
   * is also possible to embed the YAML in-line as a string.
   *
   * *The version will not be active until activation is explicitly called.*
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return await this.engine?.deploy(pathOrYAML);
  }
  /**
   * Once the app YAML file is deployed to the provider backend, the `activate` function can be
   * called to enable it for the entire quorum at the same moment.
   *
   * The approach is to establish the coordinated health of the system through series
   * of call/response exchanges. Once it is established that the quorum is healthy,
   * the quorum is instructed to run their engine in `no-cache` mode, ensuring
   * that the provider backend is consulted for the active app version each time a
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
      await ConnectorService.disconnectAll();
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
