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
import { ProviderConfig, ProvidersConfig } from '../../types/provider';
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
  RetryPolicy,
} from '../../types/stream';
import { MAX_DELAY, DEFAULT_TASK_QUEUE } from '../../modules/enums';

/**
 * A distributed service mesh that turns Postgres into a durable workflow
 * orchestration engine. Every `HotMesh.init()` call creates a **point of
 * presence** — an engine, a quorum member, and zero or more workers — that
 * collaborates with its peers through Postgres LISTEN/NOTIFY to form a
 * self-coordinating mesh with no external dependencies.
 *
 * ## Service Mesh Architecture
 *
 * Each HotMesh instance joins a **quorum** — a real-time pub/sub channel
 * backed by Postgres LISTEN/NOTIFY. The quorum is the mesh's nervous
 * system: version activations, throttle commands, roll calls, and custom
 * user messages all propagate instantly to every connected engine and
 * worker across all processes and servers.
 * 
 * ## Quick Start
 *
 * ```typescript
 * import { HotMesh } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const hotMesh = await HotMesh.init({
 *   appId: 'myapp',
 *   engine: {
 *     connection: {
 *       class: Postgres,
 *       options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *     },
 *   },
 *   workers: [{
 *     topic: 'order.process',
 *     connection: {
 *       class: Postgres,
 *       options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *     },
 *     callback: async (data) => ({
 *       metadata: { ...data.metadata },
 *       data: { orderId: data.data.id, status: 'fulfilled' },
 *     }),
 *   }],
 * });
 *
 * // Deploy a YAML workflow graph
 * await hotMesh.deploy(`
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: order.placed
 *       publishes: order.fulfilled
 *       expire: 600
 *
 *       activities:
 *         t1:
 *           type: trigger
 *         process:
 *           type: worker
 *           topic: order.process
 *           input:
 *             schema:
 *               type: object
 *               properties:
 *                 id: { type: string }
 *             maps:
 *               id: '{t1.output.data.id}'
 *       transitions:
 *         t1:
 *           - to: process
 * `);
 *
 * await hotMesh.activate('1');
 *
 * // Fire-and-forget
 * const jobId = await hotMesh.pub('order.placed', { id: 'ORD-123' });
 *
 * // Request/response (blocks until workflow completes)
 * const result = await hotMesh.pubsub('order.placed', { id: 'ORD-456' });
 * ```
 *
 * ## Quorum: The Mesh Control Plane
 *
 * The quorum channel is a broadcast bus available to every mesh member.
 * Use it for operational control, observability, and custom messaging.
 *
 * ```typescript
 * // Roll call — discover every engine and worker in the mesh
 * const members = await hotMesh.rollCall();
 * // => [{ engine_id, worker_topic, throttle, system: { CPULoad, ... } }, ...]
 *
 * // Subscribe to ALL quorum traffic (throttle, activate, pong, job, user)
 * await hotMesh.subQuorum((topic, message) => {
 *   switch (message.type) {
 *     case 'pong':      // roll call response from a mesh member
 *       console.log(`Member ${message.guid} on topic ${message.profile?.worker_topic}`);
 *       break;
 *     case 'throttle':  // a throttle command was broadcast
 *       console.log(`Throttle ${message.throttle}ms on ${message.topic ?? 'all'}`);
 *       break;
 *     case 'activate':  // a version activation is in progress
 *       console.log(`Activating version ${message.until_version}`);
 *       break;
 *     case 'job':       // a workflow completed and published its result
 *       console.log(`Job done on ${message.topic}:`, message.job);
 *       break;
 *     case 'user':      // a custom user message
 *       console.log(`User event ${message.topic}:`, message.message);
 *       break;
 *   }
 * });
 *
 * // Publish a custom message to every mesh member
 * await hotMesh.pubQuorum({
 *   type: 'user',
 *   topic: 'deploy.notify',
 *   message: { version: '2.1.0', deployer: 'ci-pipeline' },
 * });
 * ```
 *
 * ## Throttling: Backpressure Across the Mesh
 *
 * Throttle commands propagate instantly to every targeted member via
 * the quorum channel, providing fine-grained flow control.
 *
 * ```typescript
 * // Pause the ENTIRE mesh (emergency stop)
 * await hotMesh.throttle({ throttle: -1 });
 *
 * // Resume the entire mesh
 * await hotMesh.throttle({ throttle: 0 });
 *
 * // Slow a specific worker topic to 1 message per 500ms
 * await hotMesh.throttle({ throttle: 500, topic: 'order.process' });
 *
 * // Throttle a single engine/worker instance by GUID
 * await hotMesh.throttle({ throttle: 2000, guid: 'abc-123' });
 *
 * // Combine: throttle a specific topic on a specific instance
 * await hotMesh.throttle({ throttle: 1000, guid: 'abc-123', topic: 'order.process' });
 * ```
 *
 * ## Lifecycle
 *
 * 1. **`init`** — Create an engine + workers; join the quorum.
 * 2. **`deploy`** — Upload a YAML graph to Postgres (inactive).
 * 3. **`activate`** — Coordinate the quorum to switch to the new version.
 * 4. **`pub` / `pubsub`** — Trigger workflow execution.
 * 5. **`stop`** — Leave the quorum and release connections.
 *
 * ## Higher-Level Modules
 *
 * For most use cases, prefer the higher-level wrappers:
 * - **Durable** — Temporal-style durable workflow functions.
 * - **Virtual** — Virtual network functions and idempotent RPC.
 *
 * @see {@link https://hotmeshio.github.io/sdk-typescript/} - API reference
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
   * ## Retry Policy Configuration
   *
   * HotMesh supports retry policies with exponential backoff. Retry behavior
   * can be configured independently on both the `engine` and individual
   * `workers`. They are **not inherited**; each operates at its own level.
   *
   * - **Engine `retryPolicy`**: Stamps messages the engine publishes with
   *   retry metadata (stored as Postgres columns). Workers that consume
   *   these messages will use the embedded config when handling failures.
   * - **Worker `retryPolicy`**: Used as the fallback when the consumed
   *   message does not carry explicit retry metadata.
   *
   * @example Basic Configuration
   * ```typescript
   * const hotMesh = await HotMesh.init({
   *   appId: 'myapp',
   *   engine: {
   *     connection: {
   *       class: Postgres,
   *       options: {
   *         connectionString: 'postgresql://usr:pwd@localhost:5432/db',
   *       }
   *     }
   *   },
   *   workers: [...]
   * });
   * ```
   *
   * @example Engine Retry Policy
   * ```typescript
   * const hotMesh = await HotMesh.init({
   *   appId: 'myapp',
   *   engine: {
   *     connection: {
   *       class: Postgres,
   *       options: { connectionString: 'postgresql://...' }
   *     },
   *     retryPolicy: {
   *       maximumAttempts: 5,
   *       backoffCoefficient: 2,
   *       maximumInterval: '300s'
   *     }
   *   }
   * });
   * ```
   *
   * @example Worker Retry Policy
   * ```typescript
   * const hotMesh = await HotMesh.init({
   *   appId: 'myapp',
   *   engine: { connection },
   *   workers: [{
   *     topic: 'order.process',
   *     connection,
   *     retryPolicy: {
   *       maximumAttempts: 5,
   *       backoffCoefficient: 2,
   *       maximumInterval: '30s',
   *     },
   *     callback: async (data: StreamData) => {
   *       const result = await doWork(data.data);
   *       return {
   *         code: 200,
   *         status: StreamStatus.SUCCESS,
   *         metadata: { ...data.metadata },
   *         data: { result },
   *       } as StreamDataResponse;
   *     }
   *   }]
   * });
   * ```
   *
   * **Retry Policy Options**:
   * - `maximumAttempts` - Maximum retry attempts before failure (default: 3)
   * - `backoffCoefficient` - Base for exponential backoff calculation (default: 10)
   * - `maximumInterval` - Maximum delay between retries in seconds or duration string (default: '120s')
   *
   * **Retry Delays**: For `backoffCoefficient: 2`, delays are: 2s, 4s, 8s, 16s, 32s...
   * capped at `maximumInterval`.
   *
   * **Note**: Retry policies are stored in PostgreSQL columns for efficient querying and
   * observability. Each retry creates a new message, preserving message immutability.
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
   * Generates a unique ID using the same nanoid generator used
   * internally by HotMesh for job IDs and GUIDs.
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

      // Apply retry policy to stream connection if provided
      if (config.engine.retryPolicy) {
        this.applyRetryPolicy(config.engine.connection, config.engine.retryPolicy);
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
        // Apply retry policy to stream connection if provided
        if (worker.retryPolicy) {
          this.applyRetryPolicy(worker.connection, worker.retryPolicy);
        }

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

  /**
   * Apply retry policy to the stream connection within a ProviderConfig or ProvidersConfig.
   * Handles both short-form (ProviderConfig) and long-form (ProvidersConfig) connection configs.
   * @private
   */
  private applyRetryPolicy(
    connection: ProviderConfig | ProvidersConfig,
    retryPolicy: RetryPolicy,
  ): void {
    // Check if this is ProvidersConfig (has 'stream' property)
    if ('stream' in connection && connection.stream) {
      // Long-form: apply to the stream sub-config
      connection.stream.retryPolicy = retryPolicy;
    } else {
      // Short-form: apply directly to the connection
      (connection as ProviderConfig).retryPolicy = retryPolicy;
    }
  }

  // ************* PUB/SUB METHODS *************
  /**
   * Publishes a message to a workflow topic, starting a new job.
   * Returns the job ID immediately (fire-and-forget). Use `pubsub`
   * to block until the workflow completes.
   *
   * @example
   * ```typescript
   * const jobId = await hotMesh.pub('order.placed', {
   *   id: 'ORD-123',
   *   amount: 99.99,
   * });
   * console.log(`Started job ${jobId}`);
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
   * Subscribes to all output and interim emissions from a specific
   * workflow topic. The callback fires each time a job on that topic
   * completes or emits an interim result.
   *
   * @example
   * ```typescript
   * await hotMesh.sub('order.fulfilled', (topic, message) => {
   *   console.log(`Order completed:`, message.data);
   * });
   * ```
   */
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.sub(topic, callback);
  }
  /**
   * Unsubscribes from a single workflow topic previously registered
   * with `sub()`.
   */
  async unsub(topic: string): Promise<void> {
    return await this.engine?.unsub(topic);
  }
  /**
   * Subscribes to workflow emissions matching a wildcard pattern.
   * Useful for monitoring an entire domain of workflows at once.
   *
   * @example
   * ```typescript
   * // Listen to all order-related workflow completions
   * await hotMesh.psub('order.*', (topic, message) => {
   *   console.log(`${topic} completed:`, message.data);
   * });
   * ```
   */
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.psub(wild, callback);
  }
  /**
   * Unsubscribes from a wildcard pattern previously registered with `psub()`.
   */
  async punsub(wild: string): Promise<void> {
    return await this.engine?.punsub(wild);
  }
  /**
   * Publishes a message to a workflow topic and blocks until the workflow
   * completes, returning the final job output. Internally subscribes to
   * the workflow's `publishes` topic before publishing, then unsubscribes
   * after receiving the result.
   *
   * @example
   * ```typescript
   * const result = await hotMesh.pubsub('order.placed', {
   *   id: 'ORD-789',
   *   amount: 49.99,
   * });
   * console.log('Order result:', result.data);
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
   * Adds a transition message to the workstream, resuming Leg 2 of a
   * paused reentrant activity (e.g., `await`, `worker`, `hook`). This
   * is typically called by the engine internally but is exposed for
   * advanced use cases like custom activity implementations.
   */
  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return (await this.engine.add(streamData)) as string;
  }

  // ************* QUORUM METHODS *************
  /**
   * Broadcasts a roll call across the mesh and collects responses from
   * every connected engine and worker. Each member replies with its
   * `QuorumProfile` — including GUID, worker topic, stream depth,
   * throttle rate, and system health (CPU, memory, network).
   *
   * Use this for service discovery, health checks, and capacity planning.
   *
   * @example
   * ```typescript
   * const members = await hotMesh.rollCall();
   * for (const member of members) {
   *   console.log(
   *     `${member.engine_id} | topic=${member.worker_topic ?? 'engine'} ` +
   *     `| throttle=${member.throttle}ms | depth=${member.stream_depth}`,
   *   );
   * }
   * ```
   */
  async rollCall(delay?: number): Promise<QuorumProfile[]> {
    return await this.quorum?.rollCall(delay);
  }
  /**
   * Broadcasts a throttle command to the mesh via the quorum channel.
   * Targeted members insert a delay (in milliseconds) before processing
   * their next stream message, providing instant backpressure control
   * across any combination of engines and workers.
   *
   * Throttling is **stateless** — no data is lost. Messages accumulate
   * in Postgres streams and are processed once the throttle is lifted.
   *
   * ## Targeting
   *
   * | Option  | Effect |
   * |---------|--------|
   * | *(none)* | Throttle the **entire mesh** (all engines + all workers) |
   * | `topic` | Throttle all workers subscribed to this topic |
   * | `guid`  | Throttle a single engine or worker instance |
   * | `topic` + `guid` | Throttle a specific topic on a specific instance |
   *
   * ## Special Values
   *
   * | Value  | Effect |
   * |--------|--------|
   * | `0`    | Resume normal processing (remove throttle) |
   * | `-1`   | Pause indefinitely (emergency stop) |
   * | `500`  | 500ms delay between messages |
   *
   * @example
   * ```typescript
   * // Emergency stop: pause the entire mesh
   * await hotMesh.throttle({ throttle: -1 });
   *
   * // Resume the entire mesh
   * await hotMesh.throttle({ throttle: 0 });
   *
   * // Slow a specific worker topic to 1 msg per second
   * await hotMesh.throttle({ throttle: 1000, topic: 'order.process' });
   *
   * // Throttle a single instance by GUID
   * await hotMesh.throttle({ throttle: 2000, guid: 'abc-123' });
   *
   * // Throttle a specific topic on a specific instance
   * await hotMesh.throttle({
   *   throttle: 500,
   *   guid: 'abc-123',
   *   topic: 'payment.charge',
   * });
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
   * Publishes a message to every mesh member via the quorum channel
   * (Postgres LISTEN/NOTIFY). Any `QuorumMessage` type can be sent,
   * but the `user` type is the most common for application-level
   * messaging.
   *
   * @example
   * ```typescript
   * // Broadcast a custom event to all mesh members
   * await hotMesh.pubQuorum({
   *   type: 'user',
   *   topic: 'deploy.notify',
   *   message: { version: '2.1.0', deployer: 'ci-pipeline' },
   * });
   *
   * // Broadcast a config-reload signal
   * await hotMesh.pubQuorum({
   *   type: 'user',
   *   topic: 'config.reload',
   *   message: { features: { darkMode: true } },
   * });
   * ```
   */
  async pubQuorum(quorumMessage: QuorumMessage) {
    return await this.quorum?.pub(quorumMessage);
  }
  /**
   * Subscribes to the quorum channel, receiving **every** message
   * broadcast across the mesh in real time. This is the primary
   * observability hook into the service mesh — use it to monitor
   * version activations, throttle commands, roll call responses,
   * workflow completions, and custom user events.
   *
   * Messages arrive as typed `QuorumMessage` unions. Switch on
   * `message.type` to handle each:
   *
   * | Type        | When it fires |
   * |-------------|---------------|
   * | `pong`      | A mesh member responds to a roll call |
   * | `throttle`  | A throttle command was broadcast |
   * | `activate`  | A version activation is in progress |
   * | `job`       | A workflow completed and published its result |
   * | `user`      | A custom user message (via `pubQuorum`) |
   * | `ping`      | A roll call was initiated |
   * | `work`      | A work distribution event |
   * | `cron`      | A cron/scheduled event |
   *
   * @example
   * ```typescript
   * // Build a real-time mesh dashboard
   * await hotMesh.subQuorum((topic, message) => {
   *   switch (message.type) {
   *     case 'pong':
   *       dashboard.updateMember(message.guid, {
   *         topic: message.profile?.worker_topic,
   *         throttle: message.profile?.throttle,
   *         depth: message.profile?.stream_depth,
   *         cpu: message.profile?.system?.CPULoad,
   *       });
   *       break;
   *
   *     case 'throttle':
   *       dashboard.logThrottle(
   *         message.throttle,
   *         message.topic,
   *         message.guid,
   *       );
   *       break;
   *
   *     case 'job':
   *       dashboard.logCompletion(message.topic, message.job);
   *       break;
   *
   *     case 'user':
   *       dashboard.logUserEvent(message.topic, message.message);
   *       break;
   *   }
   * });
   * ```
   *
   * @example
   * ```typescript
   * // React to custom deployment events
   * await hotMesh.subQuorum((topic, message) => {
   *   if (message.type === 'user' && message.topic === 'config.reload') {
   *     reloadFeatureFlags(message.message);
   *   }
   * });
   * ```
   *
   * @example
   * ```typescript
   * // Log all mesh activity for audit
   * await hotMesh.subQuorum((topic, message) => {
   *   auditLog.append({
   *     timestamp: Date.now(),
   *     type: message.type,
   *     guid: message.guid,
   *     topic: message.topic,
   *     payload: message,
   *   });
   * });
   * ```
   */
  async subQuorum(callback: QuorumMessageCallback): Promise<void> {
    return await this.quorum?.sub(callback);
  }
  /**
   * Unsubscribes a callback previously registered with `subQuorum()`.
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
   * Deploys a YAML workflow graph to Postgres. Accepts a file path or
   * an inline YAML string. Referenced `$ref` files are resolved and
   * merged. The deployed version is **inactive** until `activate()` is
   * called.
   *
   * @example
   * ```typescript
   * // Deploy from an inline YAML string
   * await hotMesh.deploy(`
   * app:
   *   id: myapp
   *   version: '2'
   *   graphs:
   *     - subscribes: order.placed
   *       activities:
   *         t1:
   *           type: trigger
   *         process:
   *           type: worker
   *           topic: order.process
   *       transitions:
   *         t1:
   *           - to: process
   * `);
   *
   * // Deploy from a file path (resolves $ref references)
   * await hotMesh.deploy('./workflows/order.yaml');
   * ```
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return await this.engine?.deploy(pathOrYAML);
  }
  /**
   * Activates a deployed version across the entire mesh. The quorum
   * coordinates a synchronized switch-over:
   *
   * 1. Roll call to verify quorum health.
   * 2. Broadcast `nocache` mode — all engines consult Postgres for the
   *    active version on every request.
   * 3. Set the new version as active.
   * 4. Broadcast `cache` mode — engines resume caching.
   *
   * The optional `delay` adds a pause (in ms) for the quorum to reach
   * consensus under heavy traffic. Combine with `throttle()` for
   * zero-downtime version switches.
   *
   * @example
   * ```typescript
   * // Simple activation
   * await hotMesh.activate('2');
   *
   * // With consensus delay under heavy traffic
   * await hotMesh.throttle({ throttle: 500 });   // slow the mesh
   * await hotMesh.activate('2', 2000);            // activate with 2s consensus window
   * await hotMesh.throttle({ throttle: 0 });      // resume full speed
   * ```
   */
  async activate(version: string, delay?: number): Promise<boolean> {
    return await this.quorum?.activate(version, delay);
  }

  /**
   * Exports the full job state as a structured JSON object, including
   * activity data, transitions, and dependency chains. Useful for
   * debugging, auditing, and visualizing workflow execution.
   */
  async export(jobId: string): Promise<JobExport> {
    return await this.engine?.export(jobId);
  }
  /**
   * Returns all raw key-value pairs for a job's HASH record. This is
   * the lowest-level read — it returns internal engine fields alongside
   * user data. Prefer `getState()` for structured output.
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
   * Returns the numeric status semaphore for a job.
   *
   * | Value            | Meaning |
   * |------------------|---------|
   * | `> 0`            | Running (count of open activities) |
   * | `0`              | Completed normally |
   * | `-1`             | Pending (awaiting activation) |
   * | `< -100,000,000` | Interrupted (abnormal termination) |
   */
  async getStatus(jobId: string): Promise<JobStatus> {
    return this.engine?.getStatus(jobId);
  }
  /**
   * Returns the structured job state (data and metadata) for a job,
   * scoped to the given workflow topic.
   *
   * @example
   * ```typescript
   * const state = await hotMesh.getState('order.placed', jobId);
   * console.log(state.data);     // workflow output data
   * console.log(state.metadata); // jid, aid, timestamps, etc.
   * ```
   */
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return this.engine?.getState(topic, jobId);
  }
  /**
   * Returns specific searchable fields from a job's HASH record.
   * Pass field names to retrieve; use `":"` to read the reserved
   * status field.
   *
   * @example
   * ```typescript
   * const fields = ['orderId', 'status', '":"'];
   * const data = await hotMesh.getQueryState(jobId, fields);
   * // => { orderId: 'ORD-123', status: 'paid', ':': '0' }
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
   * Interrupts (terminates) an active workflow job. The job's status is
   * set to an error code indicating abnormal termination, and any pending
   * activities or timers are cancelled.
   *
   * @example
   * ```typescript
   * await hotMesh.interrupt('order.placed', jobId, {
   *   reason: 'Customer cancelled',
   *   descend: true,   // also interrupt child workflows
   * });
   * ```
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return await this.engine?.interrupt(topic, jobId, options);
  }

  /**
   * Immediately deletes a completed job from the system. The job must
   * have a non-positive status (completed or interrupted). Running jobs
   * must be interrupted first.
   */
  async scrub(jobId: string) {
    await this.engine?.scrub(jobId);
  }

  /**
   * Sends a signal to a paused workflow, resuming its execution.
   * The `topic` must match a hook rule defined in the YAML graph's
   * `hooks` section. The engine locates the exact activity and
   * dimension for reentry based on the hook rule's match conditions.
   *
   * Use this to deliver external data (approval decisions, webhook
   * payloads, partner responses) into a workflow that is sleeping
   * on a hook activity or awaiting a `waitFor()` signal.
   *
   * @example
   * ```typescript
   * // Resume a paused approval workflow with external data
   * await hotMesh.signal('order.approval', {
   *   id: jobId,
   *   approved: true,
   *   reviewer: 'manager@example.com',
   * });
   * ```
   *
   * @example
   * ```typescript
   * // Signal a Durable workflow waiting on waitFor('payment-received')
   * await hotMesh.signal(`${appId}.wfs.signal`, {
   *   id: 'payment-received',
   *   data: { amount: 99.99, currency: 'USD' },
   * });
   * ```
   */
  async signal(
    topic: string,
    data: JobData,
    status?: StreamStatus,
    code?: StreamCode,
  ): Promise<string> {
    return await this.engine?.signal(topic, data, status, code);
  }

  /**
   * Fan-out variant of `signal()` that delivers data to **all**
   * paused workflows matching a search query. Useful for resuming
   * a batch of workflows waiting on the same external event.
   *
   * @private
   */
  async signalAll(
    hookTopic: string,
    data: JobData,
    query: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<string[]> {
    return await this.engine?.signalAll(hookTopic, data, query, queryFacets);
  }

  /**
   * Stops **all** HotMesh instances in the current process — engines,
   * workers, and connections. Typically called in signal handlers
   * (`SIGTERM`, `SIGINT`) for graceful shutdown.
   *
   * @example
   * ```typescript
   * process.on('SIGTERM', async () => {
   *   await HotMesh.stop();
   *   process.exit(0);
   * });
   * ```
   */
  static async stop() {
    if (!this.disconnecting) {
      this.disconnecting = true;
      await Router.stopConsuming();
      await ConnectorService.disconnectAll();
    }
  }

  /**
   * Stops this specific HotMesh instance — its engine, quorum
   * membership, and all attached workers. Other instances in the
   * same process are unaffected.
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
