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
import { ExportOptions, JobExport } from '../../types/exporter';
import {
  JobMessageCallback,
  QuorumMessage,
  QuorumMessageCallback,
  QuorumProfile,
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

// Submodules
import * as Validation from './validation';
import * as Init from './init';
import * as PubSub from './pubsub';
import * as Quorum from './quorum';
import * as Deployment from './deployment';
import * as Jobs from './jobs';

// Codec and credential re-exports
import { SerializerService } from '../serializer';
import type { PayloadCodec } from '../../types/codec';
import * as WorkerCredentials from '../worker/credentials';
import type {
  WorkerCredential,
  WorkerCredentialInfo,
} from '../worker/credentials';

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
 * - **Durable** — Durable workflow functions with replay and retry.
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
  constructor() {}

  /**
   * @private
   */
  verifyAndSetNamespace(namespace?: string) {
    Validation.verifyAndSetNamespace(this, namespace);
  }

  /**
   * @private
   */
  verifyAndSetAppId(appId: string) {
    Validation.verifyAndSetAppId(this, appId);
  }

  /**
   * Create a HotMesh instance with an engine and optional workers.
   *
   * The engine manages workflow state in Postgres. Workers are callback
   * functions that consume messages from Postgres streams — they can
   * run on the same process or on entirely separate servers.
   *
   * @param config - Engine connection, worker definitions, app ID, and options.
   * @returns A running HotMesh instance joined to the quorum.
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
    await Init.initEngine(instance, config, instance.logger);
    await Init.initQuorum(instance, config, instance.engine, instance.logger);
    await Init.doWork(instance, config, instance.logger);
    return instance;
  }

  /**
   * Generates a unique ID using the same nanoid generator used
   * internally by HotMesh for job IDs and GUIDs.
   */
  static guid(): string {
    return guid();
  }

  // ************* PUB/SUB METHODS *************

  /**
   * Publishes a message to a workflow topic, starting a new job.
   * Returns the job ID immediately (fire-and-forget).
   *
   * @param topic - The workflow topic (must match a deployed graph's `subscribes`).
   * @param data - Input data for the workflow.
   * @returns The new job ID.
   */
  async pub(
    topic: string,
    data: JobData = {},
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    return PubSub.pub(this, topic, data, context, extended);
  }

  /**
   * Subscribes to all output and interim emissions from a workflow topic.
   *
   * @param topic - The topic to subscribe to.
   * @param callback - Invoked with each job output or interim emission.
   */
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.sub(this, topic, callback);
  }

  /**
   * Unsubscribes from a workflow topic previously registered with {@link sub}.
   */
  async unsub(topic: string): Promise<void> {
    return PubSub.unsub(this, topic);
  }

  /**
   * Subscribes to workflow emissions matching a wildcard pattern.
   *
   * @param wild - The wildcard pattern (e.g., `'order.*'`).
   * @param callback - Invoked with each matching emission.
   */
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.psub(this, wild, callback);
  }

  /**
   * Unsubscribes from a wildcard pattern previously registered with {@link psub}.
   */
  async punsub(wild: string): Promise<void> {
    return PubSub.punsub(this, wild);
  }

  /**
   * Publishes a message and blocks until the workflow completes,
   * returning the final job output. Combines {@link pub} + {@link sub}
   * into a single request/response call.
   *
   * @param topic - The workflow topic.
   * @param data - Input data for the workflow.
   * @param context - Optional job state context.
   * @param timeout - Optional timeout in milliseconds.
   * @returns The completed job output.
   */
  async pubsub(
    topic: string,
    data: JobData = {},
    context?: JobState | null,
    timeout?: number,
  ): Promise<JobOutput> {
    return PubSub.pubsub(this, topic, data, context, timeout);
  }

  /**
   * Adds a transition message to the workstream, resuming Leg 2 of a
   * paused reentrant activity.
   * @private
   */
  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return PubSub.add(this, streamData);
  }

  // ************* QUORUM METHODS *************

  /**
   * Broadcasts a PING to all connected engines and workers via
   * LISTEN/NOTIFY and collects their profiles. Returns one
   * {@link QuorumProfile} per responding instance, including
   * cumulative message `counts`, `error_count`, `stream_depth`,
   * `throttle` state, and host-level `system` health (memory/CPU).
   *
   * Use this for health checks, topology discovery, and throughput
   * monitoring across the mesh.
   *
   * @param delay - Time in ms to wait for PONG responses (default: quorum config).
   * @returns One profile per responding engine/worker instance.
   */
  async rollCall(delay?: number): Promise<QuorumProfile[]> {
    return Quorum.rollCall(this, delay);
  }

  /**
   * Broadcasts a throttle command to all instances. Use to slow down or
   * pause message consumption across the mesh.
   *
   * @param options - Throttle rate in ms (0 = no throttle, -1 = pause indefinitely).
   *   Optionally scope by `guid` (single instance) or `topic` (single worker).
   */
  async throttle(options: ThrottleOptions): Promise<boolean> {
    return Quorum.throttle(this, options);
  }

  /**
   * Publishes a custom message to every instance via the quorum channel.
   * Register a listener with {@link subQuorum} to receive these messages.
   */
  async pubQuorum(quorumMessage: QuorumMessage) {
    return Quorum.pubQuorum(this, quorumMessage);
  }

  /**
   * Subscribes to the quorum channel to receive system messages (version
   * activations, throttle commands, roll calls) and custom user messages.
   */
  async subQuorum(callback: QuorumMessageCallback): Promise<void> {
    return Quorum.subQuorum(this, callback);
  }

  /**
   * Unsubscribes a callback previously registered with {@link subQuorum}.
   */
  async unsubQuorum(callback: QuorumMessageCallback): Promise<void> {
    return Quorum.unsubQuorum(this, callback);
  }

  // ************* LIFECYCLE METHODS *************

  /** @private */
  async plan(path: string): Promise<HotMeshManifest> {
    return Deployment.plan(this, path);
  }

  /**
   * Deploys a YAML workflow graph to Postgres. The graph is stored but
   * remains **inactive** until {@link activate} is called.
   *
   * @param pathOrYAML - A file path or raw YAML string defining the workflow graph.
   * @returns The parsed manifest with version and graph metadata.
   */
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return Deployment.deploy(this, pathOrYAML);
  }

  /**
   * Activates a previously deployed version across all connected instances.
   * The quorum coordinates a synchronized version switch so every engine
   * and worker transitions together.
   *
   * @param version - The version string to activate (must match a deployed graph).
   * @param delay - Optional delay in ms before activation takes effect.
   */
  async activate(version: string, delay?: number): Promise<boolean> {
    return Deployment.activate(this, version, delay);
  }

  // ************* JOB METHODS *************

  /**
   * Exports the full job state (data, metadata, activity results) as
   * a structured JSON object.
   *
   * @param jobId - The job/workflow ID.
   * @param options - Export options (e.g., include activity details).
   */
  async export(jobId: string, options: ExportOptions = {}): Promise<JobExport> {
    return Jobs.exportJob(this, jobId, options);
  }

  /**
   * Returns all raw key-value pairs from a job's HASH record in Postgres.
   * Useful for debugging or low-level inspection.
   */
  async getRaw(jobId: string): Promise<StringStringType> {
    return Jobs.getRaw(this, jobId);
  }

  /** @private */
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return Jobs.getStats(this, topic, query);
  }

  /**
   * Returns the numeric status code for a job: `0` = completed,
   * positive = still running, negative = interrupted/errored.
   */
  async getStatus(jobId: string): Promise<JobStatus> {
    return Jobs.getStatus(this, jobId);
  }

  /**
   * Returns the structured job state (data + metadata). For a completed
   * job this is the final output; for a running job it reflects the
   * latest persisted state.
   *
   * @param topic - The workflow topic.
   * @param jobId - The job/workflow ID.
   */
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return Jobs.getState(this, topic, jobId);
  }

  /**
   * Returns specific searchable fields from a job's HASH record.
   *
   * @param jobId - The job/workflow ID.
   * @param fields - The field names to retrieve.
   */
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return Jobs.getQueryState(this, jobId, fields);
  }

  /** @private */
  async getIds(
    topic: string,
    query: JobStatsInput,
    queryFacets = [],
  ): Promise<IdsResponse> {
    return Jobs.getIds(this, topic, query, queryFacets);
  }

  /** @private */
  async resolveQuery(
    topic: string,
    query: JobStatsInput,
  ): Promise<GetStatsOptions> {
    return Jobs.resolveQuery(this, topic, query);
  }

  /**
   * Immediately terminates a running job. The job is marked as
   * interrupted and its HASH is expired. Unlike {@link cancel}, this
   * does not give the workflow a chance to run cleanup code.
   *
   * @param topic - The workflow topic.
   * @param jobId - The job/workflow ID.
   * @param options - Optional interrupt configuration.
   */
  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return Jobs.interrupt(this, topic, jobId, options);
  }

  /**
   * Requests cooperative cancellation of a running job. Sets a durable
   * cancel flag; the workflow detects it at its next durable operation
   * and throws `CancelledFailure`, which can be caught for cleanup.
   *
   * @param jobId - The job/workflow ID.
   */
  async cancel(jobId: string): Promise<void> {
    return Jobs.cancel(this, jobId);
  }

  /**
   * Immediately deletes a completed job's HASH record from Postgres.
   */
  async scrub(jobId: string) {
    return Jobs.scrub(this, jobId);
  }

  /**
   * Sends a signal to a paused workflow, delivering data and resuming
   * execution. Pairs with `condition()` in the Durable workflow API.
   *
   * @param topic - The signal topic.
   * @param data - Signal payload.
   */
  async signal(
    topic: string,
    data: JobData,
    status?: StreamStatus,
    code?: StreamCode,
  ): Promise<string> {
    return Jobs.signal(this, topic, data, status, code);
  }

  /** @private */
  async signalAll(
    hookTopic: string,
    data: JobData,
    query: JobStatsInput,
    queryFacets: string[] = [],
  ): Promise<string[]> {
    return Jobs.signalAll(this, hookTopic, data, query, queryFacets);
  }

  // ************* STATIC LIFECYCLE *************

  /**
   * Stops **all** HotMesh instances in the current process.
   */
  static async stop() {
    if (!this.disconnecting) {
      this.disconnecting = true;
      await Router.stopConsuming();
      await ConnectorService.disconnectAll();
    }
  }

  /**
   * Stops this specific HotMesh instance — leaves the quorum and
   * stops all workers. Does not affect other instances in the process.
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

  // ************* CODEC *************

  /**
   * Register a global payload codec for encoding/decoding serialized
   * object data at rest. Once registered, all object values flowing
   * through the serializer are stored as `/b{encoded}` instead of
   * `/s{json}`. Use this for encryption, compression, or custom encoding.
   *
   * The codec is global — it applies to all HotMesh and Durable instances
   * in the process. Pass `null` to remove a previously registered codec.
   *
   * **Constraints:** The codec must be synchronous and its output must be
   * a valid UTF-8 string. Use base64 encoding for binary output.
   *
   * @example
   * ```typescript
   * import { HotMesh } from '@hotmeshio/hotmesh';
   *
   * HotMesh.registerCodec({
   *   encode(json) { return Buffer.from(json).toString('base64'); },
   *   decode(encoded) { return Buffer.from(encoded, 'base64').toString('utf8'); },
   * });
   * ```
   */
  static registerCodec(codec: PayloadCodec | null): void {
    SerializerService.registerCodec(codec);
  }

  // ************* WORKER CREDENTIALS *************

  /**
   * Provision a scoped Postgres role for a worker. The role can only
   * dequeue, ack, and respond on its assigned stream names via stored
   * procedures — zero direct table access.
   */
  static provisionWorkerRole = WorkerCredentials.provisionWorkerRole;

  /** Rotate a secured worker role's password. */
  static rotateWorkerPassword = WorkerCredentials.rotateWorkerPassword;

  /** Revoke a secured worker role (disables login). */
  static revokeWorkerRole = WorkerCredentials.revokeWorkerRole;

  /** List all provisioned secured worker roles. */
  static listWorkerRoles = WorkerCredentials.listWorkerRoles;
}

export { HotMesh };
export type { PayloadCodec };
export type { WorkerCredential, WorkerCredentialInfo };
