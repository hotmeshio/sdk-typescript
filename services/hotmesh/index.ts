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
   * Initialize a HotMesh instance with an engine and optional workers.
   *
   * Workers are callback functions that consume messages from Postgres
   * streams. They can run on the same process as the engine or on
   * entirely separate servers/containers — the only coupling is the
   * shared Postgres database.
   *
   * @see Full JSDoc in prior versions for worker connection modes,
   *      secured workers, and retry policy configuration.
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

  async pub(
    topic: string,
    data: JobData = {},
    context?: JobState,
    extended?: ExtensionType,
  ): Promise<string> {
    return PubSub.pub(this, topic, data, context, extended);
  }

  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.sub(this, topic, callback);
  }

  async unsub(topic: string): Promise<void> {
    return PubSub.unsub(this, topic);
  }

  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return PubSub.psub(this, wild, callback);
  }

  async punsub(wild: string): Promise<void> {
    return PubSub.punsub(this, wild);
  }

  async pubsub(
    topic: string,
    data: JobData = {},
    context?: JobState | null,
    timeout?: number,
  ): Promise<JobOutput> {
    return PubSub.pubsub(this, topic, data, context, timeout);
  }

  async add(streamData: StreamData | StreamDataResponse): Promise<string> {
    return PubSub.add(this, streamData);
  }

  // ************* QUORUM METHODS *************

  async rollCall(delay?: number): Promise<QuorumProfile[]> {
    return Quorum.rollCall(this, delay);
  }

  async throttle(options: ThrottleOptions): Promise<boolean> {
    return Quorum.throttle(this, options);
  }

  async pubQuorum(quorumMessage: QuorumMessage) {
    return Quorum.pubQuorum(this, quorumMessage);
  }

  async subQuorum(callback: QuorumMessageCallback): Promise<void> {
    return Quorum.subQuorum(this, callback);
  }

  async unsubQuorum(callback: QuorumMessageCallback): Promise<void> {
    return Quorum.unsubQuorum(this, callback);
  }

  // ************* LIFECYCLE METHODS *************

  /**
   * @private
   */
  async plan(path: string): Promise<HotMeshManifest> {
    return Deployment.plan(this, path);
  }

  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return Deployment.deploy(this, pathOrYAML);
  }

  async activate(version: string, delay?: number): Promise<boolean> {
    return Deployment.activate(this, version, delay);
  }

  // ************* JOB METHODS *************

  async export(jobId: string, options: ExportOptions = {}): Promise<JobExport> {
    return Jobs.exportJob(this, jobId, options);
  }

  async getRaw(jobId: string): Promise<StringStringType> {
    return Jobs.getRaw(this, jobId);
  }

  /**
   * @private
   */
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return Jobs.getStats(this, topic, query);
  }

  async getStatus(jobId: string): Promise<JobStatus> {
    return Jobs.getStatus(this, jobId);
  }

  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return Jobs.getState(this, topic, jobId);
  }

  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return Jobs.getQueryState(this, jobId, fields);
  }

  /**
   * @private
   */
  async getIds(
    topic: string,
    query: JobStatsInput,
    queryFacets = [],
  ): Promise<IdsResponse> {
    return Jobs.getIds(this, topic, query, queryFacets);
  }

  /**
   * @private
   */
  async resolveQuery(
    topic: string,
    query: JobStatsInput,
  ): Promise<GetStatsOptions> {
    return Jobs.resolveQuery(this, topic, query);
  }

  async interrupt(
    topic: string,
    jobId: string,
    options: JobInterruptOptions = {},
  ): Promise<string> {
    return Jobs.interrupt(this, topic, jobId, options);
  }

  async cancel(jobId: string): Promise<void> {
    return Jobs.cancel(this, jobId);
  }

  async scrub(jobId: string) {
    return Jobs.scrub(this, jobId);
  }

  async signal(
    topic: string,
    data: JobData,
    status?: StreamStatus,
    code?: StreamCode,
  ): Promise<string> {
    return Jobs.signal(this, topic, data, status, code);
  }

  /**
   * @private
   */
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
   * Stops this specific HotMesh instance.
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

  static provisionWorkerRole = WorkerCredentials.provisionWorkerRole;
  static rotateWorkerPassword = WorkerCredentials.rotateWorkerPassword;
  static revokeWorkerRole = WorkerCredentials.revokeWorkerRole;
  static listWorkerRoles = WorkerCredentials.listWorkerRoles;
}

export { HotMesh };
export type { PayloadCodec };
export type { WorkerCredential, WorkerCredentialInfo };
