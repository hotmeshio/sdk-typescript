import {
  HMSH_LOGLEVEL,
  HMSH_EXPIRE_JOB_SECONDS,
  HMSH_QUORUM_DELAY_MS,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_DURABLE_MAX_INTERVAL,
} from '../../modules/enums';
import { hashOptions, s, sleepFor } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import {
  ClientConfig,
  ClientWorkflow,
  Connection,
  HookOptions,
  WorkflowOptions,
} from '../../types/durable';
import { JobState } from '../../types/job';
import { KeyType } from '../../modules/key';
import { StreamStatus, StringAnyType } from '../../types';
import {
  EscalationEntry,
  ClaimEscalationResult,
  ClaimByMetadataResult,
  ReleaseEscalationResult,
  ResolveEscalationResult,
  CancelEscalationResult,
  ListEscalationsParams,
  CreateEscalationParams,
  UpdateEscalationParams,
  AppendMilestonesParams,
  ClaimEscalationParams,
  ClaimByMetadataParams,
  ReleaseEscalationParams,
  ResolveEscalationParams,
  ResolveByMetadataParams,
  EscalateToRoleParams,
  MigrateEscalationParams,
} from '../../types/hmsh_escalations';

import { Search } from './search';
import { Entity } from './entity';
import { WorkflowHandleService } from './handle';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';

/**
 * Workflow client. Starts workflows, sends signals, and reads results.
 *
 * @example
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const client = new Durable.Client({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *   },
 * });
 *
 * // Start a workflow and await its result
 * const handle = await client.workflow.start({
 *   args: ['order-123'],
 *   taskQueue: 'orders',
 *   workflowName: 'orderWorkflow',
 *   workflowId: Durable.guid(),
 * });
 * const result = await handle.result();
 *
 * // Send a signal to a running workflow
 * await handle.signal('approval', { approved: true });
 *
 * // Cancel a running workflow
 * await handle.cancel();
 * ```
 */
export class ClientService {
  /**
   * @private
   */
  connection: Connection;
  /**
   * @private
   */
  options: WorkflowOptions;
  /**
   * @private
   */
  static topics: string[] = [];
  /**
   * @private
   */
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  /**
   * @private
   */
  constructor(config: ClientConfig) {
    this.connection = config.connection;
  }

  /**
   * @private
   */
  getHotMeshClient = async (taskQueue: string | null, namespace?: string) => {
    //namespace isolation requires the connection options to be hashed
    //as multiple intersecting databases can be used by the same service
    //hashing options allows for reuse of the same connection without risk of
    //overwriting data in another namespace.
    const optionsHash = this.hashOptions();
    const targetNS = namespace ?? APP_ID;
    const connectionNS = `${optionsHash}.${targetNS}`;
    if (ClientService.instances.has(connectionNS)) {
      const hotMeshClient = await ClientService.instances.get(connectionNS);
      await this.verifyWorkflowActive(hotMeshClient, targetNS);
      return hotMeshClient;
    }

    //init, but don't await
    const readonly = this.connection.readonly ?? undefined;
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      taskQueue,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        readonly,
        connection: this.connection,
      },
    });

    //synchronously cache the promise (before awaiting)
    ClientService.instances.set(connectionNS, hotMeshClient);

    //resolve, activate, and return the client
    const resolvedClient = await hotMeshClient;
    if (!readonly) {
      resolvedClient.engine.logger.info('durable-readonly-client', {
        guid: resolvedClient.engine.guid,
        appId: targetNS,
      });
      await this.activateWorkflow(resolvedClient, targetNS);
    }
    return resolvedClient;
  };

  /**
   * Creates a stream where messages can be published to ensure there is a
   * channel in place when the message arrives (a race condition for those
   * platforms without implicit topic setup).
   * @private
   */
  static createStream = async (
    hotMeshClient: HotMesh,
    workflowTopic: string,
    namespace?: string,
  ) => {
    const params = { appId: namespace ?? APP_ID, topic: workflowTopic };
    const streamKey = hotMeshClient.engine.store.mintKey(
      KeyType.STREAMS,
      params,
    );
    try {
      await hotMeshClient.engine.stream.createConsumerGroup(
        streamKey,
        'WORKER',
      );
    } catch (err) {
      //ignore if already exists
    }
  };

  hashOptions(): string {
    if ('options' in this.connection) {
      //shorthand format
      return hashOptions(this.connection.options);
    } else {
      //longhand format (sub, store, stream, pub, search)
      const response = [];
      for (const p in this.connection) {
        if (this.connection[p].options) {
          response.push(hashOptions(this.connection[p].options));
        }
      }
      return response.join('');
    }
  }

  /**
   * It is possible for a client to invoke a workflow without first
   * creating the stream. This method will verify that the stream
   * exists and if not, create it.
   * @private
   */
  verifyStream = async (
    hotMeshClient: HotMesh,
    workflowTopic: string,
    namespace?: string,
  ) => {
    const optionsHash = this.hashOptions();
    const targetNS = namespace ?? APP_ID;
    const targetTopic = `${optionsHash}.${targetNS}.${workflowTopic}`;
    if (!ClientService.topics.includes(targetTopic)) {
      ClientService.topics.push(targetTopic);
      await ClientService.createStream(hotMeshClient, workflowTopic, namespace);
    }
  };

  /**
   * @private
   */
  search = async (
    hotMeshClient: HotMesh,
    index: string,
    query: string[],
  ): Promise<string[]> => {
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.sendIndexedQuery(index, query);
  };

  /**
   * The Durable `Client` service provides methods for
   * starting, signaling, and querying workflows.
   * Starting a workflow is the primary use case and
   * is accessed by calling workflow.start().
   */
  workflow: ClientWorkflow = {
    /**
     * Starts a workflow, verifies the idempotent id, and
     * adds searchable data to the record.
     */
    start: async (options: WorkflowOptions): Promise<WorkflowHandleService> => {
      const taskQueueName = options.taskQueue ?? options.entity;
      const workflowName = options.taskQueue
        ? options.workflowName
        : options.entity ?? options.workflowName;
      const trc = options.workflowTrace;
      const spn = options.workflowSpan;
      //hotmesh `topic` is equivalent to `queue+workflowname` pattern in other systems
      const workflowTopic = `${taskQueueName}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(
        taskQueueName,
        options.namespace,
      );
      //verify that the stream channel exists before enqueueing
      await this.verifyStream(hotMeshClient, workflowTopic, options.namespace);
      const payload = {
        arguments: [...options.args],
        originJobId: options.originJobId,
        expire: options.expire ?? HMSH_EXPIRE_JOB_SECONDS,
        persistent: options.persistent,
        signalIn: options.signalIn,
        parentWorkflowId: options.parentWorkflowId,
        workflowId: options.workflowId || HotMesh.guid(),
        workflowTopic: workflowTopic,
        taskQueue: taskQueueName,
        workflowName: workflowName,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL,
        ),
      };

      const context = { metadata: { trc, spn }, data: {} };
      const jobId = await hotMeshClient.pub(
        `${options.namespace ?? APP_ID}.execute`,
        payload,
        context as JobState,
        {
          search: options?.search?.data,
          marker: options?.marker,
          pending: options?.pending,
          entity: options?.entity,
        },
      );
      return new WorkflowHandleService(hotMeshClient, workflowTopic, jobId);
    },

    /**
     * Sends a message payload to a running workflow that is paused and awaiting the signal.
     *
     * If the signal arrives before the workflow has registered its hook
     * (race condition under load), it is buffered as a pending signal
     * for up to `expire` (default 10 minutes). Use a longer duration
     * when signaling "early on purpose" (e.g., depositing a payload
     * hours before the workflow starts).
     */
    signal: async (
      signalId: string,
      data: StringAnyType,
      namespace?: string,
      expire?: string,
    ): Promise<string> => {
      const ns = namespace ?? APP_ID;
      const payload = {
        id: signalId,
        data,
        ...(expire ? { $expire: expire } : {}),
      };
      //send collator topic first (creates pending if no collator),
      //then inline waiter topic (delivers and cleans up collator pending)
      try {
        const signalTopic = `${ns}.wfs.signal`;
        await (
          await this.getHotMeshClient(signalTopic, namespace)
        ).signal(signalTopic, payload);
      } catch {
        //no hook rule — ignore
      }
      try {
        const waitTopic = `${ns}.wfs.wait`;
        return await (
          await this.getHotMeshClient(waitTopic, namespace)
        ).signal(waitTopic, payload);
      } catch {
        //no hook rule — ignore
      }
    },

    /**
     * Spawns an a new, isolated execution cycle within the same job.
     * Similar to `worker` functions, `hook` functions have a linked
     * function. But hooks do not start a new job and instead read/write
     * their isolated activity data to an existing Job record (HASH).
     *
     * This example spawns a hook that will update workflow `guid123`.
     *
     * @example
     * ```typescript
     * await client.workflow.hook({
     *   namespace: 'demo',
     *   taskQueue: 'default',
     *   workflowName: 'myDemoFunction',
     *   workflowId: 'guid123',
     *   args: ['Hello'],
     * });
     * ```
     */
    hook: async (options: HookOptions): Promise<string> => {
      const taskQueue = options.taskQueue ?? options.entity;
      const hookWorkflowName = options.entity ?? options.workflowName;
      const workflowTopic = `${taskQueue}-${hookWorkflowName}`;
      const payload = {
        arguments: [...options.args],
        id: options.workflowId,
        workflowTopic,
        taskQueue,
        workflowName: hookWorkflowName,
        backoffCoefficient:
          options.config?.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
        maximumAttempts:
          options.config?.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
        maximumInterval: s(
          options.config?.maximumInterval || HMSH_DURABLE_MAX_INTERVAL,
        ),
      };
      //seed search data before entering
      const hotMeshClient = await this.getHotMeshClient(
        taskQueue,
        options.namespace,
      );
      const msgId = await hotMeshClient.signal(
        `${hotMeshClient.appId}.flow.signal`,
        payload,
        StreamStatus.PENDING,
        202,
      );
      //todo: commit search data BEFORE enqueuing hook
      if (options.search?.data) {
        const searchSessionId = `-search-${HotMesh.guid()}-0`;
        const search = new Search(
          options.workflowId,
          hotMeshClient,
          searchSessionId,
        );
        const entries = Object.entries(options.search.data).flat();
        await search.set(...entries);
      }
      //todo: support context as well as search (-context-)
      return msgId;
    },

    /**
     * Returns a reference to a running workflow,
     * allowing callers to check the status of the workflow,
     * interrupt it, and even await its eventual response
     * if still in a pending state.
     *
     * @example
     * ```typescript
     * const handle = await client.workflow.getHandle(
     *  'default',
     *  'myFunction',
     *  'someGuid123',
     *  'demo',
     * );
     * ```
     */
    getHandle: async (
      taskQueue: string,
      workflowName: string,
      workflowId: string,
      namespace?: string,
    ): Promise<WorkflowHandleService> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(taskQueue, namespace);
      return new WorkflowHandleService(
        hotMeshClient,
        workflowTopic,
        workflowId,
      );
    },

    /**
     * Provides direct access to the SEARCH backend
     *
     * @example
     * ```typescript
     * await client.workflow.search(
     *   'someTaskQueue'
     *   'someWorkflowName',
     *   'durable',
     *   'user',
     *   ...args,
     * );
     * //returns [count, [id, fields[]], [id, fields[]], [id, fields[]], ...]]
     * ```
     */
    search: async (
      taskQueue: string,
      workflowName: string,
      namespace: null | string,
      index: string,
      ...query: string[]
    ): Promise<string[]> => {
      const workflowTopic = `${taskQueue}-${workflowName}`;
      const hotMeshClient = await this.getHotMeshClient(taskQueue, namespace);
      try {
        return await this.search(hotMeshClient, index, query);
      } catch (error) {
        hotMeshClient.engine.logger.error('durable-client-search-err', {
          error,
        });
        throw error;
      }
    },
  };

  /**
   * Any router can be used to deploy and activate the HotMesh
   * distributed executable to the active quorum EXCEPT for
   * those routers in `readonly` mode.
   */
  async deployAndActivate(
    namespace = APP_ID,
    version = APP_VERSION,
  ): Promise<void> {
    if (isNaN(Number(version))) {
      throw new Error('Invalid version number');
    }
    const hotMesh = await this.getHotMeshClient('', namespace);
    await this.activateWorkflow(hotMesh, namespace, version);
  }

  /**
   * @private
   */
  async verifyWorkflowActive(
    hotMesh: HotMesh,
    appId = APP_ID,
    count = 0,
  ): Promise<boolean> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if (isNaN(appVersion)) {
      if (count > 10) {
        throw new Error('Workflow failed to activate');
      }
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await this.verifyWorkflowActive(hotMesh, appId, count + 1);
    }
    return true;
  }

  /**
   * @private
   */
  async activateWorkflow(
    hotMesh: HotMesh,
    appId = APP_ID,
    version = APP_VERSION,
  ): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as string;
    if (appVersion === version && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', {
          error,
        });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', {
          error,
        });
        throw error;
      }
    }
  }

  /**
   * Escalation queue operations over `public.hmsh_escalations` — a global
   * table that surfaces workflow signal pauses as role-based, claimable,
   * searchable queue items.
   *
   * When a YAML `hook` activity suspends with an `escalation:` block, or
   * `Durable.workflow.condition(signalId, config)` fires, **one row is
   * written atomically** with the workflow checkpoint — no enrichment step,
   * no secondary round-trip. Every connected app shares the same table;
   * rows are namespaced by `namespace` + `app_id`.
   *
   * **Status lifecycle:**
   * ```
   * pending → claimed → resolved
   *         ↘ cancelled      (any non-terminal state)
   *         ↗ pending        (via release or releaseExpired)
   * ```
   *
   * **Typical human-in-the-loop flow:**
   * ```typescript
   * // 1. Workflow pauses and writes the escalation row automatically
   * const decision = await Durable.workflow.condition('manager-approval', {
   *   role: 'manager',
   *   type: 'order-approval',
   *   priority: 2,
   *   metadata: { orderId },
   * });
   *
   * // 2. Dashboard lists pending approvals for this role
   * const [item] = await client.escalations.list({ role: 'manager', status: 'pending' });
   *
   * // 3. Reviewer claims it (sets assigned_to + expiry)
   * await client.escalations.claim({ id: item.id, assignee: 'alice@company.com' });
   *
   * // 4. Resolve atomically marks it resolved AND delivers the signal
   * await client.escalations.resolve({
   *   id: item.id,
   *   resolverPayload: { approved: true },
   * });
   * // workflow resumes with { approved: true }
   * ```
   */
  escalations = {
    /**
     * Returns all escalation rows matching the given filters.
     *
     * @example
     * ```typescript
     * // All pending approvals for the manager role
     * const items = await client.escalations.list({ role: 'manager', status: 'pending' });
     *
     * // By workflow ID
     * const items = await client.escalations.list({ workflowId: 'order-123' });
     * ```
     */
    list: async (params?: ListEscalationsParams): Promise<EscalationEntry[]> => {
      const hotMeshClient = await this.getHotMeshClient(null, params?.namespace);
      return (hotMeshClient.engine.store as any).listEscalations(params ?? {});
    },

    /**
     * Returns a single escalation row by its UUID primary key.
     * Returns `null` if not found.
     */
    get: async (id: string, namespace?: string): Promise<EscalationEntry | null> => {
      const hotMeshClient = await this.getHotMeshClient(null, namespace);
      return (hotMeshClient.engine.store as any).getEscalation(id, namespace);
    },

    /**
     * Looks up an escalation row by its `signal_key` — the value that was
     * passed to `condition()` or stored in the hook activity's collation rule.
     * This is the same key used to deliver the signal via `hotMesh.signal()`.
     *
     * @example
     * ```typescript
     * const item = await client.escalations.getBySignalKey('manager-approval');
     * ```
     */
    getBySignalKey: async (signalKey: string, namespace?: string): Promise<EscalationEntry | null> => {
      const hotMeshClient = await this.getHotMeshClient(null, namespace);
      return (hotMeshClient.engine.store as any).getEscalationBySignalKey(signalKey, namespace);
    },

    /**
     * Creates a standalone escalation row that is **not** backed by a signal.
     * `signal_key` is `null`. Useful for external task tracking that doesn't
     * need to resume a workflow (e.g., audit tasks, out-of-band approvals).
     *
     * @example
     * ```typescript
     * const entry = await client.escalations.create({
     *   role: 'support',
     *   type: 'data-correction',
     *   description: 'Fix the customer address',
     *   metadata: { customerId: 'cust-42' },
     * });
     * ```
     */
    create: async (params: CreateEscalationParams): Promise<EscalationEntry> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).createEscalation(params);
    },

    /**
     * Patches an existing escalation row. All fields are optional — only
     * provided fields are written. `metadata` is **merged**, not replaced.
     *
     * Signal routing fields (`signalKey`, `topic`, `workflowId`, …) can be
     * enriched after the row is created — useful when the row is created
     * before the workflow starts and routing context is not yet known.
     *
     * @example
     * ```typescript
     * await client.escalations.update({
     *   id: item.id,
     *   description: 'Updated description',
     *   metadata: { extraKey: 'value' },  // merged into existing metadata
     * });
     * ```
     */
    update: async (params: UpdateEscalationParams): Promise<EscalationEntry | null> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).updateEscalation(params);
    },

    /**
     * Appends one or more milestone entries to the escalation's
     * `milestones` audit trail array. Milestones are append-only; they
     * record events like state transitions, reviewer notes, or external
     * system callbacks.
     *
     * @example
     * ```typescript
     * await client.escalations.appendMilestones({
     *   id: item.id,
     *   milestones: [{ at: new Date().toISOString(), by: 'alice', note: 'Reviewed' }],
     * });
     * ```
     */
    appendMilestones: async (params: AppendMilestonesParams): Promise<EscalationEntry | null> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).appendEscalationMilestones(params);
    },

    /**
     * Atomically claims an escalation row by UUID. Sets `assigned_to`,
     * `claimed_at`, and `claim_expires_at`. Returns `conflict` if another
     * actor already holds the claim.
     *
     * @example
     * ```typescript
     * const result = await client.escalations.claim({
     *   id: item.id,
     *   assignee: 'alice@company.com',
     *   durationMinutes: 30,
     * });
     * if (!result.ok) console.warn('Already claimed by someone else');
     * ```
     */
    claim: async (params: ClaimEscalationParams): Promise<ClaimEscalationResult> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).claimEscalation(params);
    },

    /**
     * Atomically claims the highest-priority pending escalation whose
     * `metadata` contains the given key/value pair. Uses
     * `FOR UPDATE SKIP LOCKED` so concurrent callers never double-claim.
     *
     * Returns `candidatesExist` to distinguish two cases:
     * - `not-found, candidatesExist: 0` — no rows matched the metadata filter
     * - `conflict, candidatesExist: N` — matching rows exist but all are claimed
     *
     * @example
     * ```typescript
     * const result = await client.escalations.claimByMetadata({
     *   key: 'region',
     *   value: 'west',
     *   assignee: 'bob@company.com',
     *   roles: ['manager'],
     * });
     * if (result.ok) console.log('Claimed:', result.entry.id);
     * ```
     */
    claimByMetadata: async (params: ClaimByMetadataParams): Promise<ClaimByMetadataResult> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).claimEscalationByMetadata(params);
    },

    /**
     * Releases a claimed escalation, returning it to `pending` status and
     * clearing `assigned_to` and `claim_expires_at`. The row is immediately
     * available for other actors to claim.
     *
     * @example
     * ```typescript
     * await client.escalations.release({ id: item.id });
     * ```
     */
    release: async (params: ReleaseEscalationParams): Promise<ReleaseEscalationResult> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).releaseEscalation(params);
    },

    /**
     * Reassigns the escalation to a different role, clearing any current
     * claim and returning status to `pending`. Use when an escalation must
     * be handled by a different team or tier.
     *
     * @example
     * ```typescript
     * await client.escalations.escalateToRole({ id: item.id, role: 'senior-manager' });
     * ```
     */
    escalateToRole: async (params: EscalateToRoleParams): Promise<EscalationEntry | null> => {
      const hotMeshClient = await this.getHotMeshClient(null, params.namespace);
      return (hotMeshClient.engine.store as any).escalateEscalationToRole(params);
    },

    /**
     * Terminates the escalation without delivering a signal. Rows in
     * `pending` or `claimed` state move to `cancelled`. Terminal rows
     * (`resolved`, `cancelled`) return `already-terminal`.
     *
     * @example
     * ```typescript
     * await client.escalations.cancel(item.id);
     * ```
     */
    cancel: async (id: string, namespace?: string): Promise<CancelEscalationResult> => {
      const hotMeshClient = await this.getHotMeshClient(null, namespace);
      return (hotMeshClient.engine.store as any).cancelEscalation(id, namespace);
    },

    /**
     * Atomically marks the escalation `resolved` **and** delivers the
     * signal to the waiting workflow — one round-trip, no separate
     * `signal()` call required. If `signal_key` is null (standalone
     * escalation), only the row is updated.
     *
     * @example
     * ```typescript
     * const result = await client.escalations.resolve({
     *   id: item.id,
     *   resolverPayload: { approved: true, note: 'LGTM' },
     * });
     * if (!result.ok) console.error(result.reason); // 'not-found' | 'already-resolved' | 'signal-failed'
     * // workflow resumes with { approved: true, note: 'LGTM' }
     * ```
     */
    resolve: async (
      params: ResolveEscalationParams,
      namespace?: string,
    ): Promise<ResolveEscalationResult> => {
      const ns = (params.namespace ?? namespace) ?? APP_ID;
      const hotMeshClient = await this.getHotMeshClient(null, ns);
      const store = hotMeshClient.engine.store as any;

      // store.resolveEscalation() uses FOR UPDATE inside its CTE, serializing concurrent
      // callers at the DB level. The second caller blocks on the row lock, reads
      // 'already-resolved' after the first commits, and returns — no signal is queued.
      // This eliminates the double-signal race that the previous KVTransaction approach
      // had: two concurrent callers could both pass the unlocked getEscalation() read,
      // both queue signal INSERTs, and both commit them — even though only one UPDATE won.
      const dbResult = await store.resolveEscalation({
        id: params.id,
        resolverPayload: params.resolverPayload,
        // namespace intentionally omitted — UUID lookup; passing ns would miss rows
        // stored under namespace 'hmsh' (the engine default) when ns = APP_ID = 'durable'.
      });
      if (!dbResult.ok) return dbResult;

      if (dbResult.signalKey) {
        const signalPayload = { id: dbResult.signalKey, data: params.resolverPayload ?? {} };
        const delivered = await this._deliverEscalationSignal(ns, dbResult.topic, signalPayload);
        if (!delivered) return { ok: false, reason: 'signal-failed' };
      }
      return { ok: true };
    },

    /**
     * Resolves the highest-priority matching escalation by metadata filter,
     * then delivers its signal. Identical semantics to `resolve()` but
     * selects the target row by metadata key/value instead of UUID.
     *
     * @example
     * ```typescript
     * await client.escalations.resolveByMetadata({
     *   key: 'orderId',
     *   value: 'order-123',
     *   resolverPayload: { approved: true },
     * });
     * ```
     */
    resolveByMetadata: async (
      params: ResolveByMetadataParams,
      namespace?: string,
    ): Promise<ResolveEscalationResult> => {
      const ns = (params.namespace ?? namespace) ?? APP_ID;
      const hotMeshClient = await this.getHotMeshClient(null, ns);
      const store = hotMeshClient.engine.store as any;

      // Same FOR UPDATE CTE serialization as resolve(). Metadata filter selects
      // the highest-priority matching row; the lock prevents concurrent callers
      // from both resolving it.
      const dbResult = await store.resolveEscalationByMetadata({
        key: params.key,
        value: params.value,
        resolverPayload: params.resolverPayload,
        roles: params.roles,
      });
      if (!dbResult.ok) return dbResult;

      if (dbResult.signalKey) {
        const signalPayload = { id: dbResult.signalKey, data: params.resolverPayload ?? {} };
        const delivered = await this._deliverEscalationSignal(ns, dbResult.topic, signalPayload);
        if (!delivered) return { ok: false, reason: 'signal-failed' };
      }
      return { ok: true };
    },

    /**
     * Full-fidelity migration: inserts an escalation row preserving the original
     * UUID and all lifecycle state. Returns the inserted row, or `null` if the
     * UUID already exists (idempotent — safe to call multiple times with the same
     * `params.id`). Use this to migrate rows from a legacy escalation table to
     * `hmsh_escalations` without losing original IDs or state.
     *
     * @example
     * ```typescript
     * const entry = await client.escalations.migrate({
     *   id: 'original-uuid',
     *   status: 'resolved',
     *   resolvedAt: new Date('2025-01-01'),
     *   type: 'order-approval',
     *   role: 'approver',
     * });
     * // null on subsequent calls with the same id — idempotent
     * ```
     */
    migrate: async (
      params: MigrateEscalationParams,
      namespace?: string,
    ): Promise<EscalationEntry | null> => {
      const ns = (params.namespace ?? namespace) ?? APP_ID;
      const hotMeshClient = await this.getHotMeshClient(null, ns);
      return (hotMeshClient.engine.store as any).createEscalationForMigration(params);
    },

    /**
     * Releases all claimed escalations whose `claim_expires_at` has lapsed,
     * returning them to `pending` so they can be claimed again. Returns the
     * number of rows released. Call periodically from a maintenance job or
     * cron to prevent stale claims from blocking the queue.
     *
     * @example
     * ```typescript
     * const released = await client.escalations.releaseExpired();
     * console.log(`Released ${released} expired claims`);
     * ```
     */
    releaseExpired: async (namespace?: string): Promise<number> => {
      const hotMeshClient = await this.getHotMeshClient(null, namespace);
      return (hotMeshClient.engine.store as any).releaseExpiredEscalations(namespace);
    },
  };

  /**
   * Delivers a signal to the registered escalation topic.
   *
   * When the topic is known (stored at condition() time), we deliver only to that
   * topic. This avoids writing a stale pending entry to the alternate stream, which
   * would interfere with concurrent consumers in other workflows or tests.
   *
   * When topic is null (legacy/standalone rows with no registered topic), we try
   * both wfs.signal and wfs.wait unconditionally — engine.signal() on the wrong
   * topic stores pending without throwing, so a sequential fallback would skip
   * the second topic and leave single-condition workflows permanently suspended.
   *
   * @private
   */
  private async _deliverEscalationSignal(
    ns: string,
    topic: string | null | undefined,
    signalPayload: { id: string; data: Record<string, unknown> },
  ): Promise<boolean> {
    if (topic) {
      // Registered topic known — deliver precisely, no stream pollution.
      try {
        const tc = await this.getHotMeshClient(topic, ns);
        await tc.engine.signal(topic, signalPayload, StreamStatus.SUCCESS, 200);
        return true;
      } catch { /* topic not currently registered — fall through */ }
    }

    // Topic unknown or primary delivery failed — try both topics unconditionally.
    let delivered = false;
    try {
      const sc = await this.getHotMeshClient(`${ns}.wfs.signal`, ns);
      await sc.engine.signal(`${ns}.wfs.signal`, signalPayload, StreamStatus.SUCCESS, 200);
      delivered = true;
    } catch { /* no collator hook rule for this workflow */ }
    try {
      const wc = await this.getHotMeshClient(`${ns}.wfs.wait`, ns);
      await wc.engine.signal(`${ns}.wfs.wait`, signalPayload, StreamStatus.SUCCESS, 200);
      delivered = true;
    } catch { /* no waiter hook rule for this workflow */ }
    return delivered;
  }

  /**
   * @private
   */
  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of ClientService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
