import {
  HMSH_LOGLEVEL,
} from '../../modules/enums';
import { formatISODate, guid, hashOptions } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import { EventsConfig, SystemEvent, EscalationVerb } from '../../types/system_events';
import { Connection } from '../../types/durable';
import { StreamStatus } from '../../types';
import { StreamDataType } from '../../types/stream';
import {
  EscalationWakeCommand,
  EscalationEntry,
  ClaimEscalationResult,
  ClaimByMetadataResult,
  ReleaseEscalationResult,
  ResolveEscalationResult,
  CancelEscalationResult,
  ListEscalationsParams,
  StatsEscalationsParams,
  EscalationStats,
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
  ClaimManyParams,
  EscalateManyToRoleParams,
  UpdateManyPriorityParams,
  ResolveManyParams,
  PruneEscalationsParams,
  PruneEscalationsResult,
} from '../../types/hmsh_escalations';
import { APP_ID } from '../durable/schemas/factory';

export type GetHotMeshFn = (topic: string | null, namespace?: string) => Promise<HotMesh>;

export interface EscalationClientConfig {
  /** Postgres connection options — used when creating a standalone EscalationClient. */
  connection?: Connection;
  /**
   * Inject a pre-existing `getHotMeshClient` function (e.g. from Durable.Client).
   * When provided, the client reuses the caller's engine pool — no extra connections.
   */
  getHotMeshClient?: GetHotMeshFn;
  /**
   * Optional system-event sink. When set, this client calls `events.publish`
   * post-commit for every escalation lifecycle transition it performs.
   * Fire-and-forget; a publish error never fails the committed operation.
   */
  events?: EventsConfig;
}

/**
 * Standalone client for the `public.hmsh_escalations` signal-pause surface.
 *
 * Requires NO dependency on `services/durable/`. Any HotMesh consumer — AI
 * agent, YAML DAG worker, REST API — can interact with the escalation queue
 * directly with just a Postgres connection.
 *
 * Signal delivery (for `resolve()` / `resolveByMetadata()`) uses HotMesh's
 * `engine.signal()` internally. The engine is initialised lazily on first use
 * and cached for the lifetime of the process.
 *
 * @example
 * ```typescript
 * import { Escalations } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const client = new Escalations.Client({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *   },
 * });
 *
 * // Claim the next available approval for the 'manager' role
 * const result = await client.claimByMetadata({
 *   key: 'orderId', value: 'order-123',
 *   assignee: 'alice@company.com',
 *   roles: ['manager'],
 * });
 *
 * if (result.ok) {
 *   await client.resolve({ id: result.entry.id, resolverPayload: { approved: true } });
 * }
 * ```
 */
export class EscalationClientService {
  private readonly _engine: GetHotMeshFn;
  private readonly _events?: EventsConfig;

  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: EscalationClientConfig = {}) {
    if (config.getHotMeshClient) {
      this._engine = config.getHotMeshClient;
    } else if (config.connection) {
      this._engine = this._makeEngineFactory(config.connection);
    } else {
      throw new Error('EscalationClient requires either `connection` or `getHotMeshClient`');
    }
    this._events = config.events;
  }

  /**
   * Fires the configured `events.publish` hook post-commit.
   * Detached (not awaited); a publish error never fails the caller.
   * @private
   */
  private _emit(verb: EscalationVerb, entry: EscalationEntry): void {
    if (!this._events?.publish) return;
    const ts = new Date().toISOString();
    const updatedAt = entry.updated_at
      ? formatISODate(entry.updated_at)
      : ts;
    const event: SystemEvent = {
      event_id: `${entry.id}:${verb}:${updatedAt}`,
      type: `system.escalation.${entry.id}.${verb}`,
      ts,
      namespace: entry.namespace ?? '',
      app_id: entry.app_id ?? '',
      workflow_id: entry.workflow_id ?? undefined,
      topic: entry.topic ?? undefined,
      origin_id: entry.origin_id ?? undefined,
      parent_id: entry.parent_id ?? undefined,
      trace_id: entry.trace_id ?? undefined,
      span_id: entry.span_id ?? undefined,
      data: entry as unknown as Record<string, unknown>,
    };
    void Promise.resolve(this._events.publish(event)).catch(() => { /* best-effort */ });
  }

  /**
   * Fires per-row events for bulk operations. Skipped rows are not emitted.
   * @private
   */
  private _emitMany(verb: EscalationVerb, entries: EscalationEntry[]): void {
    for (const entry of entries) this._emit(verb, entry);
  }

  private _makeEngineFactory(connection: Connection): GetHotMeshFn {
    return async (topic: string | null, namespace?: string): Promise<HotMesh> => {
      const optionsHash = this._hashConnection(connection);
      const targetNS = namespace ?? APP_ID;
      const key = `esc:${optionsHash}.${targetNS}`;
      if (EscalationClientService.instances.has(key)) {
        return await EscalationClientService.instances.get(key)!;
      }
      const pending = HotMesh.init({
        appId: targetNS,
        taskQueue: topic ?? undefined,
        logLevel: HMSH_LOGLEVEL,
        engine: { connection },
      });
      EscalationClientService.instances.set(key, pending);
      return await pending;
    };
  }

  private _hashConnection(connection: Connection): string {
    if ('options' in connection) {
      return hashOptions(connection.options);
    }
    const parts: string[] = [];
    for (const p in connection) {
      if ((connection as any)[p]?.options) {
        parts.push(hashOptions((connection as any)[p].options));
      }
    }
    return parts.join('');
  }

  // ─── Signal delivery ───────────────────────────────────────────────────────

  private async _deliverEscalationSignal(
    ns: string,
    topic: string | null | undefined,
    signalPayload: { id: string; data: Record<string, unknown> },
  ): Promise<boolean> {
    if (topic) {
      try {
        const tc = await this._engine(topic, ns);
        await tc.engine.signal(topic, signalPayload, StreamStatus.SUCCESS, 200);
        return true;
      } catch { /* topic not currently registered — fall through */ }
    }
    let delivered = false;
    try {
      const sc = await this._engine(`${ns}.wfs.signal`, ns);
      await sc.engine.signal(`${ns}.wfs.signal`, signalPayload, StreamStatus.SUCCESS, 200);
      delivered = true;
    } catch { }
    try {
      const wc = await this._engine(`${ns}.wfs.wait`, ns);
      await wc.engine.signal(`${ns}.wfs.wait`, signalPayload, StreamStatus.SUCCESS, 200);
      delivered = true;
    } catch { }
    return delivered;
  }

  /**
   * Builds the wake as a webhook message so the store can commit it
   * INSIDE the resolve/cancel transaction — the wake becomes durable
   * with the status change, closing the crash window between commit and
   * post-commit signal delivery. Mirrors `_deliverEscalationSignal`'s
   * topic fallback chain; returns null when no hook rule is deployed
   * for any candidate topic (the caller then keeps post-commit
   * delivery as the only path, preserving prior behavior).
   */
  private async _buildWakeCommand(
    ns: string,
    topic: string | null | undefined,
    signalKey: string,
    data: Record<string, unknown>,
  ): Promise<EscalationWakeCommand | null> {
    const hm = await this._engine(null, ns);
    const engine = hm.engine as any;
    const candidates = [topic, `${ns}.wfs.signal`, `${ns}.wfs.wait`].filter(
      Boolean,
    ) as string[];
    for (const candidate of candidates) {
      try {
        const hookRule = await engine.taskService.getHookRule(candidate);
        if (!hookRule) continue;
        const [aid] = await engine.getSchema(`.${hookRule.to}`);
        const streamData = {
          type: StreamDataType.WEBHOOK,
          status: StreamStatus.SUCCESS,
          code: 200,
          metadata: { guid: guid(), aid, topic: candidate },
          data: { id: signalKey, data },
        };
        return {
          forSignalKey: signalKey,
          message: JSON.stringify(streamData),
        };
      } catch {
        /* candidate not deployed — try the next topic */
      }
    }
    return null;
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Returns all escalation rows matching the given filters. Each row includes
   * a computed `available` field (true = claimable). Supports `sortBy`,
   * `sortOrder`, `orderBy[]`, and multi-role `roles[]` filter.
   */
  async list(params?: ListEscalationsParams): Promise<EscalationEntry[]> {
    const hm = await this._engine(null, params?.namespace);
    return (hm.engine.store as any).listEscalations(params ?? {});
  }

  /**
   * Returns the count of escalation rows matching the given filters.
   * Uses the same filter parameters as `list()`.
   */
  async count(params?: ListEscalationsParams): Promise<number> {
    const hm = await this._engine(null, params?.namespace);
    return (hm.engine.store as any).countEscalations(params ?? {});
  }

  /**
   * Retention: deletes terminal escalation rows (`resolved`/`cancelled`/`expired`)
   * whose `updated_at` is older than the horizon (a Postgres interval string,
   * e.g. `'90 days'`). Terminal rows are inert — every engine state transition
   * guards on `status = 'pending'` — so pruning them is safe for live waiters,
   * claims, and signal delivery; this call is the engine-blessed way to age
   * out the audit backlog. Each call deletes at most `limit` rows (default
   * 10,000) in one atomic statement; loop until `deleted` is 0 to drain a
   * large backlog. `list()`/`get()`/`stats()` reads over windows older than
   * the pruning horizon reflect only the rows retained.
   */
  async prune(params: PruneEscalationsParams): Promise<PruneEscalationsResult> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).pruneEscalations(params);
  }

  /** Returns a single escalation row by UUID. Returns `null` if not found. */
  async get(id: string, namespace?: string): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, namespace);
    return (hm.engine.store as any).getEscalation(id, namespace);
  }

  /** Looks up an escalation by `signal_key` — the value passed to `condition()`. */
  async getBySignalKey(signalKey: string, namespace?: string): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, namespace);
    return (hm.engine.store as any).getEscalationBySignalKey(signalKey, namespace);
  }

  /**
   * Creates a standalone escalation row with `signal_key = null`.
   * Useful for external task tracking that doesn't need to resume a workflow.
   */
  async create(params: CreateEscalationParams): Promise<EscalationEntry> {
    const hm = await this._engine(null, params.namespace);
    const entry = await (hm.engine.store as any).createEscalation(params);
    if (entry) this._emit('created', entry);
    return entry;
  }

  /**
   * Patches an existing escalation row. `metadata` is merged, not replaced.
   * Signal routing fields can be enriched after creation.
   */
  async update(params: UpdateEscalationParams): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).updateEscalation(params);
  }

  /** Appends milestone entries to the escalation's audit trail. */
  async appendMilestones(params: AppendMilestonesParams): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).appendEscalationMilestones(params);
  }

  /**
   * Atomically claims an escalation by UUID. Implicit model: `status` stays
   * `'pending'`; claim is expressed via `assigned_to` + `assigned_until`.
   * Returns `isExtension: true` when the same assignee re-claims a row they already hold.
   */
  async claim(params: ClaimEscalationParams): Promise<ClaimEscalationResult> {
    const hm = await this._engine(null, params.namespace);
    const result = await (hm.engine.store as any).claimEscalation(params);
    if (result.ok === true) this._emit('claimed', result.entry);
    return result;
  }

  /**
   * Atomically claims the highest-priority pending escalation whose `metadata`
   * contains the given key/value. Optionally merges `metadata` into the claimed row
   * in the same atomic UPDATE. Returns `isExtension: true` when the same assignee
   * re-claims a row they already hold (extends the expiry).
   */
  async claimByMetadata(params: ClaimByMetadataParams): Promise<ClaimByMetadataResult> {
    const hm = await this._engine(null, params.namespace);
    const result = await (hm.engine.store as any).claimEscalationByMetadata(params);
    if (result.ok === true) this._emit('claimed', result.entry);
    return result;
  }

  /** Releases a claimed escalation, returning it to available status. */
  async release(params: ReleaseEscalationParams): Promise<ReleaseEscalationResult> {
    const hm = await this._engine(null, params.namespace);
    const result = await (hm.engine.store as any).releaseEscalation(params);
    if (result.ok === true) this._emit('released', result.entry);
    return result;
  }

  /**
   * Reassigns the escalation to a different role, clearing any current claim
   * and resetting status to `'pending'`.
   */
  async escalateToRole(params: EscalateToRoleParams): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, params.namespace);
    const entry = await (hm.engine.store as any).escalateEscalationToRole(params);
    if (entry) this._emit('reassigned', entry);
    return entry;
  }

  /**
   * Cancels a pending escalation and delivers a cancellation signal to the
   * waiting workflow so that `condition()` returns `null`. Terminal rows
   * return `already-terminal`. The cancellation wake commits inside the
   * cancel transaction (same durability contract as `resolve()`); when no
   * hook rule is deployed for any candidate topic, delivery falls back to
   * a best-effort post-commit publish.
   */
  async cancel(id: string, namespace?: string): Promise<CancelEscalationResult> {
    const ns = namespace ?? APP_ID;
    const hm = await this._engine(null, ns);
    const store = hm.engine.store as any;

    //pre-build the cancellation wake so it commits INSIDE the cancel
    //transaction — condition() resumes with null even if the process
    //dies the instant the cancel commits
    let wakeCommand: EscalationWakeCommand | null = null;
    const preview = await store.getEscalation(id, namespace);
    if (preview?.signal_key) {
      wakeCommand = await this._buildWakeCommand(
        ns,
        preview.topic,
        preview.signal_key,
        { __escalation_cancelled: true },
      );
    }

    const result = await store.cancelEscalation(id, namespace, wakeCommand ?? undefined);
    if (result.ok === true) {
      this._emit('cancelled', result.entry);
      if (result.entry.signal_key && !result.wakeEnqueued) {
        await this._deliverEscalationSignal(ns, result.entry.topic, {
          id: result.entry.signal_key,
          data: { __escalation_cancelled: true },
        });
      }
    }
    return result;
  }

  /**
   * Emits local `cancelled` events for a batch of already-cancelled escalation
   * entries. Called by `WorkflowHandleService.terminate()` after the single
   * atomic transaction that interrupts the workflow and cancels its escalations
   * has committed. Fire-and-forget via the configured `events.publish` sink
   * (e.g. NATS) — instance-local, never broadcast via Postgres LISTEN/NOTIFY.
   */
  emitCancelledBatch(entries: EscalationEntry[]): void {
    this._emitMany('cancelled', entries);
  }

  /**
   * Resolves a pending escalation by UUID. Uses an explicit Postgres transaction
   * with FOR UPDATE + WHERE guard: only one concurrent caller can commit the
   * status change; the committed resolved row with its `signal_key` is the
   * durable proof. Signal delivery is best-effort post-commit — the resolved
   * row is the recovery record for any missed delivery. Returns the updated
   * row as `entry` on success.
   *
   * Pass `params.metadata` to merge the resolution outcome ("what actually
   * happened") into the row's GIN-indexed `metadata` in the same atomic UPDATE,
   * making it `@>`-queryable alongside the creation metadata ("what was
   * intended"). This is distinct from `resolverPayload`, which is delivered to
   * the waiting workflow as `condition()`'s return value and is not GIN-indexed.
   */
  async resolve(
    params: ResolveEscalationParams,
    namespace?: string,
  ): Promise<ResolveEscalationResult> {
    const ns = (params.namespace ?? namespace) ?? APP_ID;
    const hm = await this._engine(null, ns);
    const store = hm.engine.store as any;

    //pre-build the wake so it commits INSIDE the resolve transaction; the
    //row's signal routing (signal_key, topic) is immutable after creation
    let wakeCommand: EscalationWakeCommand | null = null;
    const preview = await store.getEscalation(params.id, params.namespace);
    if (preview?.signal_key) {
      wakeCommand = await this._buildWakeCommand(
        ns,
        preview.topic,
        preview.signal_key,
        params.resolverPayload ?? {},
      );
    }

    const dbResult = await store.resolveEscalation(
      { id: params.id, resolverPayload: params.resolverPayload, metadata: params.metadata },
      wakeCommand ?? undefined,
    );
    if (!dbResult.ok) return dbResult;
    if (dbResult.signalKey && !dbResult.wakeEnqueued) {
      //the wake was not part of the commit (no hook rule found, or the
      //enqueue was rolled back to its savepoint) — deliver post-commit
      await this._deliverEscalationSignal(ns, dbResult.topic, {
        id: dbResult.signalKey,
        data: params.resolverPayload ?? {},
      });
    }
    this._emit('resolved', dbResult.entry);
    return { ok: true, entry: dbResult.entry };
  }

  /**
   * Resolves the highest-priority matching escalation by metadata filter,
   * then delivers its signal. Same transaction + WHERE guard semantics as `resolve()`.
   *
   * The `key`/`value` are the *selector* used to find the row; pass
   * `params.metadata` to additionally merge a resolution patch into that row's
   * GIN-indexed `metadata` in the same atomic UPDATE. See {@link resolve}.
   */
  async resolveByMetadata(
    params: ResolveByMetadataParams,
    namespace?: string,
  ): Promise<ResolveEscalationResult> {
    const ns = (params.namespace ?? namespace) ?? APP_ID;
    const hm = await this._engine(null, ns);
    const store = hm.engine.store as any;

    //peek the row the resolve is expected to lock and pre-build its wake;
    //the forSignalKey guard covers the race where a different row wins
    let wakeCommand: EscalationWakeCommand | null = null;
    const preview = await store.peekEscalationByMetadata({
      key: params.key,
      value: params.value,
      roles: params.roles,
      namespace: params.namespace,
    });
    if (preview?.signalKey) {
      wakeCommand = await this._buildWakeCommand(
        ns,
        preview.topic,
        preview.signalKey,
        params.resolverPayload ?? {},
      );
    }

    const dbResult = await store.resolveEscalationByMetadata(
      { key: params.key, value: params.value, resolverPayload: params.resolverPayload, roles: params.roles, metadata: params.metadata },
      wakeCommand ?? undefined,
    );
    if (!dbResult.ok) return dbResult;
    if (dbResult.signalKey && !dbResult.wakeEnqueued) {
      await this._deliverEscalationSignal(ns, dbResult.topic, {
        id: dbResult.signalKey,
        data: params.resolverPayload ?? {},
      });
    }
    this._emit('resolved', dbResult.entry);
    return { ok: true, entry: dbResult.entry };
  }

  /**
   * Full-fidelity migration: inserts an escalation row preserving the original
   * UUID and lifecycle state. Returns `null` on duplicate (idempotent).
   */
  async migrate(
    params: MigrateEscalationParams,
    namespace?: string,
  ): Promise<EscalationEntry | null> {
    const ns = (params.namespace ?? namespace) ?? APP_ID;
    const hm = await this._engine(null, ns);
    return (hm.engine.store as any).createEscalationForMigration(params);
  }

  /**
   * No-op in the implicit claim model — availability is computed at query time
   * from `assigned_until`. Kept for API compatibility.
   */
  async releaseExpired(namespace?: string): Promise<number> {
    const hm = await this._engine(null, namespace);
    return (hm.engine.store as any).releaseExpiredEscalations(namespace);
  }

  // ─── Bulk operations ────────────────────────────────────────────────────────

  /**
   * Bulk-claims up to `ids.length` pending escalations in one statement.
   * Returns `{ claimed, skipped }` — skipped rows are either already claimed
   * by another assignee or non-existent. Implicit-claim semantics apply.
   */
  async claimMany(params: ClaimManyParams): Promise<{ claimed: number; skipped: number }> {
    const hm = await this._engine(null, params.namespace);
    const { entries, skipped } = await (hm.engine.store as any).claimManyEscalations(params);
    this._emitMany('claimed', entries);
    return { claimed: entries.length, skipped };
  }

  /**
   * Bulk-reassigns pending escalations to a new role, clearing any current claim.
   * Returns the count of rows updated.
   */
  async escalateManyToRole(params: EscalateManyToRoleParams): Promise<number> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).escalateManyEscalationsToRole(params);
  }

  /**
   * Bulk-updates priority for pending escalations. Returns the count of rows updated.
   */
  async updateManyPriority(params: UpdateManyPriorityParams): Promise<number> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).updateManyEscalationsPriority(params);
  }

  /**
   * Bulk-resolves pending escalations by id-set. No signal delivery — intended
   * for redirect-to-triage flows where no workflow is waiting. Returns the
   * resolved rows.
   *
   * Pass `params.metadata` to merge a resolution patch into every winning
   * (still-pending) row's GIN-indexed `metadata` in the single atomic UPDATE.
   * See {@link resolve}.
   */
  /**
   * Bulk-resolves standalone escalations (rows with `signal_key = null`).
   * Rows backing a live `condition()` waiter are excluded — they stay
   * pending so a targeted `resolve()`/`cancel()` can deliver their wake;
   * bulk resolution carries no wake and would strand the workflow.
   */
  async resolveMany(params: ResolveManyParams): Promise<EscalationEntry[]> {
    const hm = await this._engine(null, params.namespace);
    const entries = await (hm.engine.store as any).resolveManyEscalations(params);
    this._emitMany('resolved', entries);
    return entries;
  }

  // ─── Aggregates ─────────────────────────────────────────────────────────────

  /**
   * Returns dashboard-ready escalation counts. `period` controls the window
   * used for `created` and `resolved` counts (default `'24h'`). When `roles`
   * is an empty array, all counts are zero (RBAC guard).
   */
  async stats(params?: StatsEscalationsParams): Promise<EscalationStats> {
    const hm = await this._engine(null, params?.namespace);
    return (hm.engine.store as any).escalationStats(params ?? {});
  }

  /**
   * Returns the sorted list of distinct `type` values in the escalations table.
   * Useful for populating filter dropdowns.
   */
  async listDistinctTypes(namespace?: string): Promise<string[]> {
    const hm = await this._engine(null, namespace);
    return (hm.engine.store as any).listDistinctEscalationTypes(namespace);
  }

  static async shutdown(): Promise<void> {
    for (const [_, instance] of EscalationClientService.instances) {
      (await instance).stop();
    }
    EscalationClientService.instances.clear();
  }
}
