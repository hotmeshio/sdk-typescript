import {
  HMSH_LOGLEVEL,
} from '../../modules/enums';
import { hashOptions } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import { Connection } from '../../types/durable';
import { StreamStatus } from '../../types';
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
import { APP_ID } from '../durable/schemas/factory';

type GetHotMeshFn = (topic: string | null, namespace?: string) => Promise<HotMesh>;

export interface EscalationClientConfig {
  /** Postgres connection options — used when creating a standalone EscalationClient. */
  connection?: Connection;
  /**
   * Inject a pre-existing `getHotMeshClient` function (e.g. from Durable.Client).
   * When provided, the client reuses the caller's engine pool — no second connection.
   */
  getHotMeshClient?: GetHotMeshFn;
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
  private readonly _connection?: Connection;

  static instances = new Map<string, HotMesh | Promise<HotMesh>>();

  constructor(config: EscalationClientConfig = {}) {
    if (config.getHotMeshClient) {
      // Reuse a caller-supplied engine factory (e.g. Durable.Client) — no extra connections.
      this._engine = config.getHotMeshClient;
    } else if (config.connection) {
      this._connection = config.connection;
      this._engine = this._makeEngineFactory(config.connection);
    } else {
      throw new Error('EscalationClient requires either `connection` or `getHotMeshClient`');
    }
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

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Returns all escalation rows matching the given filters. Each row includes
   * a computed `available` field (true = claimable). Supports `sortBy`,
   * `sortOrder`, and multi-role `roles[]` filter.
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
    return (hm.engine.store as any).createEscalation(params);
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
   */
  async claim(params: ClaimEscalationParams): Promise<ClaimEscalationResult> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).claimEscalation(params);
  }

  /**
   * Atomically claims the highest-priority pending escalation whose `metadata`
   * contains the given key/value. Returns `isExtension: true` when the same
   * assignee re-claims a row they already hold (extends the expiry).
   */
  async claimByMetadata(params: ClaimByMetadataParams): Promise<ClaimByMetadataResult> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).claimEscalationByMetadata(params);
  }

  /** Releases a claimed escalation, returning it to available status. */
  async release(params: ReleaseEscalationParams): Promise<ReleaseEscalationResult> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).releaseEscalation(params);
  }

  /**
   * Reassigns the escalation to a different role, clearing any current claim
   * and resetting status to `'pending'`.
   */
  async escalateToRole(params: EscalateToRoleParams): Promise<EscalationEntry | null> {
    const hm = await this._engine(null, params.namespace);
    return (hm.engine.store as any).escalateEscalationToRole(params);
  }

  /**
   * Cancels a pending escalation without delivering a signal. Terminal rows
   * return `already-terminal`.
   */
  async cancel(id: string, namespace?: string): Promise<CancelEscalationResult> {
    const hm = await this._engine(null, namespace);
    return (hm.engine.store as any).cancelEscalation(id, namespace);
  }

  /**
   * Signal-first resolve: marks the escalation resolved **and** delivers the
   * signal to the waiting workflow in a single held transaction. If signal
   * delivery fails, the transaction rolls back — `committed: false`.
   */
  async resolve(
    params: ResolveEscalationParams,
    namespace?: string,
  ): Promise<ResolveEscalationResult> {
    const ns = (params.namespace ?? namespace) ?? APP_ID;
    const hm = await this._engine(null, ns);
    const dbResult = await (hm.engine.store as any).resolveEscalation(
      { id: params.id, resolverPayload: params.resolverPayload },
    );
    if (!dbResult.ok) return dbResult;
    if (dbResult.signalKey) {
      await this._deliverEscalationSignal(ns, dbResult.topic, {
        id: dbResult.signalKey,
        data: params.resolverPayload ?? {},
      });
    }
    return { ok: true };
  }

  /**
   * Resolves the highest-priority matching escalation by metadata filter,
   * then delivers its signal.
   */
  async resolveByMetadata(
    params: ResolveByMetadataParams,
    namespace?: string,
  ): Promise<ResolveEscalationResult> {
    const ns = (params.namespace ?? namespace) ?? APP_ID;
    const hm = await this._engine(null, ns);
    const dbResult = await (hm.engine.store as any).resolveEscalationByMetadata(
      { key: params.key, value: params.value, resolverPayload: params.resolverPayload, roles: params.roles },
    );
    if (!dbResult.ok) return dbResult;
    if (dbResult.signalKey) {
      await this._deliverEscalationSignal(ns, dbResult.topic, {
        id: dbResult.signalKey,
        data: params.resolverPayload ?? {},
      });
    }
    return { ok: true };
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

  static async shutdown(): Promise<void> {
    for (const [_, instance] of EscalationClientService.instances) {
      (await instance).stop();
    }
    EscalationClientService.instances.clear();
  }
}
