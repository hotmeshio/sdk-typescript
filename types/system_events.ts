/**
 * System-event emission surface for HotMesh lifecycle transitions.
 *
 * The performing actor — the one engine/SDK call that commits a durable
 * transition — fires `EventsConfig.publish` exactly once, inline,
 * post-commit. The hook is fire-and-forget; the SDK never awaits it.
 *
 * ## Ontology
 *
 * Every event has a canonical `type` string and a `data` payload.
 * Consumers pattern-match on `type` and cherry-pick fields from `data`.
 *
 * ### Escalation lifecycle  (`system.escalation.{id}.{verb}`)
 *
 * | verb        | trigger                                                  | `data` shape        |
 * |-------------|----------------------------------------------------------|---------------------|
 * | `created`   | `client.create()` or hook Leg1 INSERT                    | full `EscalationEntry` row |
 * | `claimed`   | `client.claim()` / `client.claimByMetadata()`            | full `EscalationEntry` row |
 * | `released`  | `client.release()`                                       | full `EscalationEntry` row |
 * | `reassigned`| `client.escalateToRole()` / role change                  | full `EscalationEntry` row |
 * | `resolved`  | `client.resolve()` / `client.resolveByMetadata()`        | full `EscalationEntry` row |
 * | `cancelled` | `client.cancel()`                                        | full `EscalationEntry` row |
 *
 * ### Engine lifecycle  (`system.engine.{appId}.{verb}`)
 *
 * | verb       | trigger                          | `data` shape                     |
 * |------------|----------------------------------|----------------------------------|
 * | `started`  | `HotMesh.init()` completes       | `{ appId: string, guid: string }` |
 * | `stopped`  | `hotMesh.stop()` called          | `{ appId: string, guid: string }` |
 * | `deployed` | `kvTables.deploy()` completes    | `{ appId: string }`              |
 *
 * ### Worker lifecycle  (`system.worker.{taskQueue}.{verb}`)
 *
 * | verb       | trigger                          | `data` shape                                 |
 * |------------|----------------------------------|----------------------------------------------|
 * | `started`  | `Durable.Worker.create()` ready  | `{ taskQueue: string, appId: string }`       |
 * | `stopped`  | `worker.stop()` called           | `{ taskQueue: string, appId: string }`       |
 *
 * ## Registration — three construction sites
 *
 * **Site 1 — YAML DAG / hook Leg1 (escalation `created` + engine events):**
 * ```typescript
 * import { HotMesh } from '@hotmeshio/hotmesh';
 * const hm = await HotMesh.init({
 *   appId: 'myapp',
 *   engine: { connection },
 *   events: { publish: (e) => bus.emit(e.type, e) },
 * });
 * ```
 *
 * **Site 2 — Standalone `EscalationClientService` (all 6 verbs):**
 * ```typescript
 * import { Escalations } from '@hotmeshio/hotmesh';
 * const client = new Escalations.Client({
 *   connection,
 *   events: { publish: (e) => bus.emit(e.type, e) },
 * });
 * ```
 *
 * **Site 3 — `Durable.Client` (all 6 verbs via `.escalations`):**
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * const client = new Durable.Client({
 *   connection,
 *   events: { publish: (e) => bus.emit(e.type, e) },
 * });
 * // client.escalations.* operations now emit lifecycle events
 * ```
 *
 * ## Event ID format
 *
 * `event_id` is collision-proof across recurrences:
 * - Escalation: `${id}:${verb}:${updated_at_iso}` — claim→release→reclaim
 *   produces distinct IDs because `updated_at` changes on each transition.
 * - Engine / worker: `${app_id_or_queue}:${verb}:${ts}`.
 *
 * ## Fire-and-forget contract
 *
 * The SDK wraps every `publish` call in
 * `void Promise.resolve(publish(event)).catch(() => {})`.
 * A slow or throwing `publish` implementation never blocks or fails the
 * committed operation. Use an in-process buffer or async queue if you need
 * back-pressure.
 *
 * @example
 * ```typescript
 * import { Durable } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const client = new Durable.Client({
 *   connection: { class: Postgres, options: { connectionString: process.env.DATABASE_URL } },
 *   events: {
 *     publish: (event) => {
 *       if (event.type.endsWith('.created')) {
 *         // new escalation — route to the right team
 *         dispatch(event.data as EscalationEntry);
 *       }
 *     },
 *   },
 * });
 * ```
 */

/** Verbs for escalation lifecycle transitions. */
export type EscalationVerb =
  | 'created'
  | 'claimed'
  | 'released'
  | 'reassigned'
  | 'resolved'
  | 'cancelled';

/** Verbs for engine lifecycle transitions. */
export type EngineVerb = 'started' | 'stopped' | 'deployed';

/** Verbs for worker lifecycle transitions. */
export type WorkerVerb = 'started' | 'stopped';

/**
 * Canonical lifecycle event emitted by the SDK post-commit.
 *
 * `event_id` is stable across replays:
 *   - Escalation transitions: `${id}:${verb}:${updated_at_iso}` — unique
 *     per transition; a re-claim after release gets a new `updated_at`.
 *   - Engine / worker events: `${app_id}:${verb}:${ts}`.
 *
 * `data` carries the full committed row (escalation entry) or lifecycle
 * metadata (engine/worker). Consumers cherry-pick what they need; nothing
 * is pre-projected so the shape is future-proof.
 */
export interface SystemEvent {
  /** Stable, unique ID per durable transition. */
  event_id: string;
  /**
   * Canonical topic string.
   *
   * | Class       | Pattern                               |
   * |-------------|---------------------------------------|
   * | escalation  | `system.escalation.{id}.{verb}`       |
   * | engine      | `system.engine.{appId}.{verb}`        |
   * | worker      | `system.worker.{taskQueue}.{verb}`    |
   */
  type: string;
  /** ISO timestamp at emit time (wall-clock, post-commit). */
  ts: string;
  namespace: string;
  app_id: string;
  workflow_id?: string;
  topic?: string;
  origin_id?: string;
  parent_id?: string;
  trace_id?: string;
  span_id?: string;
  /**
   * Full committed row for escalation events; lifecycle metadata for
   * engine/worker events. Long-tail and hike-mono each cherry-pick fields
   * for their own event shape.
   */
  data: Record<string, unknown>;
}

/**
 * System-event sink configuration. Attach to `HotMeshConfig.events`,
 * `EscalationClientConfig.events`, or `ClientConfig.events` to receive
 * lifecycle events from the SDK.
 *
 * The SDK calls `publish` after each durable transition commits, from the
 * single actor that performed the commit. In a multi-container fleet every
 * container's SDK calls its own `publish` hook — and only for the work it
 * performed — so exactly one container's `publish` fires per real event.
 *
 * @example
 * ```typescript
 * const events: EventsConfig = {
 *   publish: (event) => {
 *     // map SystemEvent → your LTEvent shape and hand to NATS/Socket.IO
 *     eventRegistry.publish(mapToLTEvent(event));
 *   },
 * };
 * ```
 */
export interface EventsConfig {
  /**
   * Called post-commit by the performing actor. Fire-and-forget — the SDK
   * does not await the return value; a thrown/rejected promise is silently
   * swallowed so the committed call is never failed by a publish error.
   */
  publish: (event: SystemEvent) => void | Promise<void>;
}
