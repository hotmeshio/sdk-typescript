export interface ConditionQueueConfig {
  role?: string;
  type?: string;
  subtype?: string;
  entity?: string;
  priority?: number;
  description?: string;
  taskQueue?: string;
  workflowType?: string;
  originId?: string;
  parentId?: string;
  initiatedBy?: string;
  traceId?: string;
  spanId?: string;
  /** GIN-indexed; put claim/filter keys here */
  metadata?: Record<string, unknown>;
  /** Unindexed display/form context for resolver UIs */
  envelope?: Record<string, unknown>;
  expiresAt?: Date;
  /**
   * Declares the wait as a batch accumulator: the escalation only resolves
   * once every named item has been submitted via `resolveBatchItem()`. Each
   * entry is an item key a contributor fills exactly once; the wait's signal
   * fires with the full collection (`Record<itemKey, payload>`) when the LAST
   * item lands. Folded at creation into the row's queryable facets
   * (`metadata.batch_pending` / `batch_count` / `batch_keys`) and payload
   * accumulator (`envelope.batch_items`) — one atomic INSERT with the
   * workflow checkpoint. `timeout` and `cancel()` semantics are unchanged:
   * `false` / `null`, with any partially filled items preserved on the
   * terminal row for audit.
   */
  batch?: string[];
  /**
   * Born-assigned: writes `assigned_to` in the same atomic INSERT that
   * creates the row — on the `condition()` path, one commit with the
   * workflow's Leg1 checkpoint. Alone it is a durable pre-assignment: the
   * row surfaces in the assignee's `list({ assignedTo })` immediately, is
   * resolvable by the assignee with `assertClaim`, and remains claimable
   * by others (a routing hint). With `durationMinutes` it is a hard claim
   * at creation. The `created` event payload carries the assignment.
   */
  assignee?: string;
  /**
   * With `assignee`, arms the claim TTL window (`assigned_until` /
   * `claim_expires_at`) at creation — the row is born locked to the
   * assignee, exactly as a post-create `claim()` locks it.
   */
  durationMinutes?: number;
  /**
   * SLA timer for the wait itself (e.g. `'30m'`, `'24h'`). Arms the same
   * resume timer as `condition(signalId, '30m')`: when it fires first, the
   * workflow resumes with `false` and the escalation row transitions
   * `pending → expired` (a later resolve fails as already-expired). A signal
   * that arrives first resolves normally and the timer is inert.
   */
  timeout?: string;
}

export interface EscalationEntry {
  id: string;
  namespace: string;
  app_id: string;
  /** Job ID / Durable signalId; NULL for standalone (no-signal) escalations */
  signal_key: string | null;
  /** Hook topic for signal delivery */
  topic: string | null;
  workflow_id: string | null;
  task_queue: string | null;
  workflow_type: string | null;
  type: string | null;
  subtype: string | null;
  entity: string | null;
  description: string | null;
  role: string | null;
  /** Lifecycle status. Claims are implicit: status='pending' + assigned_to IS NOT NULL + assigned_until > NOW(). */
  status: 'pending' | 'resolved' | 'cancelled' | 'expired';
  priority: number;
  assigned_to: string | null;
  assigned_until: Date | null;
  claimed_at: Date | null;
  claim_expires_at: Date | null;
  resolved_at: Date | null;
  escalation_payload: Record<string, unknown> | null;
  resolver_payload: Record<string, unknown> | null;
  envelope: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
  origin_id: string | null;
  parent_id: string | null;
  initiated_by: string | null;
  created_by: string | null;
  milestones: unknown[];
  trace_id: string | null;
  span_id: string | null;
  expires_at: Date | null;
  /** Nullable passthrough column — populated when downstream needs task-level context. */
  task_id: string | null;
  created_at: Date;
  updated_at: Date;
  /** Computed by list(): true when the row is claimable (no active assignee or expired claim). */
  available?: boolean;
}

/**
 * Result of `claim()` — identifies whether failure was due to the row not
 * existing (`not-found`) or existing but locked / in a non-claimable state
 * (`conflict`). Distinguishing these lets callers decide whether to retry or
 * surface an error to the user.
 */
export type ClaimEscalationResult =
  | { ok: true; entry: EscalationEntry; isExtension: boolean }
  | { ok: false; reason: 'not-found' | 'conflict' };

/**
 * Result of `claimByMetadata()`. Includes `candidatesExist` and `isExtension`:
 * - `candidatesExist` — total count of rows matching the filter regardless of claimability
 * - `isExtension` — true when the same assignee re-claims a row they already hold (extends the expiry)
 */
export type ClaimByMetadataResult =
  | { ok: true; entry: EscalationEntry; candidatesExist: number; isExtension: boolean }
  | { ok: false; reason: 'not-found' | 'conflict'; candidatesExist: number };

export type ResolveEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'already-resolved' | 'already-cancelled' | 'already-expired' | 'claim-expired' | 'claimed-by-other' };

/**
 * A pre-built wake message, committed INSIDE the resolve/cancel transaction
 * so the awaiting workflow's wake is durable with the status change. The
 * client composes the webhook message; the store owns the stream INSERT.
 * `forSignalKey` pins the wake to the row it was built for — it is written
 * only when the affected row's `signal_key` matches (a mismatched row falls
 * back to post-commit delivery).
 */
export interface EscalationWakeCommand {
  forSignalKey: string;
  message: string;
}

/**
 * Resolution provenance delivered to the waiting workflow alongside the
 * resolver payload, under the reserved `$resolution` key. Present exactly when
 * the resolving caller supplied `resolvedBy` — without it the signal payload
 * passes through byte-identical, so existing waiters are unaffected. The
 * `$`-prefixed key namespace is reserved for control data riding the signal
 * (like `$escalation_id` on the legacy routing path); consumer payload fields
 * never collide with it. The stored `resolver_payload` row column stays clean —
 * provenance rides the signal only.
 *
 * A waiting workflow declares the key in its `condition()` payload generic:
 *
 * ```typescript
 * const decision = await Durable.workflow.condition<{
 *   approved: boolean;
 *   $resolution?: EscalationResolution;
 * }>(signalId, config);
 * decision.$resolution?.resolvedBy; // who resolved it
 * ```
 */
export interface EscalationResolution {
  /** The id of the escalation row whose resolve delivered this signal. */
  escalationId: string;
  /** Resolver's user id. */
  resolvedBy: string;
  /** Resolver's email — present when supplied alongside the id. */
  resolvedByEmail?: string;
}

/**
 * Resolver identity, supplied by the resolving caller (the API layer that
 * authenticated the human or agent). Delivered to the waiting workflow inside
 * `$resolution`; never written to the stored `resolver_payload`.
 */
export interface ResolvedByIdentity {
  id: string;
  email?: string;
}

export type ReleaseEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'wrong-assignee' };

export type CancelEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'already-terminal' };

/**
 * Retention parameters for `pruneEscalations`. Prunes only terminal rows
 * (`resolved`/`cancelled`/`expired`) — the statuses every engine state
 * transition treats as final — older than the given horizon.
 */
export interface PruneEscalationsParams {
  /**
   * Age horizon as a Postgres interval string (e.g. `'90 days'`, `'12 hours'`).
   * Rows qualify when `updated_at < NOW() - olderThan`.
   */
  olderThan: string;
  /** Terminal statuses to prune. Defaults to all three; non-terminal values are ignored. */
  statuses?: Array<'resolved' | 'cancelled' | 'expired'>;
  namespace?: string;
  /**
   * Max rows deleted per call (bounds lock time and vacuum pressure).
   * Default 10,000, capped at 100,000. Loop until `deleted` is 0 to drain.
   */
  limit?: number;
}

export interface PruneEscalationsResult {
  deleted: number;
}

export interface ListEscalationsParams {
  namespace?: string;
  role?: string;
  /** Filter by one or more roles (OR semantics; takes precedence over `role` when both set). */
  roles?: string[];
  type?: string;
  subtype?: string;
  entity?: string;
  status?: string;
  assignedTo?: string;
  workflowId?: string;
  originId?: string;
  /**
   * Filter by `parent_id` — the hand-off lineage key. With `assignedTo`
   * this is the precise fallback query for a born-assigned child: "the
   * child of the escalation I just resolved, assigned to me."
   */
  parentId?: string;
  /** When true, returns only rows without an active claim. When false, returns only actively claimed rows. */
  available?: boolean;
  /** Exact priority match. */
  priority?: number;
  /** JSONB containment filter — rows whose `metadata` contains all provided keys/values. */
  metadata?: Record<string, unknown>;
  /** Filter by a set of UUIDs. */
  ids?: string[];
  /** Filter by `task_id` column. */
  taskId?: string;
  sortBy?: 'created_at' | 'priority' | 'updated_at';
  sortOrder?: 'asc' | 'desc';
  /**
   * Multi-column sort. When provided, supersedes `sortBy`/`sortOrder`.
   * Columns are applied left to right.
   */
  orderBy?: Array<{
    column: 'priority' | 'created_at' | 'updated_at' | 'resolved_at' | 'role' | 'type';
    direction: 'asc' | 'desc';
  }>;
  limit?: number;
  offset?: number;
}

export interface StatsEscalationsParams {
  namespace?: string;
  /** RBAC scope — when an empty array is provided, all counts are zero. */
  roles?: string[];
  /** Counting window for created/resolved. Default: '24h'. */
  period?: '1h' | '24h' | '7d' | '30d';
}

export interface EscalationStats {
  pending: number;
  claimed: number;
  created: number;
  resolved: number;
  by_role: Array<{ role: string; pending: number; claimed: number }>;
  by_type: Array<{ type: string; pending: number; claimed: number; resolved: number }>;
}

export interface CreateEscalationParams {
  namespace?: string;
  appId?: string;
  signalKey?: string;
  topic?: string;
  workflowId?: string;
  taskQueue?: string;
  workflowType?: string;
  type?: string;
  subtype?: string;
  entity?: string;
  description?: string;
  role?: string;
  priority?: number;
  originId?: string;
  parentId?: string;
  initiatedBy?: string;
  createdBy?: string;
  traceId?: string;
  spanId?: string;
  taskId?: string;
  escalationPayload?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
  expiresAt?: Date;
  /**
   * Born-assigned: writes `assigned_to` in the creation INSERT. See
   * {@link ConditionQueueConfig.assignee} for the two modes (durable
   * pre-assignment vs. hard claim with `durationMinutes`).
   */
  assignee?: string;
  /** With `assignee`, arms the claim TTL window at creation. See {@link ConditionQueueConfig.durationMinutes}. */
  durationMinutes?: number;
  /**
   * Declares the row as a batch accumulator. See
   * {@link ConditionQueueConfig.batch}. Standalone batch rows (no
   * `signalKey`) accumulate and complete identically; the wake is a no-op.
   */
  batch?: string[];
}

/**
 * Fields that can be patched on an existing escalation. All fields are
 * optional — only provided fields are written. Signal routing fields
 * (`signalKey`, `topic`, `workflowId`, `taskQueue`, `workflowType`) support
 * the legacy two-step pattern where routing context is enriched after creation.
 */
export interface UpdateEscalationParams {
  id: string;
  namespace?: string;
  description?: string;
  priority?: number;
  role?: string;
  taskId?: string;
  /** Merged into existing metadata (keys overwritten, others preserved) */
  metadata?: Record<string, unknown>;
  /** Replaces existing envelope */
  envelope?: Record<string, unknown>;
  /** Signal routing enrichment — equivalent to long-tail's enrichEscalationRouting */
  signalKey?: string;
  topic?: string;
  workflowId?: string;
  taskQueue?: string;
  workflowType?: string;
  expiresAt?: Date;
}

export interface AppendMilestonesParams {
  id: string;
  namespace?: string;
  milestones: Array<{ name: string; value: unknown; [key: string]: unknown }>;
}

export interface ClaimEscalationParams {
  id: string;
  namespace?: string;
  assignee?: string;
  durationMinutes?: number;
}

export interface ClaimByMetadataParams {
  key: string;
  value: unknown;
  namespace?: string;
  assignee?: string;
  durationMinutes?: number;
  roles?: string[];
  /** Merged (not replaced) into the claimed row's metadata in the same atomic UPDATE. */
  metadata?: Record<string, unknown>;
}

export interface ReleaseEscalationParams {
  id: string;
  namespace?: string;
  /** When provided, the release is rejected with `wrong-assignee` if the current assignee differs */
  assignee?: string;
}

export interface ResolveEscalationParams {
  id: string;
  namespace?: string;
  resolverPayload?: Record<string, unknown>;
  /**
   * Merged (not replaced) into the resolved row's `metadata` in the same atomic
   * UPDATE — and only on the winning resolve. Records "what actually happened"
   * into the GIN-indexed (`@>`-queryable) surface alongside the creation metadata.
   * Distinct from `resolverPayload`, which is delivered to the waiting workflow as
   * `condition()`'s return value and is not GIN-indexed.
   */
  metadata?: Record<string, unknown>;
  /**
   * When provided, the resolve additionally asserts — inside the same guarded
   * UPDATE — that no claim LOCK stands against this assignee. A claim is a
   * lock only while its TTL window (`assigned_until`) is active; the assert
   * blocks exactly two states: a live window held by a different assignee
   * (`claimed-by-other`), and this assignee's own lapsed window
   * (`claim-expired` — stale work; re-claim to resolve). Unclaimed rows,
   * durable pre-assignments (`assigned_to` with no window), and rows whose
   * window lapsed under a different assignee resolve normally. Closes the
   * claim-race window for interactive claim-then-resolve flows.
   */
  assertClaim?: string;
  /**
   * Resolver identity, delivered to the waiting workflow under the reserved
   * `$resolution` signal key (see {@link EscalationResolution}). Never written
   * to the stored `resolver_payload`.
   */
  resolvedBy?: ResolvedByIdentity;
}

export interface ResolveByMetadataParams {
  key: string;
  value: unknown;
  namespace?: string;
  resolverPayload?: Record<string, unknown>;
  roles?: string[];
  /**
   * Merge patch applied to the matched row's `metadata` (shallow, not replaced) in
   * the same atomic UPDATE. Note this is the resolution patch — distinct from the
   * `key`/`value` selector used to find the row. See {@link ResolveEscalationParams.metadata}.
   */
  metadata?: Record<string, unknown>;
  /** Resolver identity delivered under `$resolution`. See {@link ResolveEscalationParams.resolvedBy}. */
  resolvedBy?: ResolvedByIdentity;
}

export interface EscalateToRoleParams {
  id: string;
  targetRole: string;
  namespace?: string;
}

// ─── Batch accumulation ───────────────────────────────────────────────────────

/** Metadata facet: item keys still awaiting submission (jsonb string array).
 * `metadata @> '{"batch_pending":["x"]}'` finds rows still missing item `x`. */
export const ESCALATION_BATCH_PENDING_KEY = 'batch_pending';
/** Metadata facet: count of items still awaiting submission. Recomputed from
 * `batch_pending` in every fill statement — the two can never drift. */
export const ESCALATION_BATCH_COUNT_KEY = 'batch_count';
/** Metadata facet: the full declared item-key list, immutable after creation. */
export const ESCALATION_BATCH_KEYS_KEY = 'batch_keys';
/** Envelope key: the payload accumulator (`Record<itemKey, payload>`).
 * Payloads are plumbing, not facets — they live in the unindexed envelope.
 * Payload keys are caller-owned: the platform will never reserve names
 * inside `batch_items` values. */
export const ESCALATION_BATCH_ITEMS_KEY = 'batch_items';
/** Envelope key: per-item fill timestamps (`Record<itemKey, iso8601>`),
 * stamped by the DATABASE clock inside the same guarded fill statement —
 * row truth, not caller clocks. The attempt timeline for free. */
export const ESCALATION_BATCH_FILLED_AT_KEY = 'batch_filled_at';

/** Item keys are non-empty strings up to this length. Any characters are
 * accepted (keys are always parameterized and stored as jsonb text); prefer
 * URL/query-friendly names (e.g. `u1-L`) for endpoint ergonomics. */
export const ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH = 128;

/**
 * Outcome of a `resolveBatchItem()` call.
 * - `completed` — this was the LAST item: the row resolved and the waiting
 *   workflow was woken with the full collection, in the same statement.
 * - `accepted` — interim fill: the item landed, the row stays `pending`.
 * - `duplicate-item` — the key was declared but already filled.
 * - `unknown-item` — the key was never declared in `batch_keys`.
 * - `not-batch` — the row carries no batch declaration.
 * - Remaining values match {@link ResolveEscalationResult} semantics.
 */
export type BatchItemOutcome =
  | 'completed'
  | 'accepted'
  | 'duplicate-item'
  | 'unknown-item'
  | 'not-batch'
  | 'not-found'
  | 'already-resolved'
  | 'already-cancelled'
  | 'already-expired'
  | 'claim-expired'
  | 'claimed-by-other';

export interface ResolveBatchItemParams {
  /** Row selector — exactly one of `id` | `signalKey`. */
  id?: string;
  /** Row selector — the value passed to `condition()`. */
  signalKey?: string;
  namespace?: string;
  /** The declared batch key this submission fills. */
  itemKey: string;
  /** The item's payload — stored under `envelope.batch_items[itemKey]` and,
   * on completion, delivered as `collection[itemKey]` to the waiter. */
  payload: Record<string, unknown>;
  /** Merge patch applied to the row's GIN-indexed `metadata` in the same
   * atomic UPDATE. See {@link ResolveEscalationParams.metadata}. */
  metadata?: Record<string, unknown>;
  /** Claim-lock assertion inside the same guarded UPDATE. Batch fills are
   * claim-agnostic without it (multiple contributors are the norm). See
   * {@link ResolveEscalationParams.assertClaim}. */
  assertClaim?: string;
  /** Resolver identity. Delivered under `$resolution` ONLY on the completing
   * item's signal. See {@link ResolveEscalationParams.resolvedBy}. */
  resolvedBy?: ResolvedByIdentity;
}

export interface ResolveBatchItemByMetadataParams {
  /** Facet selector — mirrors {@link ResolveByMetadataParams}. */
  key: string;
  value: unknown;
  roles?: string[];
  namespace?: string;
  itemKey: string;
  payload: Record<string, unknown>;
  /** Merge patch for the matched row's `metadata` — distinct from the
   * `key`/`value` selector. */
  metadata?: Record<string, unknown>;
  resolvedBy?: ResolvedByIdentity;
}

/**
 * Result of `resolveBatchItem()` / `resolveBatchItemByMetadata()`. On
 * `ok: true`, `remaining` is the count of items still unfilled (0 exactly
 * when `outcome` is `completed`) and `entry` is the post-fill row.
 */
export type ResolveBatchItemResult =
  | { ok: true; outcome: 'completed' | 'accepted'; remaining: number; entry: EscalationEntry }
  | { ok: false; outcome: Exclude<BatchItemOutcome, 'completed' | 'accepted'> };

/**
 * Query selector for `claimManyByQuery()` — the filterable subset of
 * `ListEscalationsParams` that describes a claimable population. `status` is
 * not accepted: the claim targets pending rows by definition, enforced in SQL.
 */
export interface ClaimManyQuerySelector {
  role?: string;
  roles?: string[];
  type?: string;
  subtype?: string;
  entity?: string;
  priority?: number;
  /** GIN-indexed `@>` containment against row metadata — the facet filter. */
  metadata?: Record<string, unknown>;
}

/**
 * Atomic query-form bulk claim: one UPDATE selects and claims every matching
 * pending, claimable row — no SELECT-then-claim window. Prefer this over
 * `list()` + `claimMany({ids})` whenever the population is describable by
 * filter: a row that re-parks between a search and an ids-claim is invisible
 * to the ids form but claimed by this one.
 */
export interface ClaimManyByQueryParams {
  query: ClaimManyQuerySelector;
  namespace?: string;
  assignee: string;
  durationMinutes?: number;
}

export interface ClaimManyParams {
  ids: string[];
  namespace?: string;
  assignee: string;
  durationMinutes?: number;
}

export interface EscalateManyToRoleParams {
  ids: string[];
  namespace?: string;
  targetRole: string;
}

export interface UpdateManyPriorityParams {
  ids: string[];
  namespace?: string;
  priority: number;
}

export interface ResolveManyParams {
  ids: string[];
  namespace?: string;
  resolverPayload?: Record<string, unknown>;
  /**
   * Merged (not replaced) into every winning (still-pending) row's `metadata` in
   * the single bulk UPDATE. See {@link ResolveEscalationParams.metadata}.
   */
  metadata?: Record<string, unknown>;
}

/** One member of a `resolveAllOrNone()` batch — its own `resolverPayload` is
 * stored as that row's `resolver_payload` and delivered to that row's waiting
 * workflow as `condition()`'s return value. */
export interface ResolveAllOrNoneItem {
  id: string;
  resolverPayload?: Record<string, unknown>;
}

export interface ResolveAllOrNoneParams {
  /** The batch. Ids must be unique; each item carries its own payload. */
  items: ResolveAllOrNoneItem[];
  namespace?: string;
  /**
   * Shared outcome patch merged (not replaced) into EVERY row's GIN-indexed
   * `metadata` in the single atomic statement. See {@link ResolveEscalationParams.metadata}.
   */
  metadata?: Record<string, unknown>;
  /**
   * When provided, every row must currently be assigned to this assignee
   * (`assigned_to` equality, asserted inside the same statement). Closes the
   * claim-race window for claim-then-resolve flows: a row re-claimed by another
   * principal between the caller's claim and this resolve blocks the batch.
   */
  assertAssignee?: string;
  /**
   * Resolver identity delivered to EVERY item's waiting workflow under
   * `$resolution` (one resolver per batch). See {@link ResolveEscalationParams.resolvedBy}.
   */
  resolvedBy?: ResolvedByIdentity;
}

/** Why a specific row blocked a `resolveAllOrNone()` batch. */
export type ResolveAllOrNoneBlockReason =
  | 'not-found'
  | 'already-resolved'
  | 'already-cancelled'
  | 'already-expired'
  | 'assignee-mismatch';

/**
 * Result of `resolveAllOrNone()`. On `ok: false` NOTHING was written; `failed`
 * lists only the rows that blocked the batch (rows that were themselves
 * resolvable are not listed — they remain pending, untouched).
 */
export type ResolveAllOrNoneResult =
  | { ok: true; entries: EscalationEntry[] }
  | { ok: false; failed: Array<{ id: string; reason: ResolveAllOrNoneBlockReason }> };

/**
 * Full-fidelity migration params. Extends `CreateEscalationParams` with:
 * - `id` (required) — preserves the original UUID; no auto-generation
 * - lifecycle state fields (`status`, `assignedTo`, `claimExpiresAt`, …) — carry over
 *   the exact state of the migrated row so in-flight escalations land correctly
 * - `createdAt` / `updatedAt` — preserve original timestamps
 *
 * The underlying INSERT uses `ON CONFLICT (id) DO NOTHING`, so calling
 * `migrate()` multiple times with the same ID is safe — subsequent calls
 * return `null` without touching the existing row.
 */
export interface MigrateEscalationParams extends CreateEscalationParams {
  /** Required — preserve the original UUID from the source table. */
  id: string;
  status?: 'pending' | 'claimed' | 'resolved' | 'cancelled' | 'expired';
  assignedTo?: string;
  claimExpiresAt?: Date;
  claimedAt?: Date;
  resolvedAt?: Date;
  resolverPayload?: Record<string, unknown>;
  milestones?: Array<{ name: string; value: unknown; [key: string]: unknown }>;
  createdAt?: Date;
  updatedAt?: Date;
}
