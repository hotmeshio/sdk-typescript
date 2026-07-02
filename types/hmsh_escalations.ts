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
  | { ok: false; reason: 'not-found' | 'already-resolved' | 'already-cancelled' | 'already-expired' };

export type ReleaseEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'wrong-assignee' };

export type CancelEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'already-terminal' };

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
}

export interface EscalateToRoleParams {
  id: string;
  targetRole: string;
  namespace?: string;
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
