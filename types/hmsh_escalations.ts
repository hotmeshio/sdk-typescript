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
  status: 'pending' | 'claimed' | 'resolved' | 'cancelled' | 'expired';
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
  created_at: Date;
  updated_at: Date;
}

/**
 * Result of `claim()` — identifies whether failure was due to the row not
 * existing (`not-found`) or existing but locked / in a non-claimable state
 * (`conflict`). Distinguishing these lets callers decide whether to retry or
 * surface an error to the user.
 */
export type ClaimEscalationResult =
  | { ok: true; entry: EscalationEntry }
  | { ok: false; reason: 'not-found' | 'conflict' };

/**
 * Result of `claimByMetadata()`. Includes `candidatesExist` — the total
 * count of rows matching the metadata filter regardless of claimability — so
 * callers can distinguish "nothing matching at all" from "found candidates but
 * all are locked or in-progress".
 */
export type ClaimByMetadataResult =
  | { ok: true; entry: EscalationEntry; candidatesExist: number }
  | { ok: false; reason: 'not-found' | 'conflict'; candidatesExist: number };

export type ResolveEscalationResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'already-resolved' | 'already-cancelled' | 'signal-failed' };

export type ReleaseEscalationResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'wrong-assignee' };

export type CancelEscalationResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'already-terminal' };

export interface ListEscalationsParams {
  namespace?: string;
  role?: string;
  type?: string;
  subtype?: string;
  entity?: string;
  status?: string;
  assignedTo?: string;
  workflowId?: string;
  originId?: string;
  limit?: number;
  offset?: number;
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
}

export interface ResolveByMetadataParams {
  key: string;
  value: unknown;
  namespace?: string;
  resolverPayload?: Record<string, unknown>;
  roles?: string[];
}

export interface EscalateToRoleParams {
  id: string;
  targetRole: string;
  namespace?: string;
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
