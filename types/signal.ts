/**
 * Represents a queued signal record in the hotmesh_signals table.
 * Created atomically with workflow suspension when condition() is called
 * with a queue configuration.
 */
export interface SignalQueueEntry {
  id: string;
  namespace: string;
  appId: string;
  signalKey: string;
  workflowId: string;
  jobId?: string;
  topic?: string;
  status: 'pending' | 'claimed' | 'resolved' | 'expired' | 'released';
  role?: string;
  type?: string;
  subtype?: string;
  priority: number;
  description?: string;
  taskQueue?: string;
  workflowType?: string;
  assignedTo?: string;
  claimedAt?: Date;
  claimExpiresAt?: Date;
  resolvedAt?: Date;
  resolverPayload?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  expiresAt?: Date;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Optional queue configuration passed to condition() to create a
 * richly-typed, indexed, claimable signal record alongside workflow suspension.
 *
 * metadata is GIN-indexed and used for claimByMetadata / resolveByMetadata queries.
 * envelope is unindexed and intended for display context (form schemas, etc.).
 */
export interface ConditionQueueConfig {
  role?: string;
  type?: string;
  subtype?: string;
  priority?: number;
  description?: string;
  taskQueue?: string;
  workflowType?: string;
  assignedTo?: string;
  metadata?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
  durationMinutes?: number;
}

export interface EnqueueSignalParams {
  namespace: string;
  appId: string;
  signalKey: string;
  workflowId: string;
  jobId?: string;
  topic?: string;
  role?: string;
  type?: string;
  subtype?: string;
  priority?: number;
  description?: string;
  taskQueue?: string;
  workflowType?: string;
  assignedTo?: string;
  metadata?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
  expiresAt?: Date;
}

export interface ClaimSignalParams {
  id: string;
  assignee?: string;
  durationMinutes?: number;
}

export interface ClaimSignalByMetadataParams {
  key: string;
  value: unknown;
  assignee?: string;
  durationMinutes?: number;
}

export interface ResolveSignalParams {
  id: string;
  resolverPayload?: Record<string, unknown>;
}

export interface ResolveSignalByMetadataParams {
  key: string;
  value: unknown;
  resolverPayload?: Record<string, unknown>;
}

export interface ListSignalsParams {
  status?: 'pending' | 'claimed' | 'resolved' | 'expired' | 'released';
  role?: string;
  taskQueue?: string;
  limit?: number;
  offset?: number;
}

/**
 * Result of a claim operation (by ID or by metadata).
 *
 * - ok: true  → signal was claimed; entry contains the full record
 * - ok: false, reason: 'not-found'  → no signal exists for the given id/metadata
 * - ok: false, reason: 'conflict'   → signal exists but was already claimed concurrently
 */
export type ClaimSignalResult =
  | { ok: true; entry: SignalQueueEntry }
  | { ok: false; reason: 'not-found' | 'conflict' };

/**
 * Result of a resolve operation (by ID or by metadata).
 *
 * - ok: true  → signal marked resolved and workflow signal delivered
 * - ok: false, reason: 'not-found'       → no pending/claimed signal found
 * - ok: false, reason: 'already-resolved' → signal exists but was already resolved (id-based only)
 * - ok: false, reason: 'signal-failed'    → DB record updated but workflow signal delivery failed;
 *                                            signalKey is provided so callers can retry delivery
 */
export type ResolveSignalResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' }
  | { ok: false; reason: 'already-resolved' }
  | { ok: false; reason: 'signal-failed'; signalKey: string };

/**
 * Result of a release operation.
 *
 * - ok: true  → signal returned to pending
 * - ok: false, reason: 'not-found'    → no signal exists for this id
 * - ok: false, reason: 'wrong-status' → signal exists but is not in 'claimed' status
 */
export type ReleaseSignalResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'wrong-status' };
