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
