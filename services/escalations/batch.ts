import {
  AccumulateConfig,
  BatchFilledEntry,
  ESCALATION_BATCH_COUNT_KEY,
  ESCALATION_BATCH_FILLED_AT_KEY,
  ESCALATION_BATCH_FILLED_BY_KEY,
  ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH,
  ESCALATION_BATCH_ITEMS_KEY,
  ESCALATION_BATCH_KEYS_KEY,
  ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY,
  ESCALATION_BATCH_PENDING_KEY,
} from '../../types/hmsh_escalations';

const RESERVED_METADATA_KEYS = new Set([
  ESCALATION_BATCH_PENDING_KEY,
  ESCALATION_BATCH_COUNT_KEY,
  ESCALATION_BATCH_KEYS_KEY,
]);

const RESERVED_ENVELOPE_KEYS = new Set([
  ESCALATION_BATCH_ITEMS_KEY,
  ESCALATION_BATCH_FILLED_AT_KEY,
  ESCALATION_BATCH_FILLED_BY_KEY,
  ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY,
]);

interface FoldedFills {
  pending: string[];
  items: Record<string, unknown>;
  filledAt: Record<string, string>;
  filledBy: Record<string, string>;
}

/**
 * Validates `filled` against the declared keys and splits it into the
 * envelope maps. Pure: `at` is normalized from the caller's value, never
 * read from a clock.
 */
function foldFilledEntries(
  batch: string[],
  filled: Record<string, BatchFilledEntry> | undefined,
): FoldedFills {
  const folded: FoldedFills = {
    pending: [...batch],
    items: {},
    filledAt: {},
    filledBy: {},
  };
  if (filled === undefined) return folded;
  if (!filled || typeof filled !== 'object' || Array.isArray(filled)) {
    throw new Error('filled must be an object keyed by batch item key');
  }
  for (const [key, entry] of Object.entries(filled)) {
    if (!batch.includes(key)) {
      throw new Error(`filled key '${key}' is not declared in batch`);
    }
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error(`filled['${key}'] must be an object`);
    }
    const { payload, at, actor } = entry;
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      throw new Error(`filled['${key}'].payload must be an object`);
    }
    const time = typeof at === 'string' ? Date.parse(at) : NaN;
    if (Number.isNaN(time)) {
      throw new Error(`filled['${key}'].at must be an ISO-8601 timestamp`);
    }
    if (
      actor !== undefined &&
      (typeof actor !== 'string' || actor.length === 0)
    ) {
      throw new Error(`filled['${key}'].actor must be a non-empty string`);
    }
    folded.items[key] = payload;
    folded.filledAt[key] = new Date(time).toISOString();
    if (actor !== undefined) folded.filledBy[key] = actor;
  }
  folded.pending = batch.filter((key) => !(key in folded.items));
  if (folded.pending.length === 0) {
    throw new Error(
      'filled satisfies every batch key; there is nothing to wait for',
    );
  }
  return folded;
}

/**
 * Folds a `batch` declaration into the escalation's storage shape:
 * queryable facets in `metadata` (`batch_pending`, `batch_count`,
 * `batch_keys`) and the payload accumulator in `envelope`
 * (`batch_items: {}`). `partialOnTimeout` is stored beside the items so
 * the expiry statement delivers the filled items instead of `false`.
 * `filled` entries start satisfied: they are left out of `batch_pending`
 * and written into `batch_items`, `batch_filled_at`, and (when an entry
 * names an actor) `batch_filled_by`. Pure
 * and deterministic — safe on the replayed `condition()` path. Throws
 * synchronously on an invalid declaration so a bad batch never reaches a
 * durable write.
 */
export function foldBatchConfig<
  T extends {
    batch?: string[];
    partialOnTimeout?: boolean;
    filled?: Record<string, BatchFilledEntry>;
    accumulate?: AccumulateConfig;
    metadata?: Record<string, unknown>;
    envelope?: Record<string, unknown>;
  },
>(config: T): Omit<T, 'batch' | 'partialOnTimeout' | 'filled'> {
  const { batch, partialOnTimeout, filled, ...rest } = config;
  if (!batch) {
    if (partialOnTimeout !== undefined) {
      throw new Error('partialOnTimeout requires a batch declaration');
    }
    if (filled !== undefined) {
      throw new Error('filled requires a batch declaration');
    }
    return rest;
  }
  if (rest.accumulate) {
    throw new Error('accumulate and batch are mutually exclusive');
  }
  if (partialOnTimeout !== undefined && typeof partialOnTimeout !== 'boolean') {
    throw new Error('partialOnTimeout must be a boolean');
  }
  if (!Array.isArray(batch) || batch.length === 0) {
    throw new Error('batch must be a non-empty array of item keys');
  }
  const seen = new Set<string>();
  for (const key of batch) {
    if (typeof key !== 'string' || key.length === 0) {
      throw new Error('batch item keys must be non-empty strings');
    }
    if (key.length > ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH) {
      throw new Error(
        `batch item keys must be at most ${ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH} characters: '${key.slice(0, 32)}…'`,
      );
    }
    if (seen.has(key)) {
      throw new Error(`batch item keys must be unique: '${key}'`);
    }
    seen.add(key);
  }
  for (const reserved of RESERVED_METADATA_KEYS) {
    if (rest.metadata && reserved in rest.metadata) {
      throw new Error(`metadata key '${reserved}' is reserved for batch state`);
    }
  }
  for (const reserved of RESERVED_ENVELOPE_KEYS) {
    if (rest.envelope && reserved in rest.envelope) {
      throw new Error(`envelope key '${reserved}' is reserved for batch state`);
    }
  }
  const fills = foldFilledEntries(batch, filled);
  return {
    ...rest,
    metadata: {
      ...(rest.metadata ?? {}),
      [ESCALATION_BATCH_PENDING_KEY]: fills.pending,
      [ESCALATION_BATCH_COUNT_KEY]: fills.pending.length,
      [ESCALATION_BATCH_KEYS_KEY]: [...batch],
    },
    envelope: {
      ...(rest.envelope ?? {}),
      [ESCALATION_BATCH_ITEMS_KEY]: fills.items,
      [ESCALATION_BATCH_FILLED_AT_KEY]: fills.filledAt,
      ...(Object.keys(fills.filledBy).length
        ? { [ESCALATION_BATCH_FILLED_BY_KEY]: fills.filledBy }
        : {}),
      ...(partialOnTimeout
        ? { [ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY]: true }
        : {}),
    },
  };
}
