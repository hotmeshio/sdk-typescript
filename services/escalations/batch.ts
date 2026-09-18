import {
  AccumulateConfig,
  ESCALATION_BATCH_COUNT_KEY,
  ESCALATION_BATCH_FILLED_AT_KEY,
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
  ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY,
]);

/**
 * Folds a `batch` declaration into the escalation's storage shape:
 * queryable facets in `metadata` (`batch_pending`, `batch_count`,
 * `batch_keys`) and the payload accumulator in `envelope`
 * (`batch_items: {}`). `partialOnTimeout` is stored beside the items so
 * the expiry statement delivers the filled items instead of `false`. Pure
 * and deterministic — safe on the replayed `condition()` path. Throws
 * synchronously on an invalid declaration so a bad batch never reaches a
 * durable write.
 */
export function foldBatchConfig<
  T extends {
    batch?: string[];
    partialOnTimeout?: boolean;
    accumulate?: AccumulateConfig;
    metadata?: Record<string, unknown>;
    envelope?: Record<string, unknown>;
  },
>(config: T): Omit<T, 'batch' | 'partialOnTimeout'> {
  const { batch, partialOnTimeout, ...rest } = config;
  if (!batch) {
    if (partialOnTimeout !== undefined) {
      throw new Error('partialOnTimeout requires a batch declaration');
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
  return {
    ...rest,
    metadata: {
      ...(rest.metadata ?? {}),
      [ESCALATION_BATCH_PENDING_KEY]: [...batch],
      [ESCALATION_BATCH_COUNT_KEY]: batch.length,
      [ESCALATION_BATCH_KEYS_KEY]: [...batch],
    },
    envelope: {
      ...(rest.envelope ?? {}),
      [ESCALATION_BATCH_ITEMS_KEY]: {},
      [ESCALATION_BATCH_FILLED_AT_KEY]: {},
      ...(partialOnTimeout
        ? { [ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY]: true }
        : {}),
    },
  };
}
