import {
  AccumulateConfig,
  ESCALATION_ACCUMULATE_CONFIG_KEY,
  ESCALATION_ACCUMULATE_COUNT_KEY,
  ESCALATION_ACCUMULATE_ITEMS_KEY,
  ESCALATION_ACCUMULATE_KEYS_KEY,
  ESCALATION_ACCUMULATE_MAX_KEY,
  ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH,
} from '../../types/hmsh_escalations';

export const ACCUMULATE_RESERVED_METADATA_KEYS: ReadonlySet<string> = new Set([
  ESCALATION_ACCUMULATE_COUNT_KEY,
  ESCALATION_ACCUMULATE_MAX_KEY,
  ESCALATION_ACCUMULATE_KEYS_KEY,
]);

export const ACCUMULATE_RESERVED_ENVELOPE_KEYS: ReadonlySet<string> = new Set([
  ESCALATION_ACCUMULATE_ITEMS_KEY,
  ESCALATION_ACCUMULATE_CONFIG_KEY,
]);

/** The stored form of an `accumulate` declaration (`envelope.accumulate_config`). */
export interface StoredAccumulateConfig {
  unique: boolean;
  resolveAtMax: boolean;
}

/**
 * Validates an item key for the accumulate ops: a non-empty string within
 * the shared item-key length limit. Throws synchronously so a bad key never
 * reaches a statement.
 */
export function assertAccumulateItemKey(
  itemKey: unknown,
): asserts itemKey is string {
  if (typeof itemKey !== 'string' || itemKey.length === 0) {
    throw new Error('itemKey must be a non-empty string');
  }
  if (itemKey.length > ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH) {
    throw new Error(
      `itemKey must be at most ${ESCALATION_BATCH_ITEM_KEY_MAX_LENGTH} characters: '${itemKey.slice(0, 32)}…'`,
    );
  }
}

/**
 * Folds an `accumulate` declaration into the escalation's storage shape:
 * queryable facets in `metadata` (`accumulate_count: 0`, `accumulate_max`,
 * `accumulate_keys: []`) and the item store plus stored options in
 * `envelope` (`accumulate_items: {}`, `accumulate_config`). Pure and
 * deterministic, so it is safe on the replayed `condition()` path. Throws
 * synchronously on an invalid declaration so a bad accumulator never
 * reaches a durable write.
 */
export function foldAccumulateConfig<
  T extends {
    accumulate?: AccumulateConfig;
    batch?: string[];
    partialOnTimeout?: boolean;
    metadata?: Record<string, unknown>;
    envelope?: Record<string, unknown>;
  },
>(config: T): Omit<T, 'accumulate' | 'partialOnTimeout'> {
  const { accumulate, partialOnTimeout, ...rest } = config;
  if (!accumulate) return rest;
  if (typeof accumulate !== 'object' || Array.isArray(accumulate)) {
    throw new Error('accumulate must be an object');
  }
  if (rest.batch) {
    throw new Error('accumulate and batch are mutually exclusive');
  }
  if (partialOnTimeout !== undefined) {
    throw new Error(
      'partialOnTimeout applies to batch only; an accumulator always delivers on timeout',
    );
  }
  const { max, resolveAtMax = true, unique = true } = accumulate;
  if (max !== undefined && (!Number.isInteger(max) || max < 1)) {
    throw new Error('accumulate.max must be a positive integer');
  }
  if (typeof resolveAtMax !== 'boolean') {
    throw new Error('accumulate.resolveAtMax must be a boolean');
  }
  if (typeof unique !== 'boolean') {
    throw new Error('accumulate.unique must be a boolean');
  }
  for (const reserved of ACCUMULATE_RESERVED_METADATA_KEYS) {
    if (rest.metadata && reserved in rest.metadata) {
      throw new Error(
        `metadata key '${reserved}' is reserved for accumulate state`,
      );
    }
  }
  for (const reserved of ACCUMULATE_RESERVED_ENVELOPE_KEYS) {
    if (rest.envelope && reserved in rest.envelope) {
      throw new Error(
        `envelope key '${reserved}' is reserved for accumulate state`,
      );
    }
  }
  const stored: StoredAccumulateConfig = { unique, resolveAtMax };
  return {
    ...rest,
    metadata: {
      ...(rest.metadata ?? {}),
      [ESCALATION_ACCUMULATE_COUNT_KEY]: 0,
      [ESCALATION_ACCUMULATE_MAX_KEY]: max ?? null,
      [ESCALATION_ACCUMULATE_KEYS_KEY]: [],
    },
    envelope: {
      ...(rest.envelope ?? {}),
      [ESCALATION_ACCUMULATE_ITEMS_KEY]: {},
      [ESCALATION_ACCUMULATE_CONFIG_KEY]: stored,
    },
  };
}
