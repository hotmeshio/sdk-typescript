import {
  AccumulateConfig,
  BatchFilledEntry,
} from '../../types/hmsh_escalations';

import { foldAccumulateConfig } from './accumulate';
import { foldBatchConfig } from './batch';

type Foldable = {
  batch?: string[];
  partialOnTimeout?: boolean;
  filled?: Record<string, BatchFilledEntry>;
  accumulate?: AccumulateConfig;
  metadata?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
};

/**
 * Folds whichever accumulator declaration a creation config carries
 * (`batch` or `accumulate`) into its storage shape, and passes every other
 * config through untouched. The single entry point for both the
 * `condition()` path and standalone `create()`.
 */
export function foldEscalationConfig<T extends Foldable>(
  config: T,
): Omit<T, 'batch' | 'partialOnTimeout' | 'filled' | 'accumulate'> {
  const { batch, partialOnTimeout, filled, accumulate, ...rest } = config;
  if (batch) {
    if (accumulate) {
      throw new Error('accumulate and batch are mutually exclusive');
    }
    return foldBatchConfig({
      ...rest,
      batch,
      partialOnTimeout,
      filled,
    }) as Omit<T, 'batch' | 'partialOnTimeout' | 'filled' | 'accumulate'>;
  }
  if (accumulate) {
    if (filled !== undefined) {
      throw new Error('filled applies to batch only');
    }
    return foldAccumulateConfig({ ...rest, accumulate }) as Omit<
      T,
      'batch' | 'partialOnTimeout' | 'filled' | 'accumulate'
    >;
  }
  if (partialOnTimeout !== undefined) {
    throw new Error('partialOnTimeout requires a batch declaration');
  }
  if (filled !== undefined) {
    throw new Error('filled requires a batch declaration');
  }
  return rest;
}
