import { AccumulateConfig } from '../../types/hmsh_escalations';

import { foldAccumulateConfig } from './accumulate';
import { foldBatchConfig } from './batch';

type Foldable = {
  batch?: string[];
  partialOnTimeout?: boolean;
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
): Omit<T, 'batch' | 'partialOnTimeout' | 'accumulate'> {
  const { batch, partialOnTimeout, accumulate, ...rest } = config;
  if (batch) {
    if (accumulate) {
      throw new Error('accumulate and batch are mutually exclusive');
    }
    return foldBatchConfig({ ...rest, batch, partialOnTimeout }) as Omit<
      T,
      'batch' | 'partialOnTimeout' | 'accumulate'
    >;
  }
  if (accumulate) {
    return foldAccumulateConfig({ ...rest, accumulate }) as Omit<
      T,
      'batch' | 'partialOnTimeout' | 'accumulate'
    >;
  }
  if (partialOnTimeout !== undefined) {
    throw new Error('partialOnTimeout requires a batch declaration');
  }
  return rest;
}
