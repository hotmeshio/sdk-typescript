import { describe, it, expect } from 'vitest';

import { foldAccumulateConfig } from '../../../../services/escalations/accumulate';
import { foldBatchConfig } from '../../../../services/escalations/batch';
import { foldEscalationConfig } from '../../../../services/escalations/fold';

describe('UNIT | escalations | foldAccumulateConfig', () => {
  it('folds the declaration into facets and an empty item store', () => {
    const folded = foldAccumulateConfig({
      role: 'bin',
      metadata: { binKey: 'b-1' },
      envelope: { note: 'x' },
      accumulate: { max: 4 },
    });
    expect((folded as any).accumulate).toBeUndefined();
    expect(folded.metadata).toEqual({
      binKey: 'b-1',
      accumulate_count: 0,
      accumulate_max: 4,
      accumulate_keys: [],
    });
    expect(folded.envelope).toEqual({
      note: 'x',
      accumulate_items: {},
      accumulate_config: { unique: true, resolveAtMax: true },
    });
  });

  it('stores null max for an unbounded accumulator and keeps explicit options', () => {
    const folded = foldAccumulateConfig({ accumulate: { unique: false, resolveAtMax: false } });
    expect(folded.metadata!.accumulate_max).toBeNull();
    expect(folded.envelope!.accumulate_config).toEqual({ unique: false, resolveAtMax: false });
  });

  it('passes a config without accumulate through untouched', () => {
    const config = { role: 'x', metadata: { a: 1 } };
    expect(foldAccumulateConfig(config)).toEqual(config);
  });

  it('is deterministic for the replayed condition() path', () => {
    const config = { metadata: { k: 1 }, accumulate: { max: 2 } };
    expect(foldAccumulateConfig(config)).toEqual(foldAccumulateConfig(config));
  });

  it.each([
    [{ accumulate: { max: 0 } }, /positive integer/],
    [{ accumulate: { max: 1.5 } }, /positive integer/],
    [{ accumulate: { max: -3 } }, /positive integer/],
    [{ accumulate: { unique: 'yes' as unknown as boolean } }, /unique must be a boolean/],
    [{ accumulate: { resolveAtMax: 1 as unknown as boolean } }, /resolveAtMax must be a boolean/],
    [{ accumulate: [] as unknown as { max: number } }, /must be an object/],
    [{ accumulate: { max: 2 }, batch: ['a'] }, /mutually exclusive/],
    [{ accumulate: { max: 2 }, partialOnTimeout: true }, /batch only/],
    [{ accumulate: {}, metadata: { accumulate_count: 9 } }, /reserved for accumulate/],
    [{ accumulate: {}, envelope: { accumulate_items: {} } }, /reserved for accumulate/],
  ])('rejects %j', (config, message) => {
    expect(() => foldAccumulateConfig(config as any)).toThrow(message);
  });
});

describe('UNIT | escalations | foldBatchConfig partialOnTimeout', () => {
  it('stores the opt-in beside the item store', () => {
    const folded = foldBatchConfig({ batch: ['a', 'b'], partialOnTimeout: true });
    expect(folded.envelope!.batch_partial_on_timeout).toBe(true);
    expect((folded as any).partialOnTimeout).toBeUndefined();
  });

  it('writes nothing extra when the opt-in is absent', () => {
    const folded = foldBatchConfig({ batch: ['a'] });
    expect('batch_partial_on_timeout' in folded.envelope!).toBe(false);
  });

  it('rejects the opt-in without a batch and a non-boolean value', () => {
    expect(() => foldBatchConfig({ partialOnTimeout: true })).toThrow(/requires a batch/);
    expect(() => foldBatchConfig({ batch: ['a'], partialOnTimeout: 'x' as any })).toThrow(/boolean/);
  });

  it('rejects a reserved envelope key for the opt-in', () => {
    expect(() => foldBatchConfig({ batch: ['a'], envelope: { batch_partial_on_timeout: true } })).toThrow(/reserved/);
  });
});

describe('UNIT | escalations | foldEscalationConfig', () => {
  it('routes batch, accumulate, and plain configs', () => {
    expect(foldEscalationConfig({ batch: ['a'] }).metadata!.batch_count).toBe(1);
    expect(foldEscalationConfig({ accumulate: { max: 1 } }).metadata!.accumulate_max).toBe(1);
    const plain = { role: 'r', metadata: { z: 1 } };
    expect(foldEscalationConfig(plain)).toEqual(plain);
  });

  it('rejects both declarations at once and a stray partialOnTimeout', () => {
    expect(() => foldEscalationConfig({ batch: ['a'], accumulate: {} })).toThrow(/mutually exclusive/);
    expect(() => foldEscalationConfig({ partialOnTimeout: false })).toThrow(/requires a batch/);
  });
});
