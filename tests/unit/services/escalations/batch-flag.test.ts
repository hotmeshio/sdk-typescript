import { describe, it, expect } from 'vitest';

import { assertBatchFlagParams, foldBatchConfig } from '../../../../services/escalations/batch';
import { foldEscalationConfig } from '../../../../services/escalations/fold';

describe('UNIT | escalations | foldBatchConfig wakeOnFlag', () => {
  it('stores the opt-in beside the item store', () => {
    const folded = foldBatchConfig({ batch: ['a', 'b'], wakeOnFlag: true });
    expect(folded.envelope!.batch_wake_on_flag).toBe(true);
    expect((folded as any).wakeOnFlag).toBeUndefined();
  });

  it('writes nothing extra when the opt-in is absent or false', () => {
    expect('batch_wake_on_flag' in foldBatchConfig({ batch: ['a'] }).envelope!).toBe(false);
    expect('batch_wake_on_flag' in foldBatchConfig({ batch: ['a'], wakeOnFlag: false }).envelope!).toBe(false);
  });

  it('keeps partialOnTimeout and wakeOnFlag independent', () => {
    const folded = foldBatchConfig({ batch: ['a'], partialOnTimeout: true, wakeOnFlag: true });
    expect(folded.envelope!.batch_partial_on_timeout).toBe(true);
    expect(folded.envelope!.batch_wake_on_flag).toBe(true);
  });

  it('rejects the opt-in without a batch and a non-boolean value', () => {
    expect(() => foldBatchConfig({ wakeOnFlag: true })).toThrow(/requires a batch/);
    expect(() => foldBatchConfig({ batch: ['a'], wakeOnFlag: 'yes' as any })).toThrow(/boolean/);
  });

  it('reserves the flag envelope keys at creation', () => {
    expect(() => foldBatchConfig({ batch: ['a'], envelope: { batch_flags: {} } })).toThrow(/reserved/);
    expect(() => foldBatchConfig({ batch: ['a'], envelope: { batch_wake_on_flag: true } })).toThrow(/reserved/);
  });

  it('routes the opt-in through foldEscalationConfig and rejects it elsewhere', () => {
    expect(foldEscalationConfig({ batch: ['a'], wakeOnFlag: true }).envelope!.batch_wake_on_flag).toBe(true);
    expect(() => foldEscalationConfig({ accumulate: { max: 2 }, wakeOnFlag: true })).toThrow(/batch only/);
    expect(() => foldEscalationConfig({ wakeOnFlag: true })).toThrow(/requires a batch/);
  });
});

describe('UNIT | escalations | assertBatchFlagParams', () => {
  const base = { id: 'e-1', itemKey: 'a', label: 'Missing' };

  it('accepts a minimal and a full request', () => {
    expect(() => assertBatchFlagParams(base)).not.toThrow();
    expect(() => assertBatchFlagParams({
      ...base, payload: { note: 'x' }, actor: 'u-1', metadata: { outcome: 'missing' },
    })).not.toThrow();
  });

  it('requires exactly one row selector', () => {
    expect(() => assertBatchFlagParams({ itemKey: 'a', label: 'x' })).toThrow(/exactly one/);
    expect(() => assertBatchFlagParams({ ...base, signalKey: 's' })).toThrow(/exactly one/);
  });

  it('bounds the key and the label', () => {
    expect(() => assertBatchFlagParams({ ...base, itemKey: '' })).toThrow(/itemKey/);
    expect(() => assertBatchFlagParams({ ...base, itemKey: 'k'.repeat(129) })).toThrow(/itemKey/);
    expect(() => assertBatchFlagParams({ ...base, label: '' })).toThrow(/label/);
    expect(() => assertBatchFlagParams({ ...base, label: 'l'.repeat(129) })).toThrow(/label/);
  });

  it('rejects malformed payload, actor and metadata', () => {
    expect(() => assertBatchFlagParams({ ...base, payload: [] as any })).toThrow(/payload/);
    expect(() => assertBatchFlagParams({ ...base, actor: '' })).toThrow(/actor/);
    expect(() => assertBatchFlagParams({ ...base, metadata: 'x' as any })).toThrow(/metadata/);
  });

  it('rejects a reserved batch key in the metadata patch', () => {
    for (const key of ['batch_pending', 'batch_count', 'batch_keys']) {
      expect(() => assertBatchFlagParams({ ...base, metadata: { [key]: [] } })).toThrow(/reserved/);
    }
  });
});
