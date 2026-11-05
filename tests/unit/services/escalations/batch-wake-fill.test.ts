import { describe, it, expect } from 'vitest';

import { foldBatchConfig } from '../../../../services/escalations/batch';
import { foldEscalationConfig } from '../../../../services/escalations/fold';

describe('UNIT | escalations | foldBatchConfig wakeOnFill', () => {
  it('stores true beside the item store', () => {
    const folded = foldBatchConfig({ batch: ['a', 'b'], wakeOnFill: true });
    expect(folded.envelope!.batch_wake_on_fill).toBe(true);
    expect((folded as any).wakeOnFill).toBeUndefined();
  });

  it('stores a key list de-duplicated', () => {
    const folded = foldBatchConfig({ batch: ['a', 'b', 'c'], wakeOnFill: ['b', 'c', 'b'] });
    expect(folded.envelope!.batch_wake_on_fill).toEqual(['b', 'c']);
  });

  it('writes nothing extra when the opt-in is absent, false, or empty', () => {
    for (const wakeOnFill of [undefined, false, []]) {
      const folded = foldBatchConfig({ batch: ['a'], wakeOnFill });
      expect('batch_wake_on_fill' in folded.envelope!).toBe(false);
    }
  });

  it('keeps wakeOnFill, wakeOnFlag, partialOnTimeout and filled independent', () => {
    const folded = foldBatchConfig({
      batch: ['a', 'b'],
      wakeOnFill: true,
      wakeOnFlag: true,
      partialOnTimeout: true,
      filled: { a: { payload: { ok: true }, at: '2026-11-05T00:00:00.000Z' } },
    });
    expect(folded.envelope).toMatchObject({
      batch_wake_on_fill: true,
      batch_wake_on_flag: true,
      batch_partial_on_timeout: true,
      batch_items: { a: { ok: true } },
    });
    expect(folded.metadata!.batch_pending).toEqual(['b']);
  });

  it('rejects an undeclared key, a non-list value, and the opt-in without a batch', () => {
    expect(() => foldBatchConfig({ batch: ['a'], wakeOnFill: ['z'] })).toThrow(/not declared/);
    expect(() => foldBatchConfig({ batch: ['a'], wakeOnFill: [1 as any] })).toThrow(/not declared/);
    expect(() => foldBatchConfig({ batch: ['a'], wakeOnFill: 'a' as any })).toThrow(/boolean or a list/);
    expect(() => foldBatchConfig({ wakeOnFill: true })).toThrow(/requires a batch/);
  });

  it('reserves the envelope key at creation', () => {
    expect(() => foldBatchConfig({ batch: ['a'], envelope: { batch_wake_on_fill: true } })).toThrow(/reserved/);
  });

  it('routes the opt-in through foldEscalationConfig and rejects it elsewhere', () => {
    expect(foldEscalationConfig({ batch: ['a'], wakeOnFill: ['a'] }).envelope!.batch_wake_on_fill).toEqual(['a']);
    expect(() => foldEscalationConfig({ accumulate: { max: 2 }, wakeOnFill: true })).toThrow(/batch only/);
    expect(() => foldEscalationConfig({ wakeOnFill: true })).toThrow(/requires a batch/);
  });
});
