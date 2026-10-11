import { describe, it, expect } from 'vitest';

import { foldBatchConfig } from '../../../../services/escalations/batch';
import { foldEscalationConfig } from '../../../../services/escalations/fold';

const at = '2026-01-02T03:04:05Z';

describe('UNIT | escalations | foldBatchConfig filled', () => {
  it('starts the declared keys satisfied with payload, time, and actor', () => {
    const folded = foldBatchConfig({
      batch: ['a', 'b', 'c'],
      metadata: { orderId: 'o-1' },
      filled: {
        a: { payload: { x: 1 }, at, actor: 'u-1' },
        c: { payload: { x: 3 }, at },
      },
    });
    expect((folded as any).filled).toBeUndefined();
    expect(folded.metadata).toEqual({
      orderId: 'o-1',
      batch_pending: ['b'],
      batch_count: 1,
      batch_keys: ['a', 'b', 'c'],
    });
    expect(folded.envelope).toEqual({
      batch_items: { a: { x: 1 }, c: { x: 3 } },
      batch_filled_at: { a: '2026-01-02T03:04:05.000Z', c: '2026-01-02T03:04:05.000Z' },
      batch_filled_by: { a: 'u-1' },
    });
  });

  it('writes no batch_filled_by when no entry names an actor', () => {
    const folded = foldBatchConfig({ batch: ['a', 'b'], filled: { a: { payload: {}, at } } });
    expect('batch_filled_by' in folded.envelope!).toBe(false);
  });

  it('is deterministic for the replayed condition() path', () => {
    const config = { batch: ['a', 'b'], filled: { a: { payload: { k: 1 }, at, actor: 'u' } } };
    expect(foldBatchConfig(config)).toEqual(foldBatchConfig(config));
  });

  it('keeps the batch shape unchanged without filled', () => {
    expect(foldBatchConfig({ batch: ['a'] }).envelope).toEqual({ batch_items: {}, batch_filled_at: {} });
  });

  it.each([
    [{ batch: ['a'], filled: { a: { payload: {}, at } } }, /nothing to wait for/],
    [{ batch: ['a', 'b'], filled: { z: { payload: {}, at } } }, /not declared in batch/],
    [{ batch: ['a', 'b'], filled: [] }, /must be an object keyed/],
    [{ batch: ['a', 'b'], filled: { a: null } }, /must be an object/],
    [{ batch: ['a', 'b'], filled: { a: { at } } }, /payload must be an object/],
    [{ batch: ['a', 'b'], filled: { a: { payload: [], at } } }, /payload must be an object/],
    [{ batch: ['a', 'b'], filled: { a: { payload: {} } } }, /ISO-8601/],
    [{ batch: ['a', 'b'], filled: { a: { payload: {}, at: 'soon' } } }, /ISO-8601/],
    [{ batch: ['a', 'b'], filled: { a: { payload: {}, at, actor: '' } } }, /actor must be a non-empty string/],
    [{ batch: ['a', 'b'], envelope: { batch_filled_by: {} } }, /reserved/],
    [{ filled: { a: { payload: {}, at } } }, /requires a batch/],
  ])('rejects %j', (config, message) => {
    expect(() => foldBatchConfig(config as any)).toThrow(message);
  });
});

describe('UNIT | escalations | foldEscalationConfig filled', () => {
  it('rejects filled on an accumulator and without any declaration', () => {
    expect(() => foldEscalationConfig({ accumulate: {}, filled: { a: { payload: {}, at } } }))
      .toThrow(/batch only/);
    expect(() => foldEscalationConfig({ filled: { a: { payload: {}, at } } }))
      .toThrow(/requires a batch/);
  });
});
