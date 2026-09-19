import { describe, it, expect } from 'vitest';

import {
  accumulatedCollectionSql,
  buildAccumulateStatement,
  buildRemoveStatement,
  resolvePayloadSql,
  resolveWakeMessageSql,
  timeoutPayloadSql,
  SqlParams,
} from '../../../../services/store/providers/postgres/escalation-accumulate';

const base = {
  namespace: 'durable',
  itemKey: 'order-1',
  payload: { weight: 2 },
  reciprocalPayload: null,
  metadata: null,
  actor: 'scanner-7',
  assertClaim: null,
  wakes: [],
  appId: 'durable',
  schemaName: 'durable',
};

describe('UNIT | store | accumulate SQL builders', () => {
  it('numbers parameters in insertion order', () => {
    const p = new SqlParams();
    expect(p.add('a')).toBe('$1');
    expect(p.add(null)).toBe('$2');
    expect(p.values).toEqual(['a', null]);
  });

  it('locks both rows in one ordered CTE and gates the write on both being eligible', () => {
    const { sql, values } = buildAccumulateStatement({
      ...base,
      primary: { kind: 'id', id: 'c-1' },
      reciprocal: { kind: 'signalKey', signalKey: 'member-sig' },
    });
    expect(sql.match(/FOR UPDATE/g)).toHaveLength(1);
    expect(sql).toContain('ORDER BY e.id\n        FOR UPDATE');
    expect(sql).toContain('(SELECT COUNT(*) FROM eligible) = $7::int');
    expect(sql).toContain('(SELECT COUNT(*) FROM target)   = $7::int');
    expect(values[6]).toBe(2);
    expect(values).toContain('c-1');
    expect(values).toContain('member-sig');
  });

  it('expects one row when no reciprocal is named and never selects a phantom', () => {
    const { sql, values } = buildAccumulateStatement({
      ...base,
      primary: { kind: 'signalKey', signalKey: 'sig-1' },
      reciprocal: null,
    });
    expect(values[6]).toBe(1);
    expect(sql).toContain('reciprocal_pick AS MATERIALIZED (SELECT NULL::uuid AS id WHERE FALSE)');
  });

  it('selects a metadata target by containment, role, and priority order', () => {
    const { sql, values } = buildAccumulateStatement({
      ...base,
      primary: { kind: 'metadata', key: 'binKey', value: 'b-9', roles: ['bin'] },
      reciprocal: null,
    });
    expect(sql).toContain("e.metadata @> $9::jsonb");
    // a facet selects among pending accumulator rows only
    expect(sql).toContain("e.metadata ? 'accumulate_count'");
    expect(sql).toContain("e.status = 'pending'");
    expect(sql).not.toContain("e.status IN ('pending', 'cancelled')");
    expect(sql).toContain('ORDER BY e.priority ASC, e.created_at ASC');
    expect(values[8]).toBe(JSON.stringify({ binKey: 'b-9' }));
    expect(values[9]).toEqual(['bin']);
  });

  it('writes one wake CTE per pre-built command, rewriting the payload slot from the row', () => {
    const { sql, values } = buildAccumulateStatement({
      ...base,
      primary: { kind: 'id', id: 'c-1' },
      reciprocal: { kind: 'id', id: 'm-1' },
      wakes: [
        { command: { forSignalKey: 'sig-c', message: '{"c":1}' }, resolutionJson: null },
        { command: { forSignalKey: 'sig-m', message: '{"m":1}' }, resolutionJson: '{"$resolution":{}}' },
      ],
    });
    expect(sql).toContain('wake_0 AS (');
    expect(sql).toContain('wake_1 AS (');
    expect(sql).toContain("jsonb_set($");
    expect(sql).toContain("w.status = 'resolved' AND w.signal_key = $");
    expect(sql).toContain('AS wake_counts');
    expect(values).toContain('sig-c');
    expect(values).toContain('{"$resolution":{}}');
  });

  it('completes only at max with resolveAtMax and delivers an ordered collection', () => {
    const { sql } = buildAccumulateStatement({ ...base, primary: { kind: 'id', id: 'c-1' }, reciprocal: null });
    expect(sql).toContain('s.max_items IS NOT NULL AND s.resolve_at_max');
    expect(sql).toContain("'$trigger', 'count'");
    expect(sql).toContain("ORDER BY (kv.value->>'at')::timestamptz, kv.key");
    expect(sql).toContain("'duplicate-item'");
    expect(sql).toContain("'full'");
    expect(sql).toContain("'not-accumulator'");
  });

  it('builds a removal that never resolves or wakes', () => {
    const { sql, values } = buildRemoveStatement({
      primary: { kind: 'id', id: 'c-1' },
      reciprocal: null,
      itemKey: 'order-1',
    });
    expect(sql).not.toContain("'resolved'");
    expect(sql).not.toContain('engine_streams');
    expect(sql).toContain("'item-absent'");
    expect(sql).toContain('s.items - s.item_key');
    expect(values[0]).toBe('order-1');
  });

  it('shapes the terminal-path payload expressions', () => {
    expect(resolvePayloadSql('e', '$2')).toContain("'$trigger', 'resolve'");
    expect(resolvePayloadSql('e', '$2')).toContain("ELSE $2::jsonb END");
    expect(timeoutPayloadSql('e')).toContain("'$trigger', 'timeout'");
    expect(timeoutPayloadSql('e')).toContain("batch_partial_on_timeout");
    expect(timeoutPayloadSql('e')).toContain('ELSE e.resolver_payload END');
    expect(resolveWakeMessageSql('resolved', '$5', '$8')).toContain("jsonb_set($5::jsonb, '{data,data}'");
    expect(accumulatedCollectionSql("e.envelope->'accumulate_items'", 'count')).toContain("'$accumulated'");
  });
});
