import { describe, it, expect } from 'vitest';

import {
  buildFlagStatement,
  buildUnflagStatement,
  mapFlagRow,
  mapUnflagRow,
} from '../../../../services/store/providers/postgres/escalation-batch-flag';

const flagBase = {
  selector: { kind: 'id' as const, id: 'e-1' },
  itemKey: 'shoe#1',
  flag: { label: 'Missing', payload: { note: 'not on pallet' }, actor: 'u-1' },
  metadata: null,
  wake: null,
  appId: 'durable',
  schemaName: 'durable',
};

const wake = {
  command: { forSignalKey: 'sig-1', message: '{"data":{"id":"sig-1","data":{}}}' },
  resolutionJson: '{"$resolution":{"escalationId":"e-1","resolvedBy":"u-1"}}',
};

describe('UNIT | store | batch flag SQL builders', () => {
  it('locks one row and binds the selector, key, flag and metadata in order', () => {
    const { sql, values } = buildFlagStatement({ ...flagBase, metadata: { outcome: 'missing' } });
    expect(sql.match(/FOR UPDATE/g)).toHaveLength(1);
    expect(sql).toContain('WHERE id = $1');
    expect(values).toEqual([
      'e-1',
      'shoe#1',
      JSON.stringify(flagBase.flag),
      JSON.stringify({ outcome: 'missing' }),
    ]);
  });

  it('selects by signal key and namespace', () => {
    const { sql, values } = buildFlagStatement({
      ...flagBase,
      selector: { kind: 'signalKey', signalKey: 'sig-1' },
      namespace: 'ns',
    });
    expect(sql).toContain('WHERE signal_key = $1 AND namespace = $2');
    expect(values.slice(0, 3)).toEqual(['sig-1', 'ns', 'shoe#1']);
  });

  it('writes only while the row is pending and the key is still pending', () => {
    const { sql } = buildFlagStatement(flagBase);
    expect(sql).toContain("t.status = 'pending'");
    expect(sql).toContain("jsonb_typeof(t.batch_pending) = 'array'");
    expect(sql).toContain('t.batch_pending ? $2::text');
  });

  it('stamps the database clock and writes one key of batch_flags', () => {
    const { sql } = buildFlagStatement(flagBase);
    expect(sql).toContain("COALESCE(e.envelope->'batch_flags', '{}'::jsonb)");
    expect(sql).toContain("jsonb_build_object('at', to_jsonb(NOW()))");
  });

  it('never rewrites batch_pending and re-asserts the reserved facets after the patch', () => {
    const { sql } = buildFlagStatement(flagBase);
    expect(sql).not.toMatch(/batch_pending'\)\s*-/);
    expect(sql).toMatch(/'batch_pending', e\.metadata->'batch_pending'/);
    expect(sql).toMatch(/'batch_keys',\s+e\.metadata->'batch_keys'/);
  });

  it('resolves with items, $flags and $trigger only on a wakeOnFlag row', () => {
    const { sql } = buildFlagStatement(flagBase);
    expect(sql).toContain("status = CASE WHEN t.wake_on_flag THEN 'resolved' ELSE e.status END");
    expect(sql).toContain("COALESCE(e.envelope->'batch_items', '{}'::jsonb)");
    expect(sql).toContain("'$flags'");
    expect(sql).toContain("'$trigger', 'flag'");
    expect(sql).toContain("COALESCE((envelope->>'batch_wake_on_flag')::boolean, false)");
  });

  it('adds no wake without a command', () => {
    const { sql } = buildFlagStatement(flagBase);
    expect(sql).not.toContain('engine_streams');
    expect(sql).toContain('0::int AS wake_count');
  });

  it('enqueues the wake only from a resolved row with the matching signal key', () => {
    const { sql, values } = buildFlagStatement({ ...flagBase, wake });
    expect(sql).toContain('INSERT INTO durable.engine_streams');
    expect(sql).toContain("WHERE w.status = 'resolved' AND w.signal_key = $7");
    expect(values.slice(4)).toEqual(['durable', wake.command.message, 'sig-1', wake.resolutionJson]);
  });

  it('classifies every refusal after the success cases', () => {
    const { sql } = buildFlagStatement(flagBase);
    for (const outcome of ['completed', 'flagged', 'not-found', 'blocked', 'not-batch', 'unknown-item', 'already-filled']) {
      expect(sql).toContain(`'${outcome}'`);
    }
  });

  it('unflag removes one key while the row is pending and the key is flagged', () => {
    const { sql, values } = buildUnflagStatement({ selector: { kind: 'id', id: 'e-1' }, itemKey: 'shoe#1' });
    expect(values).toEqual(['e-1', 'shoe#1']);
    expect(sql).toContain("(e.envelope->'batch_flags') - $2::text");
    expect(sql).toContain("t.status = 'pending'");
    expect(sql).toContain('t.batch_flags ? $2::text');
    const setClause = sql.slice(sql.indexOf('SET'), sql.indexOf('FROM target'));
    expect(setClause).not.toContain('status');
    expect(setClause).not.toContain('resolver_payload');
    expect(sql).toContain("'not-flagged'");
  });
});

describe('UNIT | store | batch flag result mapping', () => {
  const entry = { id: 'e-1' };

  it('maps flagged and completed with the wake facts', () => {
    expect(mapFlagRow({ outcome: 'flagged', entry_json: entry, signal_key: 's', topic: 't', wake_count: 0 }))
      .toEqual({ ok: true, outcome: 'flagged', entry, signalKey: 's', topic: 't', wakeEnqueued: false });
    expect(mapFlagRow({ outcome: 'completed', entry_json: entry, signal_key: 's', topic: 't', wake_count: 1 }))
      .toMatchObject({ ok: true, outcome: 'completed', wakeEnqueued: true });
  });

  it('names the prior terminal status of a blocked row', () => {
    expect(mapFlagRow({ outcome: 'blocked', prior_status: 'resolved' })).toEqual({ ok: false, outcome: 'already-resolved' });
    expect(mapFlagRow({ outcome: 'blocked', prior_status: 'cancelled' })).toEqual({ ok: false, outcome: 'already-cancelled' });
    expect(mapUnflagRow({ outcome: 'blocked', prior_status: 'expired' })).toEqual({ ok: false, outcome: 'already-expired' });
  });

  it('maps a missing row and passes other refusals through', () => {
    expect(mapFlagRow(undefined)).toEqual({ ok: false, outcome: 'not-found' });
    expect(mapFlagRow({ outcome: 'already-filled' })).toEqual({ ok: false, outcome: 'already-filled' });
    expect(mapUnflagRow({ outcome: 'unflagged', entry_json: entry })).toEqual({ ok: true, outcome: 'unflagged', entry });
    expect(mapUnflagRow({ outcome: 'not-flagged' })).toEqual({ ok: false, outcome: 'not-flagged' });
  });
});
