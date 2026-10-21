import {
  ESCALATION_BATCH_COUNT_KEY,
  ESCALATION_BATCH_FLAGS_KEY,
  ESCALATION_BATCH_ITEMS_KEY,
  ESCALATION_BATCH_KEYS_KEY,
  ESCALATION_BATCH_PENDING_KEY,
  ESCALATION_BATCH_WAKE_ON_FLAG_KEY,
  ESCALATION_FLAGS_KEY,
  ESCALATION_TRIGGER_KEY,
  FlagBatchItemResult,
  UnflagBatchItemResult,
} from '../../../../types/hmsh_escalations';

import { AccumulateWake, SqlParams } from './escalation-accumulate';

/**
 * SQL builders for the batch flag store operations. Each builder is a pure
 * function of its input; the provider owns the connection. One statement
 * locks the row, writes only while the row and the key are pending, and
 * classifies the outcome. A flag never changes `batch_pending`, so every
 * fill and completion path reads the row as before.
 */

export type BatchRowSelector =
  | { kind: 'id'; id: string }
  | { kind: 'signalKey'; signalKey: string };

/** The flag value as the caller supplies it; the statement stamps `at`. */
export interface BatchFlagValue {
  label: string;
  payload?: Record<string, unknown>;
  actor?: string;
}

export interface FlagStatementInput {
  selector: BatchRowSelector;
  namespace?: string;
  itemKey: string;
  flag: BatchFlagValue;
  metadata: Record<string, unknown> | null;
  /** Present when the row declared `wakeOnFlag`; fires only if the row resolves. */
  wake: AccumulateWake | null;
  appId: string;
  schemaName: string;
}

export interface UnflagStatementInput {
  selector: BatchRowSelector;
  namespace?: string;
  itemKey: string;
}

/** A provider result: the public result plus what the client needs to settle a wake. */
export type FlagRowResult = FlagBatchItemResult & {
  signalKey?: string | null;
  topic?: string | null;
  wakeEnqueued?: boolean;
};

const FLAGS = ESCALATION_BATCH_FLAGS_KEY;
const ITEMS = ESCALATION_BATCH_ITEMS_KEY;
const PENDING = ESCALATION_BATCH_PENDING_KEY;
const KEYS = ESCALATION_BATCH_KEYS_KEY;
const COUNT = ESCALATION_BATCH_COUNT_KEY;
const WAKE_ON_FLAG = ESCALATION_BATCH_WAKE_ON_FLAG_KEY;

function targetCte(
  selector: BatchRowSelector,
  namespace: string | undefined,
  p: SqlParams,
): string {
  const column = selector.kind === 'id' ? 'id' : 'signal_key';
  const value = p.add(
    selector.kind === 'id' ? selector.id : selector.signalKey,
  );
  const ns = namespace ? ` AND namespace = ${p.add(namespace)}` : '';
  return `target AS MATERIALIZED (
        SELECT id, signal_key, topic, status,
               metadata->'${KEYS}'    AS batch_keys,
               metadata->'${PENDING}' AS batch_pending,
               envelope->'${FLAGS}'   AS batch_flags,
               COALESCE((envelope->>'${WAKE_ON_FLAG}')::boolean, false) AS wake_on_flag
        FROM public.hmsh_escalations
        WHERE ${column} = ${value}${ns}
        LIMIT 1 FOR UPDATE
      )`;
}

/** Outcome cases shared by both statements, after the success case. */
function refusalCases(key: string, fallback: string): string {
  return `WHEN t.id IS NULL THEN 'not-found'
          WHEN t.status <> 'pending' THEN 'blocked'
          WHEN jsonb_typeof(t.batch_pending) IS DISTINCT FROM 'array' THEN 'not-batch'
          WHEN NOT COALESCE(t.batch_keys ? ${key}::text, false) THEN 'unknown-item'
          ELSE '${fallback}'`;
}

/**
 * Flag one pending batch key. The flag lands in `envelope.batch_flags`
 * beside the items, stamped with the database clock. On a row that
 * declared `wakeOnFlag` the same UPDATE resolves the row with the items
 * filled so far, `$flags`, and `$trigger: 'flag'`, and the wake commits
 * with it. A key already filled, a row no longer pending, or a race lost to
 * the completing fill writes nothing.
 */
export function buildFlagStatement(input: FlagStatementInput): {
  sql: string;
  values: unknown[];
} {
  const p = new SqlParams();
  const target = targetCte(input.selector, input.namespace, p);
  const key = p.add(input.itemKey);
  const flag = p.add(JSON.stringify(input.flag));
  const meta = p.add(input.metadata ? JSON.stringify(input.metadata) : null);
  const flags = `(COALESCE(e.envelope->'${FLAGS}', '{}'::jsonb)
                 || jsonb_build_object(${key}::text,
                      ${flag}::jsonb || jsonb_build_object('at', to_jsonb(NOW()))))`;
  const wake = wakeCte(input.wake, input.appId, input.schemaName, p);
  const sql = `
      WITH ${target},
      written AS (
        UPDATE public.hmsh_escalations e
        SET envelope = COALESCE(e.envelope, '{}'::jsonb)
                       || jsonb_build_object('${FLAGS}', ${flags}),
            metadata = COALESCE(e.metadata, '{}'::jsonb)
                       || COALESCE(${meta}::jsonb, '{}'::jsonb)
                       || jsonb_build_object(
                            '${PENDING}', e.metadata->'${PENDING}',
                            '${COUNT}',   e.metadata->'${COUNT}',
                            '${KEYS}',    e.metadata->'${KEYS}'),
            status = CASE WHEN t.wake_on_flag THEN 'resolved' ELSE e.status END,
            resolved_at = CASE WHEN t.wake_on_flag THEN NOW() ELSE e.resolved_at END,
            resolver_payload = CASE WHEN t.wake_on_flag
              THEN COALESCE(e.envelope->'${ITEMS}', '{}'::jsonb)
                   || jsonb_build_object('${ESCALATION_FLAGS_KEY}', ${flags},
                                         '${ESCALATION_TRIGGER_KEY}', 'flag')
              ELSE e.resolver_payload END,
            updated_at = NOW()
        FROM target t
        WHERE e.id = t.id
          AND t.status = 'pending'
          AND jsonb_typeof(t.batch_pending) = 'array'
          AND t.batch_pending ? ${key}::text
        RETURNING e.*
      )${wake.cte}
      SELECT t.id, t.status AS prior_status, t.signal_key, t.topic,
        CASE
          WHEN w.id IS NOT NULL AND w.status = 'resolved' THEN 'completed'
          WHEN w.id IS NOT NULL THEN 'flagged'
          ${refusalCases(key, 'already-filled')}
        END AS outcome,
        row_to_json(w.*) AS entry_json,
        ${wake.count}
      FROM (SELECT * FROM target) t
      FULL OUTER JOIN (SELECT * FROM written) w ON w.id = t.id`;
  return { sql, values: p.values };
}

/**
 * Clear one key's flag while the row is pending. Removes only that key's
 * entry; the row's status, items and pending keys are untouched.
 */
export function buildUnflagStatement(input: UnflagStatementInput): {
  sql: string;
  values: unknown[];
} {
  const p = new SqlParams();
  const target = targetCte(input.selector, input.namespace, p);
  const key = p.add(input.itemKey);
  const sql = `
      WITH ${target},
      written AS (
        UPDATE public.hmsh_escalations e
        SET envelope = e.envelope
                       || jsonb_build_object('${FLAGS}', (e.envelope->'${FLAGS}') - ${key}::text),
            updated_at = NOW()
        FROM target t
        WHERE e.id = t.id
          AND t.status = 'pending'
          AND jsonb_typeof(t.batch_pending) = 'array'
          AND jsonb_typeof(t.batch_flags) = 'object'
          AND t.batch_flags ? ${key}::text
        RETURNING e.*
      )
      SELECT t.id, t.status AS prior_status,
        CASE
          WHEN w.id IS NOT NULL THEN 'unflagged'
          ${refusalCases(key, 'not-flagged')}
        END AS outcome,
        row_to_json(w.*) AS entry_json
      FROM (SELECT * FROM target) t
      FULL OUTER JOIN (SELECT * FROM written) w ON w.id = t.id`;
  return { sql, values: p.values };
}

function wakeCte(
  wake: AccumulateWake | null,
  appId: string,
  schemaName: string,
  p: SqlParams,
): { cte: string; count: string } {
  if (!wake) return { cte: '', count: '0::int AS wake_count' };
  const app = p.add(appId);
  const msg = p.add(wake.command.message);
  const sk = p.add(wake.command.forSignalKey);
  const res = p.add(wake.resolutionJson);
  return {
    cte: `,
      wake AS (
        INSERT INTO ${schemaName}.engine_streams (stream_name, message, priority)
        SELECT ${app},
               jsonb_set(${msg}::jsonb, '{data,data}',
                         w.resolver_payload::jsonb || COALESCE(${res}::jsonb, '{}'::jsonb))::text,
               5
        FROM written w
        WHERE w.status = 'resolved' AND w.signal_key = ${sk}
        RETURNING id
      )`,
    count: '(SELECT COUNT(*) FROM wake)::int AS wake_count',
  };
}

/** `blocked` names the row's prior terminal status. */
function blockedOutcome(
  priorStatus: string | null | undefined,
): 'already-resolved' | 'already-cancelled' | 'already-expired' {
  if (priorStatus === 'cancelled') return 'already-cancelled';
  if (priorStatus === 'expired') return 'already-expired';
  return 'already-resolved';
}

export function mapFlagRow(row: any): FlagRowResult {
  if (!row || row.outcome === 'not-found')
    return { ok: false, outcome: 'not-found' };
  if (row.outcome === 'flagged' || row.outcome === 'completed') {
    return {
      ok: true,
      outcome: row.outcome,
      entry: row.entry_json,
      signalKey: row.signal_key,
      topic: row.topic,
      wakeEnqueued: row.wake_count > 0,
    };
  }
  if (row.outcome === 'blocked')
    return { ok: false, outcome: blockedOutcome(row.prior_status) };
  return { ok: false, outcome: row.outcome };
}

export function mapUnflagRow(row: any): UnflagBatchItemResult {
  if (!row || row.outcome === 'not-found')
    return { ok: false, outcome: 'not-found' };
  if (row.outcome === 'unflagged')
    return { ok: true, outcome: 'unflagged', entry: row.entry_json };
  if (row.outcome === 'blocked')
    return { ok: false, outcome: blockedOutcome(row.prior_status) };
  return { ok: false, outcome: row.outcome };
}
