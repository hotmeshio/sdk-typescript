import {
  ESCALATION_ACCUMULATE_CONFIG_KEY,
  ESCALATION_ACCUMULATE_COUNT_KEY,
  ESCALATION_ACCUMULATE_ITEMS_KEY,
  ESCALATION_ACCUMULATE_KEYS_KEY,
  ESCALATION_ACCUMULATE_MAX_KEY,
  ESCALATION_ACCUMULATED_KEY,
  ESCALATION_BATCH_ITEMS_KEY,
  ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY,
  ESCALATION_TRIGGER_KEY,
  AccumulatorTrigger,
  EscalationWakeCommand,
} from '../../../../types/hmsh_escalations';

/**
 * SQL builders for the accumulator store operations. Every builder is a
 * pure function of parameter positions; the provider owns the connection
 * and the result mapping. Two rows (container and reciprocal) are locked
 * in one ordered CTE and written by one UPDATE gated on both being
 * eligible, so a reciprocal add is both-or-neither by construction.
 */

/** Collects statement parameters and hands back their `$n` placeholders. */
export class SqlParams {
  readonly values: unknown[] = [];
  add(value: unknown): string {
    this.values.push(value);
    return `$${this.values.length}`;
  }
}

export type AccumulateRowSelector =
  | { kind: 'id'; id: string }
  | { kind: 'signalKey'; signalKey: string }
  | { kind: 'metadata'; key: string; value: unknown; roles?: string[] };

export interface AccumulateWake {
  command: EscalationWakeCommand;
  /** Pre-serialized `$resolution` object merged into the delivered
   * collection only, never into the stored `resolver_payload`. */
  resolutionJson: string | null;
}

const ITEMS = ESCALATION_ACCUMULATE_ITEMS_KEY;
const CONFIG = ESCALATION_ACCUMULATE_CONFIG_KEY;
const KEYS = ESCALATION_ACCUMULATE_KEYS_KEY;
const COUNT = ESCALATION_ACCUMULATE_COUNT_KEY;
const MAX = ESCALATION_ACCUMULATE_MAX_KEY;

/** True when the row's envelope carries an accumulator item store. */
export function isAccumulatorSql(envelopeExpr: string): string {
  return `COALESCE(jsonb_typeof(${envelopeExpr}->'${ITEMS}') = 'object', false)`;
}

/**
 * The delivered collection for an item store expression: `$accumulated`
 * ordered by each entry's database-clock `at`, plus the `$trigger` that
 * ended the wait.
 */
export function accumulatedCollectionSql(
  itemsExpr: string,
  trigger: AccumulatorTrigger,
): string {
  return `jsonb_build_object(
    '${ESCALATION_ACCUMULATED_KEY}',
    (SELECT COALESCE(jsonb_agg(jsonb_build_object('itemKey', kv.key) || kv.value
                              ORDER BY (kv.value->>'at')::timestamptz, kv.key), '[]'::jsonb)
     FROM jsonb_each(${itemsExpr}) AS kv),
    '${ESCALATION_TRIGGER_KEY}', '${trigger}')`;
}

/**
 * The stored `resolver_payload` for a manual resolve: the collection merged
 * with the resolver's payload on an accumulator row, the payload alone on
 * any other row.
 */
export function resolvePayloadSql(
  rowAlias: string,
  payloadParam: string,
): string {
  return `CASE WHEN ${isAccumulatorSql(`${rowAlias}.envelope`)}
      THEN ${accumulatedCollectionSql(`${rowAlias}.envelope->'${ITEMS}'`, 'resolve')}
           || COALESCE(${payloadParam}::jsonb, '{}'::jsonb)
      ELSE ${payloadParam}::jsonb END`;
}

/**
 * The `resolver_payload` written by the timeout path: an accumulator
 * delivers its collection, a batch row with `partialOnTimeout` delivers the
 * items filled so far, every other row keeps its column untouched.
 */
export function timeoutPayloadSql(rowAlias: string): string {
  const env = `${rowAlias}.envelope`;
  return `CASE
      WHEN ${isAccumulatorSql(env)}
        THEN ${accumulatedCollectionSql(`${env}->'${ITEMS}'`, 'timeout')}
      WHEN jsonb_typeof(${env}->'${ESCALATION_BATCH_ITEMS_KEY}') = 'object'
       AND COALESCE((${env}->>'${ESCALATION_BATCH_PARTIAL_ON_TIMEOUT_KEY}')::boolean, false)
        THEN (${env}->'${ESCALATION_BATCH_ITEMS_KEY}')
             || jsonb_build_object('${ESCALATION_TRIGGER_KEY}', 'timeout')
      ELSE ${rowAlias}.resolver_payload END`;
}

/**
 * The wake message for a resolve statement: on an accumulator row the
 * `{data,data}` slot is rewritten from the committed `resolver_payload`
 * (plus `$resolution`), so the waiter receives exactly what the row stores.
 */
export function resolveWakeMessageSql(
  fromCTE: string,
  messageParam: string,
  resolutionParam: string,
): string {
  return `CASE WHEN ${isAccumulatorSql(`${fromCTE}.envelope`)}
      THEN jsonb_set(${messageParam}::jsonb, '{data,data}',
                     ${fromCTE}.resolver_payload::jsonb || COALESCE(${resolutionParam}::jsonb, '{}'::jsonb))::text
      ELSE ${messageParam}::text END`;
}

function claimPredicateSql(alias: string, claimParam: string): string {
  return `(${claimParam}::text IS NULL
       OR ${alias}.assigned_to IS NULL
       OR ${alias}.assigned_until IS NULL
       OR (${alias}.assigned_to =  ${claimParam}::text AND ${alias}.assigned_until >  NOW())
       OR (${alias}.assigned_to <> ${claimParam}::text AND ${alias}.assigned_until <= NOW()))`;
}

function pickCte(
  name: string,
  selector: AccumulateRowSelector | null,
  namespace: string | undefined,
  p: SqlParams,
): string {
  if (!selector)
    return `${name} AS MATERIALIZED (SELECT NULL::uuid AS id WHERE FALSE)`;
  const ns = namespace ? ` AND e.namespace = ${p.add(namespace)}` : '';
  const from = `FROM public.hmsh_escalations e`;
  switch (selector.kind) {
    case 'id':
      return `${name} AS MATERIALIZED (SELECT e.id ${from} WHERE e.id = ${p.add(selector.id)}${ns} LIMIT 1)`;
    case 'signalKey':
      return `${name} AS MATERIALIZED (SELECT e.id ${from} WHERE e.signal_key = ${p.add(selector.signalKey)}${ns} LIMIT 1)`;
    case 'metadata': {
      const filter = p.add(JSON.stringify({ [selector.key]: selector.value }));
      const roles = p.add(selector.roles ?? null);
      return `${name} AS MATERIALIZED (
        SELECT e.id ${from}
        WHERE e.metadata @> ${filter}::jsonb
          AND (${roles}::text[] IS NULL OR e.role = ANY(${roles}::text[]))
          AND e.status IN ('pending', 'cancelled')${ns}
        ORDER BY e.priority ASC, e.created_at ASC
        LIMIT 1)`;
    }
  }
}

/**
 * The shared head of both item statements: pick the container (and the
 * reciprocal), lock both in id order in ONE CTE, and derive each side's
 * item key, payload, and partner id. The reciprocal's item key is the
 * container's id; each side's `other_id` becomes its entry's
 * `reciprocalId`.
 */
function targetCtes(
  primary: AccumulateRowSelector,
  reciprocal: AccumulateRowSelector | null,
  namespace: string | undefined,
  p: SqlParams,
  itemKeyParam: string,
  payloadParam: string | null,
  reciprocalPayloadParam: string | null,
): string {
  const payload = payloadParam ? `${payloadParam}::jsonb` : 'NULL::jsonb';
  const rPayload = reciprocalPayloadParam
    ? `${reciprocalPayloadParam}::jsonb`
    : 'NULL::jsonb';
  return `
      ${pickCte('primary_pick', primary, namespace, p)},
      ${pickCte('reciprocal_pick', reciprocal, namespace, p)},
      target AS MATERIALIZED (
        SELECT e.id, e.signal_key, e.topic, e.status, e.assigned_to, e.assigned_until,
               e.metadata, e.envelope,
               (e.id = (SELECT id FROM primary_pick)) AS is_primary
        FROM public.hmsh_escalations e
        WHERE e.id IN (SELECT id FROM primary_pick UNION SELECT id FROM reciprocal_pick)
        ORDER BY e.id
        FOR UPDATE
      ),
      sides AS (
        SELECT t.*,
               CASE WHEN t.is_primary THEN ${itemKeyParam}::text
                    ELSE (SELECT p.id::text FROM target p WHERE p.is_primary) END AS item_key,
               CASE WHEN t.is_primary THEN ${payload} ELSE ${rPayload} END AS item_payload,
               (SELECT o.id::text FROM target o WHERE o.is_primary <> t.is_primary) AS other_id,
               COALESCE((t.envelope->'${CONFIG}'->>'unique')::boolean, true) AS is_unique,
               COALESCE((t.envelope->'${CONFIG}'->>'resolveAtMax')::boolean, true) AS resolve_at_max,
               (t.metadata->>'${MAX}')::int AS max_items,
               COALESCE(t.envelope->'${ITEMS}', '{}'::jsonb) AS items,
               COALESCE(t.metadata->'${KEYS}', '[]'::jsonb) AS keys
        FROM target t
      )`;
}

function wakeCtes(
  wakes: AccumulateWake[],
  appId: string,
  schemaName: string,
  p: SqlParams,
): { ctes: string; counts: string } {
  if (!wakes.length)
    return { ctes: '', counts: `ARRAY[]::int[] AS wake_counts` };
  const app = p.add(appId);
  const names: string[] = [];
  const ctes = wakes
    .map((wake, i) => {
      const name = `wake_${i}`;
      names.push(name);
      const msg = p.add(wake.command.message);
      const sk = p.add(wake.command.forSignalKey);
      const res = p.add(wake.resolutionJson);
      return `,
      ${name} AS (
        INSERT INTO ${schemaName}.engine_streams (stream_name, message, priority)
        SELECT ${app},
               jsonb_set(${msg}::jsonb, '{data,data}',
                         w.resolver_payload::jsonb || COALESCE(${res}::jsonb, '{}'::jsonb))::text,
               5
        FROM written w
        WHERE w.status = 'resolved' AND w.signal_key = ${sk}
        RETURNING id
      )`;
    })
    .join('');
  const counts = `ARRAY[${names.map((n) => `(SELECT COUNT(*) FROM ${n})::int`).join(', ')}] AS wake_counts`;
  return { ctes, counts };
}

export interface AccumulateStatementInput {
  primary: AccumulateRowSelector;
  reciprocal: AccumulateRowSelector | null;
  namespace?: string;
  itemKey: string;
  payload: Record<string, unknown> | null;
  reciprocalPayload: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
  actor: string | null;
  assertClaim: string | null;
  wakes: AccumulateWake[];
  appId: string;
  schemaName: string;
}

/**
 * One statement: guarded append of one item on the container (and the
 * container's id on the reciprocal), facet recompute, resolve-at-max with
 * the ordered collection as `resolver_payload`, per-side wake enqueue, and
 * per-side outcome classification. Both writes or neither.
 */
export function buildAccumulateStatement(input: AccumulateStatementInput): {
  sql: string;
  values: unknown[];
} {
  const p = new SqlParams();
  const itemKey = p.add(input.itemKey);
  const payload = p.add(
    input.payload === null ? null : JSON.stringify(input.payload),
  );
  const rPayload = p.add(
    input.reciprocalPayload === null
      ? null
      : JSON.stringify(input.reciprocalPayload),
  );
  const meta = p.add(input.metadata ? JSON.stringify(input.metadata) : null);
  const actor = p.add(input.actor);
  const claim = p.add(input.assertClaim);
  const expected = p.add(input.reciprocal ? 2 : 1);
  const head = targetCtes(
    input.primary,
    input.reciprocal,
    input.namespace,
    p,
    itemKey,
    payload,
    rPayload,
  );
  const { ctes: wakeSql, counts: wakeCounts } = wakeCtes(
    input.wakes,
    input.appId,
    input.schemaName,
    p,
  );

  const newKeys = `CASE WHEN s.keys ? s.item_key THEN s.keys ELSE s.keys || to_jsonb(s.item_key) END`;
  const newCount = `jsonb_array_length(${newKeys})`;
  const entry = `jsonb_build_object('at', to_jsonb(NOW()))
              || CASE WHEN s.item_payload IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('payload', s.item_payload) END
              || CASE WHEN ${actor}::text IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('actor', ${actor}::text) END
              || CASE WHEN s.other_id IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('reciprocalId', s.other_id) END`;
  const newItems = `s.items || jsonb_build_object(s.item_key, ${entry})`;
  const complete = `(s.max_items IS NOT NULL AND s.resolve_at_max AND ${newCount} >= s.max_items)`;
  const isFull = `(s.max_items IS NOT NULL AND NOT (s.items ? s.item_key) AND jsonb_array_length(s.keys) >= s.max_items)`;
  const isDuplicate = `(s.is_unique AND s.items ? s.item_key)`;

  const sql = `
      WITH ${head},
      eligible AS (
        SELECT s.id FROM sides s
        WHERE s.status = 'pending'
          AND ${isAccumulatorSql('s.envelope')}
          AND s.item_key IS NOT NULL
          AND (NOT s.is_primary OR ${claimPredicateSql('s', claim)})
          AND NOT ${isDuplicate}
          AND NOT ${isFull}
      ),
      written AS (
        UPDATE public.hmsh_escalations e
        SET envelope = COALESCE(e.envelope, '{}'::jsonb)
              || jsonb_build_object('${ITEMS}', ${newItems}),
            metadata = COALESCE(e.metadata, '{}'::jsonb)
              || CASE WHEN s.is_primary THEN COALESCE(${meta}::jsonb, '{}'::jsonb) ELSE '{}'::jsonb END
              || jsonb_build_object('${KEYS}', ${newKeys}, '${COUNT}', ${newCount}),
            status           = CASE WHEN ${complete} THEN 'resolved' ELSE e.status END,
            resolved_at      = CASE WHEN ${complete} THEN NOW() ELSE e.resolved_at END,
            resolver_payload = CASE WHEN ${complete}
                                    THEN ${accumulatedCollectionSql(newItems, 'count')}
                                    ELSE e.resolver_payload END,
            updated_at = NOW()
        FROM sides s
        WHERE e.id = s.id
          AND (SELECT COUNT(*) FROM eligible) = ${expected}::int
          AND (SELECT COUNT(*) FROM target)   = ${expected}::int
        RETURNING e.*
      )${wakeSql}
      SELECT s.id, s.is_primary, s.status AS prior_status, s.signal_key, s.topic, s.assigned_to,
        CASE
          WHEN w.id IS NOT NULL AND w.status = 'resolved' THEN 'completed'
          WHEN w.id IS NOT NULL                           THEN 'accepted'
          WHEN s.status <> 'pending'                      THEN 'blocked'
          WHEN NOT ${isAccumulatorSql('s.envelope')}      THEN 'not-accumulator'
          WHEN s.is_primary AND NOT ${claimPredicateSql('s', claim)} THEN 'claim-blocked'
          WHEN ${isDuplicate}                             THEN 'duplicate-item'
          WHEN ${isFull}                                  THEN 'full'
          ELSE 'gated'
        END AS outcome,
        row_to_json(w.*) AS entry_json,
        ${wakeCounts}
      FROM sides s
      LEFT JOIN written w ON w.id = s.id
      ORDER BY s.is_primary DESC`;
  return { sql, values: p.values };
}

export interface RemoveStatementInput {
  primary: AccumulateRowSelector;
  reciprocal: AccumulateRowSelector | null;
  namespace?: string;
  itemKey: string;
}

/**
 * One statement: guarded removal of one held item from the container (and
 * of the container's id from the reciprocal), facet recompute, no wake and
 * no status change. Both writes or neither.
 */
export function buildRemoveStatement(input: RemoveStatementInput): {
  sql: string;
  values: unknown[];
} {
  const p = new SqlParams();
  const itemKey = p.add(input.itemKey);
  const expected = p.add(input.reciprocal ? 2 : 1);
  const head = targetCtes(
    input.primary,
    input.reciprocal,
    input.namespace,
    p,
    itemKey,
    null,
    null,
  );
  const newKeys = `s.keys - s.item_key`;
  const sql = `
      WITH ${head},
      eligible AS (
        SELECT s.id FROM sides s
        WHERE s.status = 'pending'
          AND ${isAccumulatorSql('s.envelope')}
          AND s.item_key IS NOT NULL
          AND s.items ? s.item_key
      ),
      written AS (
        UPDATE public.hmsh_escalations e
        SET envelope = COALESCE(e.envelope, '{}'::jsonb)
              || jsonb_build_object('${ITEMS}', s.items - s.item_key),
            metadata = COALESCE(e.metadata, '{}'::jsonb)
              || jsonb_build_object('${KEYS}', ${newKeys}, '${COUNT}', jsonb_array_length(${newKeys})),
            updated_at = NOW()
        FROM sides s
        WHERE e.id = s.id
          AND (SELECT COUNT(*) FROM eligible) = ${expected}::int
          AND (SELECT COUNT(*) FROM target)   = ${expected}::int
        RETURNING e.*
      )
      SELECT s.id, s.is_primary, s.status AS prior_status,
        CASE
          WHEN w.id IS NOT NULL                       THEN 'removed'
          WHEN s.status <> 'pending'                  THEN 'blocked'
          WHEN NOT ${isAccumulatorSql('s.envelope')}  THEN 'not-accumulator'
          WHEN NOT (s.items ? s.item_key)             THEN 'item-absent'
          ELSE 'gated'
        END AS outcome,
        row_to_json(w.*) AS entry_json
      FROM sides s
      LEFT JOIN written w ON w.id = s.id
      ORDER BY s.is_primary DESC`;
  return { sql, values: p.values };
}
