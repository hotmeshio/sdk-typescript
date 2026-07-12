/**
 * Proves escalations.resolveAllOrNone(): atomic bulk resolve with per-row
 * payloads. A gang of condition()-parked workflows is claimed and then
 * mandated in ONE statement — every member receives its own resolverPayload
 * as condition()'s return value, or nothing resolves at all. The failure
 * shape names exactly the rows that blocked the batch.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor, uuid } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { ResolveAllOrNoneResult } from '../../../types/hmsh_escalations';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

// asserts the batch was blocked AND narrows to the failure member
function expectBlocked(
  result: ResolveAllOrNoneResult,
): asserts result is Extract<ResolveAllOrNoneResult, { ok: false }> {
  expect(result.ok).toBe(false);
}

describe('DURABLE | escalations.resolveAllOrNone | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const gangId = guid();
  const units = ['left-a', 'right-a', 'left-b'];
  const handles = new Map<string, WorkflowHandleService>();

  // condition() writes each row inside its Leg1 checkpoint — poll until the
  // whole gang is parked rather than guessing a fixed delay
  const awaitGangRows = async (gang: string, count: number, timeoutMs = 30_000) => {
    const deadline = Date.now() + timeoutMs;
    let rows: Awaited<ReturnType<typeof client.escalations.list>> = [];
    while (Date.now() < deadline) {
      rows = await client.escalations.list({
        role: 'printer',
        status: 'pending',
        metadata: { gangId: gang },
      });
      if (rows.length >= count) return rows;
      await sleepFor(250);
    }
    return rows;
  };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    client = new Client({ connection });
    for (const unitId of units) {
      const handle = await client.workflow.start({
        args: [gangId, unitId],
        taskQueue: 'gang-test',
        workflowName: 'gangMemberWorkflow',
        workflowId: guid(),
      });
      handles.set(unitId, handle);
    }
    const worker = await Worker.create({
      connection,
      taskQueue: 'gang-test',
      workflow: workflows.gangMemberWorkflow,
    });
    await worker.run();
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('gang handoff (golden path)', () => {
    it('resolves every member with its own payload and wakes each workflow', async () => {
      const rows = await awaitGangRows(gangId, units.length);
      expect(rows.length).toBe(units.length);

      // the broker claims the gang, then hands each member its own mandate
      const claimed = await client.escalations.claimMany({
        ids: rows.map((r) => r.id),
        assignee: 'broker-1',
        durationMinutes: 5,
      });
      expect(claimed.claimed).toBe(units.length);

      const items = rows.map((r) => ({
        id: r.id,
        resolverPayload: {
          gcodeRef: `gcode-${(r.metadata as any).unitId}`,
          unitId: (r.metadata as any).unitId,
        },
      }));
      const result = await client.escalations.resolveAllOrNone({
        items,
        assertAssignee: 'broker-1',
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entries.length).toBe(units.length);

      // each row stored ITS OWN payload as its audit record
      for (const entry of result.entries) {
        const unitId = (entry.metadata as any).unitId;
        expect(entry.status).toBe('resolved');
        expect((entry.resolver_payload as any).unitId).toBe(unitId);
        expect((entry.resolver_payload as any).gcodeRef).toBe(`gcode-${unitId}`);
      }

      // each parked workflow woke with ITS OWN payload as condition()'s return
      for (const unitId of units) {
        const payload = (await handles.get(unitId)!.result()) as Record<string, unknown>;
        expect(payload.unitId).toBe(unitId);
        expect(payload.gcodeRef).toBe(`gcode-${unitId}`);
      }
    }, 60_000);
  });

  describe('all-or-none gate', () => {
    it('an already-terminal member blocks the batch; resolvable members stay pending', async () => {
      const gangB = guid();
      const bUnits = ['b1', 'b2'];
      const bHandles = new Map<string, WorkflowHandleService>();
      for (const unitId of bUnits) {
        bHandles.set(unitId, await client.workflow.start({
          args: [gangB, unitId],
          taskQueue: 'gang-test',
          workflowName: 'gangMemberWorkflow',
          workflowId: guid(),
        }));
      }
      const rows = await awaitGangRows(gangB, 2);
      expect(rows.length).toBe(2);
      const b1 = rows.find((r) => (r.metadata as any).unitId === 'b1')!;
      const b2 = rows.find((r) => (r.metadata as any).unitId === 'b2')!;

      // b1 drifts out from under the gang (cancelled mid-claim)
      const cancelled = await client.escalations.cancel(b1.id);
      expect(cancelled.ok).toBe(true);

      const blocked = await client.escalations.resolveAllOrNone({
        items: [
          { id: b1.id, resolverPayload: { unitId: 'b1' } },
          { id: b2.id, resolverPayload: { unitId: 'b2' } },
        ],
      });
      expectBlocked(blocked);
      // only the true blocker is reported — b2 was resolvable and is not listed
      expect(blocked.failed).toEqual([{ id: b1.id, reason: 'already-cancelled' }]);

      // b2 was NOT resolved: the gang is intact for re-ganging
      const b2Row = await client.escalations.get(b2.id);
      expect(b2Row?.status).toBe('pending');

      // re-gang: resolving b2 alone succeeds and wakes its workflow
      const regang = await client.escalations.resolveAllOrNone({
        items: [{ id: b2.id, resolverPayload: { unitId: 'b2', regang: true } }],
      });
      expect(regang.ok).toBe(true);
      const b2Payload = (await bHandles.get('b2')!.result()) as Record<string, unknown>;
      expect(b2Payload.regang).toBe(true);
      // the cancelled member's workflow resumed with null
      expect(await bHandles.get('b1')!.result()).toBeNull();
    }, 60_000);

    it('an unknown id blocks the batch with not-found', async () => {
      const row = await client.escalations.create({
        type: 'printer-availability',
        role: 'printer',
        metadata: { probe: `nf-${guid()}` },
      });
      const missing = uuid();
      const blocked = await client.escalations.resolveAllOrNone({
        items: [
          { id: row.id, resolverPayload: { ok: true } },
          { id: missing, resolverPayload: { ok: true } },
        ],
      });
      expectBlocked(blocked);
      expect(blocked.failed).toEqual([{ id: missing, reason: 'not-found' }]);
      expect((await client.escalations.get(row.id))?.status).toBe('pending');
    }, 10_000);

    it('assertAssignee: a member held by another principal blocks the batch', async () => {
      const tag = `aa-${guid()}`;
      const mine = await client.escalations.create({ role: 'printer', metadata: { tag } });
      const theirs = await client.escalations.create({ role: 'printer', metadata: { tag } });
      await client.escalations.claimMany({ ids: [mine.id], assignee: 'broker-1', durationMinutes: 5 });
      await client.escalations.claimMany({ ids: [theirs.id], assignee: 'broker-2', durationMinutes: 5 });

      const blocked = await client.escalations.resolveAllOrNone({
        items: [
          { id: mine.id, resolverPayload: { n: 1 } },
          { id: theirs.id, resolverPayload: { n: 2 } },
        ],
        assertAssignee: 'broker-1',
      });
      expectBlocked(blocked);
      expect(blocked.failed).toEqual([{ id: theirs.id, reason: 'assignee-mismatch' }]);
      expect((await client.escalations.get(mine.id))?.status).toBe('pending');
      expect((await client.escalations.get(theirs.id))?.status).toBe('pending');

      // without the assertion, status is the only gate — both resolve
      const resolved = await client.escalations.resolveAllOrNone({
        items: [
          { id: mine.id, resolverPayload: { n: 1 } },
          { id: theirs.id, resolverPayload: { n: 2 } },
        ],
      });
      expect(resolved.ok).toBe(true);
    }, 10_000);
  });

  describe('standalone rows and input contract', () => {
    it('per-row payloads and a shared metadata patch land in one statement', async () => {
      const tag = `meta-${guid()}`;
      const a = await client.escalations.create({ role: 'printer', metadata: { tag, unit: 'a' } });
      const b = await client.escalations.create({ role: 'printer', metadata: { tag, unit: 'b' } });
      const result = await client.escalations.resolveAllOrNone({
        items: [
          { id: a.id, resolverPayload: { mandate: 'a' } },
          { id: b.id, resolverPayload: { mandate: 'b' } },
        ],
        metadata: { sweptBy: 'gang-test' },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      const byId = new Map(result.entries.map((e) => [e.id, e]));
      expect((byId.get(a.id)!.resolver_payload as any).mandate).toBe('a');
      expect((byId.get(b.id)!.resolver_payload as any).mandate).toBe('b');
      for (const entry of result.entries) {
        expect((entry.metadata as any).tag).toBe(tag); // creation key preserved
        expect((entry.metadata as any).sweptBy).toBe('gang-test'); // patch merged
      }
    }, 10_000);

    it('empty items returns ok with no entries', async () => {
      const result = await client.escalations.resolveAllOrNone({ items: [] });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entries).toEqual([]);
    }, 5_000);

    it('duplicate ids throw', async () => {
      const row = await client.escalations.create({ role: 'printer' });
      await expect(
        client.escalations.resolveAllOrNone({
          items: [
            { id: row.id, resolverPayload: { n: 1 } },
            { id: row.id, resolverPayload: { n: 2 } },
          ],
        }),
      ).rejects.toThrow(/duplicate/);
    }, 5_000);
  });

  describe('concurrency', () => {
    it('overlapping batches: exactly one wins; the loser writes nothing', async () => {
      const tag = `race-${guid()}`;
      const [a, b, c] = await Promise.all([
        client.escalations.create({ role: 'printer', metadata: { tag, unit: 'a' } }),
        client.escalations.create({ role: 'printer', metadata: { tag, unit: 'b' } }),
        client.escalations.create({ role: 'printer', metadata: { tag, unit: 'c' } }),
      ]);
      // both batches want row b — deterministic lock order serializes them;
      // whichever commits first resolves b and blocks the other entirely
      const [r1, r2] = await Promise.all([
        client.escalations.resolveAllOrNone({
          items: [
            { id: a.id, resolverPayload: { batch: 1 } },
            { id: b.id, resolverPayload: { batch: 1 } },
          ],
        }),
        client.escalations.resolveAllOrNone({
          items: [
            { id: b.id, resolverPayload: { batch: 2 } },
            { id: c.id, resolverPayload: { batch: 2 } },
          ],
        }),
      ]);
      const outcomes = [r1, r2];
      const winners = outcomes.filter((r) => r.ok);
      const losers = outcomes.filter((r) => !r.ok);
      expect(winners.length).toBe(1);
      expect(losers.length).toBe(1);
      const loser = losers[0];
      expectBlocked(loser);
      expect(loser.failed).toEqual([{ id: b.id, reason: 'already-resolved' }]);

      // the contested row carries the winner's payload; the loser's other
      // member is untouched
      const winnerBatch = r1.ok ? 1 : 2;
      const loserOnlyId = winnerBatch === 1 ? c.id : a.id;
      const bRow = await client.escalations.get(b.id);
      expect((bRow?.resolver_payload as any).batch).toBe(winnerBatch);
      expect((await client.escalations.get(loserOnlyId))?.status).toBe('pending');
    }, 15_000);
  });
});
