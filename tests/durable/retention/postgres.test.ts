/**
 * Proves the escalation retention contract: pruneEscalations deletes ONLY
 * terminal rows (resolved/cancelled/expired) older than the horizon.
 *
 * The contract the dependent platform builds on:
 * - old terminal rows are deleted (audit backlog ages out);
 * - pending rows survive regardless of age — a live waiter parked on
 *   condition() for months keeps its row and still resolves;
 * - fresh terminal rows inside the horizon survive;
 * - `limit` batches the delete and looping until 0 drains the backlog;
 * - `statuses` narrows which terminal states are pruned.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | retention | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const caseId = guid();

  const OLD = `NOW() - INTERVAL '120 days'`;

  const seedTerminal = async (status: string, count: number, backdated = true) => {
    const ids: string[] = [];
    for (let i = 0; i < count; i++) {
      const esc = await client.escalations.create({
        role: 'retention-seed',
        type: 'retention-audit',
        description: `${status} seed ${i}`,
        metadata: { seed: true },
      });
      ids.push(esc.id);
    }
    await postgresClient.query(
      `UPDATE public.hmsh_escalations
       SET status = $1, updated_at = ${backdated ? OLD : 'NOW()'}
       WHERE id = ANY($2::uuid[])`,
      [status, ids],
    );
    return ids;
  };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('setup', () => {
    it('should connect, start a live waiter, and process it to suspension', async () => {
      const conn = await Connection.connect(connection);
      expect(conn).toBeDefined();
      client = new Client({ connection });
      handle = await client.workflow.start({
        args: [caseId],
        taskQueue: 'retention-test',
        workflowName: 'retentionWaiter',
        workflowId: guid(),
      });
      const worker = await Worker.create({
        connection,
        taskQueue: 'retention-test',
        workflow: workflows.retentionWaiter,
      });
      await worker.run();
      await sleepFor(2000);
      const rows = await client.escalations.list({ role: 'retention-waiter', status: 'pending' });
      expect(rows.find((e) => (e.metadata as any)?.caseId === caseId)).toBeDefined();
    }, 20_000);
  });

  describe('prune contract', () => {
    it('should delete only terminal rows older than the horizon, batched by limit', async () => {
      // Backlog: 18 old terminal rows across all three terminal states.
      await seedTerminal('resolved', 8);
      await seedTerminal('cancelled', 5);
      await seedTerminal('expired', 5);
      // Survivors: old pending rows, fresh terminal rows, and the live waiter
      // (its pending row backdated past the horizon).
      const oldPending = await seedTerminal('pending', 4);
      const freshResolved = await seedTerminal('resolved', 3, false);
      await postgresClient.query(
        `UPDATE public.hmsh_escalations SET updated_at = ${OLD}
         WHERE role = 'retention-waiter' AND metadata @> $1::jsonb`,
        [JSON.stringify({ caseId })],
      );

      // A namespace that owns no rows deletes nothing.
      const other = await client.escalations.prune({ olderThan: '90 days', namespace: 'no-such-ns' });
      expect(other.deleted).toBe(0);

      // Drain with a limit smaller than the backlog: 18 rows / 7 per call
      // → 7, 7, 4, 0. The loop must terminate.
      let total = 0;
      let calls = 0;
      for (;;) {
        const { deleted } = await client.escalations.prune({ olderThan: '90 days', limit: 7 });
        total += deleted;
        calls += 1;
        expect(calls).toBeLessThanOrEqual(10);
        if (deleted === 0) break;
      }
      expect(total).toBe(18);
      expect(calls).toBe(4);

      // Old pending rows survive regardless of age.
      const pendingLeft = await client.escalations.list({ role: 'retention-seed', status: 'pending' });
      expect(pendingLeft.map((e) => e.id).sort()).toEqual([...oldPending].sort());
      // Terminal rows inside the horizon survive.
      const resolvedLeft = await client.escalations.list({ role: 'retention-seed', status: 'resolved' });
      expect(resolvedLeft.map((e) => e.id).sort()).toEqual([...freshResolved].sort());
      // No old terminal rows remain.
      const staleTerminal = await postgresClient.query(
        `SELECT COUNT(*)::int AS total FROM public.hmsh_escalations
         WHERE role = 'retention-seed'
           AND status IN ('resolved','cancelled','expired')
           AND updated_at < NOW() - INTERVAL '90 days'`,
      );
      expect(staleTerminal.rows[0].total).toBe(0);
    }, 30_000);

    it('should prune only the requested terminal statuses', async () => {
      const resolvedIds = await seedTerminal('resolved', 2);
      const cancelledIds = await seedTerminal('cancelled', 2);

      const { deleted } = await client.escalations.prune({
        olderThan: '90 days',
        statuses: ['resolved'],
      });
      expect(deleted).toBe(resolvedIds.length);

      const left = await client.escalations.list({ role: 'retention-seed', status: 'cancelled' });
      expect(left.map((e) => e.id).sort()).toEqual([...cancelledIds].sort());

      const drained = await client.escalations.prune({ olderThan: '90 days', statuses: ['cancelled'] });
      expect(drained.deleted).toBe(cancelledIds.length);
    }, 15_000);

    it('should ignore non-terminal statuses passed to the filter', async () => {
      const { deleted } = await client.escalations.prune({
        olderThan: '90 days',
        statuses: ['pending' as any],
      });
      expect(deleted).toBe(0);
      const waiter = await client.escalations.list({ role: 'retention-waiter', status: 'pending' });
      expect(waiter.find((e) => (e.metadata as any)?.caseId === caseId)).toBeDefined();
    }, 10_000);

    it('should leave the aged live waiter resolvable — the workflow completes after pruning', async () => {
      const rows = await client.escalations.list({ role: 'retention-waiter', status: 'pending' });
      const esc = rows.find((e) => (e.metadata as any)?.caseId === caseId);
      expect(esc).toBeDefined();

      const resultPromise = handle.result();
      const result = await client.escalations.resolve({
        id: esc!.id,
        resolverPayload: { resolved: true, resolvedBy: 'auditor-1' },
      });
      expect(result.ok).toBe(true);

      const output = await resultPromise;
      expect((output as any).resolved).toBe(true);
      expect((output as any).resolvedBy).toBe('auditor-1');
    }, 20_000);
  });
});
