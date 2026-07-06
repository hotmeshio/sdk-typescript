import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { EscalationClientService } from '../../../services/escalations/client';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

/**
 * The wake of a condition() awaiter must be atomic with the escalation
 * resolve commit. The historical window: resolveEscalation committed
 * `status='resolved'` (+resolver_payload), then the wake signal was
 * published as a separate post-commit step — a process death between
 * the two left the row resolved and the awaiting workflow asleep
 * forever, with no error and no retry.
 *
 * This test simulates that death faithfully by suppressing the
 * post-commit delivery step entirely: the workflow must still wake,
 * because the wake message commits WITH the resolve transaction.
 */
describe('DURABLE | wake-atomicity | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const runId = guid();

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  describe('Worker and Client', () => {
    it('should park a workflow on condition()', async () => {
      client = new Client({ connection });
      handle = await client.workflow.start({
        args: [runId],
        taskQueue: 'wake-world',
        workflowName: 'crashWindowWorkflow',
        workflowId: guid(),
        expire: 600,
      });

      const worker = await Worker.create({
        connection,
        taskQueue: 'wake-world',
        workflow: workflows.crashWindowWorkflow,
      });
      await worker.run();

      //wait for the condition() suspension to write the escalation row
      let escalation: { id: string } | undefined;
      const start = Date.now();
      while (!escalation) {
        const list = await client.escalations.list({
          role: 'crash-approver',
          status: 'pending',
        });
        escalation = list.find((e) => (e.metadata as any)?.runId === runId);
        if (!escalation && Date.now() - start > 15_000) {
          throw new Error('timed out waiting for escalation row');
        }
        if (!escalation) await sleepFor(250);
      }
      expect(escalation).toBeDefined();
    }, 20_000);
  });

  describe('Resolve with the post-commit delivery step suppressed', () => {
    it('should wake the awaiting workflow from the resolve transaction alone', async () => {
      const list = await client.escalations.list({
        role: 'crash-approver',
        status: 'pending',
      });
      const escalation = list.find((e) => (e.metadata as any)?.runId === runId);
      expect(escalation).toBeDefined();

      //simulate death between the resolve commit and the post-commit
      //wake publish: the delivery step never runs
      const spy = vi
        .spyOn(
          EscalationClientService.prototype as any,
          '_deliverEscalationSignal',
        )
        .mockResolvedValue(false);

      try {
        const result = await client.escalations.resolve({
          id: escalation!.id,
          resolverPayload: { approved: true, via: 'atomic-wake' },
        });
        expect(result.ok).toBe(true);

        //the durable proof is committed (the crash-window precondition)
        const row = await postgresClient.query(
          `SELECT status FROM public.hmsh_escalations WHERE id = $1`,
          [escalation!.id],
        );
        expect(row.rows[0].status).toBe('resolved');

        //the workflow must wake and return the resolver payload — the
        //wake rides the resolve transaction, not the suppressed step
        const output = (await Promise.race([
          handle.result(),
          sleepFor(25_000).then(() => {
            throw new Error(
              'workflow never woke: wake was lost with the suppressed post-commit delivery',
            );
          }),
        ])) as { approved: boolean; via: string };
        expect(output.approved).toBe(true);
        expect(output.via).toBe('atomic-wake');
      } finally {
        spy.mockRestore();
      }
    }, 40_000);
  });
});
