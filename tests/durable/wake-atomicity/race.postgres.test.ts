import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { Hook } from '../../../services/activities/hook';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Client, Worker } = Durable;

/**
 * Registration-window race (field addendum): the escalation row commits
 * with the waiter's Leg1 transaction and is immediately visible to hot
 * consumers, but the waiter's hook-signal registration runs post-commit.
 * A resolve that lands INSIDE that window must still wake the workflow:
 * the wake's webhook finds no registration, parks as a $pending marker,
 * and the completing registration must detect and redeliver it.
 *
 * The window is widened deterministically (registration delayed 1500ms)
 * so the resolve always lands inside it — no timing luck involved.
 */
describe('DURABLE | wake-atomicity | registration-window race', () => {
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

  it('should wake the workflow when the resolve lands inside the registration window', async () => {
    //hold the waiter's post-commit registration open: the escalation row
    //is visible while the hook signal is not yet registered
    const original = Hook.prototype.registerWebHookSignal;
    const spy = vi
      .spyOn(Hook.prototype, 'registerWebHookSignal')
      .mockImplementation(async function (this: unknown) {
        await sleepFor(1_500);
        return original.call(this);
      });

    try {
      client = new Client({ connection });
      handle = await client.workflow.start({
        args: [runId],
        taskQueue: 'wake-race',
        workflowName: 'crashWindowWorkflow',
        workflowId: guid(),
        expire: 600,
      });
      const worker = await Worker.create({
        connection,
        taskQueue: 'wake-race',
        workflow: workflows.crashWindowWorkflow,
      });
      await worker.run();

      //hot consumer: resolve the row the instant it becomes visible
      let resolved = false;
      const start = Date.now();
      while (!resolved) {
        const list = await client.escalations.list({
          role: 'crash-approver',
          status: 'pending',
        });
        const escalation = list.find(
          (e) => (e.metadata as any)?.runId === runId,
        );
        if (escalation) {
          const result = await client.escalations.resolve({
            id: escalation.id,
            resolverPayload: { approved: true, via: 'in-window' },
          });
          expect(result.ok).toBe(true);
          resolved = true;
        } else {
          if (Date.now() - start > 20_000) {
            throw new Error('timed out waiting for escalation row');
          }
          await sleepFor(25);
        }
      }

      //registration completes ~1.5s later; the parked wake must be
      //detected and redelivered — the workflow wakes with the payload
      const output = (await Promise.race([
        handle.result(),
        sleepFor(25_000).then(() => {
          throw new Error(
            'workflow never woke: in-window resolve was delivered to nobody',
          );
        }),
      ])) as { approved: boolean; via: string };
      expect(output.approved).toBe(true);
      expect(output.via).toBe('in-window');
    } finally {
      spy.mockRestore();
    }
  }, 60_000);

  it('should wake the workflow when the pending handoff dies after consuming the marker', async () => {
    //same in-window resolve, but the process "dies" the instant the
    //registration consumes the $pending marker — before the in-memory
    //redelivery publish. The redelivery must be durable (committed with
    //the marker consumption), or the wake is destroyed with the marker.
    const raceRunId = guid();
    const original = Hook.prototype.registerWebHookSignal;
    const delaySpy = vi
      .spyOn(Hook.prototype, 'registerWebHookSignal')
      .mockImplementation(async function (this: unknown) {
        await sleepFor(1_500);
        return original.call(this);
      });
    const deathSpy = vi
      .spyOn(Hook.prototype as any, 'redeliverPendingSignal')
      .mockResolvedValue(undefined);

    try {
      const raceHandle = await client.workflow.start({
        args: [raceRunId],
        taskQueue: 'wake-race',
        workflowName: 'crashWindowWorkflow',
        workflowId: guid(),
        expire: 600,
      });

      let resolved = false;
      const start = Date.now();
      while (!resolved) {
        const list = await client.escalations.list({
          role: 'crash-approver',
          status: 'pending',
        });
        const escalation = list.find(
          (e) => (e.metadata as any)?.runId === raceRunId,
        );
        if (escalation) {
          const result = await client.escalations.resolve({
            id: escalation.id,
            resolverPayload: { approved: true, via: 'durable-handoff' },
          });
          expect(result.ok).toBe(true);
          resolved = true;
        } else {
          if (Date.now() - start > 20_000) {
            throw new Error('timed out waiting for escalation row');
          }
          await sleepFor(25);
        }
      }

      const output = (await Promise.race([
        raceHandle.result(),
        sleepFor(25_000).then(() => {
          throw new Error(
            'workflow never woke: the pending marker was consumed but its redelivery died in memory',
          );
        }),
      ])) as { approved: boolean; via: string };
      expect(output.approved).toBe(true);
      expect(output.via).toBe('durable-handoff');
    } finally {
      delaySpy.mockRestore();
      deathSpy.mockRestore();
    }
  }, 60_000);
});
