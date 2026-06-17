import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | signal-queue | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('Connection', () => {
    it('should echo config', async () => {
      const conn = await Connection.connect({ class: Postgres, options: postgres_options });
      expect(conn).toBeDefined();
    });
  });

  describe('signal queue lifecycle', () => {
    it('claim → resolve via claimByMetadata → resolveByMetadata', async () => {
      const orderId = `order-${guid()}`;
      const signalId = `approve-order-${orderId}`;

      const client = new Client({ connection });

      await Worker.create({
        connection,
        taskQueue: 'signal-queue-test',
        workflow: workflows.queuedApproval,
      }).then(w => w.run());

      //start workflow — it will call condition() with queueConfig and suspend
      const handle = await client.workflow.start({
        args: [orderId],
        taskQueue: 'signal-queue-test',
        workflowName: 'queuedApproval',
        workflowId: guid(),
      });

      //allow time for the workflow to reach condition() and enqueue the signal
      await sleepFor(3_000);

      //verify the signal queue record was created
      const pending = await client.signalQueue.list({ role: 'pharmacist', status: 'pending' });
      const match = pending.find(s => s.metadata?.orderId === orderId);
      expect(match).toBeDefined();
      expect(match.signalKey).toBe(signalId);
      expect(match.status).toBe('pending');
      expect(match.envelope?.formSchema).toBeDefined();

      //claim by metadata key — must succeed
      const claimResult = await client.signalQueue.claimByMetadata({
        key: 'orderId',
        value: orderId,
        assignee: 'pharmacist-jane',
        durationMinutes: 10,
      });
      expect(claimResult.ok).toBe(true);
      if (!claimResult.ok) throw new Error('unreachable');
      expect(claimResult.entry.status).toBe('claimed');
      expect(claimResult.entry.assignedTo).toBe('pharmacist-jane');

      //concurrent claim attempt must return conflict
      const concurrent = await client.signalQueue.claimByMetadata({
        key: 'orderId',
        value: orderId,
      });
      expect(concurrent.ok).toBe(false);
      if (concurrent.ok) throw new Error('unreachable');
      expect(concurrent.reason).toBe('conflict');

      //resolve — delivers signal to paused workflow
      const resolveResult = await client.signalQueue.resolve({
        id: claimResult.entry.id,
        resolverPayload: { approved: true, notes: 'Looks good' },
      });
      expect(resolveResult.ok).toBe(true);

      //workflow resumes and returns the resolver payload
      const result = await handle.result();
      expect(result).toMatchObject({ approved: true, notes: 'Looks good' });

      //signal queue record should now be resolved
      const record = await client.signalQueue.get(claimResult.entry.id);
      expect(record.status).toBe('resolved');
      expect(record.resolverPayload).toMatchObject({ approved: true });

      //re-resolve must return already-resolved
      const dupResolve = await client.signalQueue.resolve({
        id: claimResult.entry.id,
        resolverPayload: { approved: true },
      });
      expect(dupResolve.ok).toBe(false);
      if (dupResolve.ok) throw new Error('unreachable');
      expect(dupResolve.reason).toBe('already-resolved');
    }, 30_000);

    it('resolveByMetadata resumes workflow', async () => {
      const orderId = `order-${guid()}`;
      const client = new Client({ connection });

      const handle = await client.workflow.start({
        args: [orderId],
        taskQueue: 'signal-queue-test',
        workflowName: 'queuedApproval',
        workflowId: guid(),
      });

      await sleepFor(3_000);

      const resolveResult = await client.signalQueue.resolveByMetadata({
        key: 'orderId',
        value: orderId,
        resolverPayload: { approved: false, reason: 'Out of stock' },
      });
      expect(resolveResult.ok).toBe(true);

      const result = await handle.result();
      expect(result).toMatchObject({ approved: false });

      //second resolveByMetadata must return not-found (already resolved)
      const dupResolve = await client.signalQueue.resolveByMetadata({
        key: 'orderId',
        value: orderId,
      });
      expect(dupResolve.ok).toBe(false);
      if (dupResolve.ok) throw new Error('unreachable');
      expect(dupResolve.reason).toBe('not-found');
    }, 20_000);

    it('expiry sweeper re-queues expired claims', async () => {
      const orderId = `order-${guid()}`;
      const client = new Client({ connection });

      const handle = await client.workflow.start({
        args: [orderId],
        taskQueue: 'signal-queue-test',
        workflowName: 'queuedApproval',
        workflowId: guid(),
      });

      await sleepFor(3_000);

      const claimResult = await client.signalQueue.claimByMetadata({
        key: 'orderId',
        value: orderId,
        durationMinutes: 0,
      });
      expect(claimResult.ok).toBe(true);
      if (!claimResult.ok) throw new Error('unreachable');
      expect(claimResult.entry.status).toBe('claimed');

      //release a non-existent id must return not-found
      const badRelease = await client.signalQueue.release({
        id: '00000000-0000-0000-0000-000000000000',
      });
      expect(badRelease.ok).toBe(false);
      if (badRelease.ok) throw new Error('unreachable');
      expect(badRelease.reason).toBe('not-found');

      //manually force expiry by calling releaseExpired
      const released = await client.signalQueue.releaseExpired();
      expect(released).toBeGreaterThanOrEqual(1);

      //record should be pending again
      const record = await client.signalQueue.get(claimResult.entry.id);
      expect(record.status).toBe('pending');

      //release on pending record must return wrong-status
      const wrongStatus = await client.signalQueue.release({ id: claimResult.entry.id });
      expect(wrongStatus.ok).toBe(false);
      if (wrongStatus.ok) throw new Error('unreachable');
      expect(wrongStatus.reason).toBe('wrong-status');

      //clean up — resolve it so the workflow completes
      const cleanup = await client.signalQueue.resolve({
        id: claimResult.entry.id,
        resolverPayload: { cleaned: true },
      });
      expect(cleanup.ok).toBe(true);
      await handle.result();
    }, 20_000);

    it('condition() with timeout + queueConfig still enqueues', async () => {
      const orderId = `order-${guid()}`;
      const client = new Client({ connection });

      await Worker.create({
        connection,
        taskQueue: 'signal-queue-test',
        workflow: workflows.queuedApprovalWithTimeout,
      }).then(w => w.run());

      const handle = await client.workflow.start({
        args: [orderId],
        taskQueue: 'signal-queue-test',
        workflowName: 'queuedApprovalWithTimeout',
        workflowId: guid(),
      });

      await sleepFor(3_000);

      const pending = await client.signalQueue.list({ role: 'pharmacist', status: 'pending' });
      const match = pending.find(s => s.metadata?.orderId === orderId);
      expect(match).toBeDefined();

      //resolve before timeout
      const resolveResult = await client.signalQueue.resolve({
        id: match.id,
        resolverPayload: { approved: true },
      });
      expect(resolveResult.ok).toBe(true);

      const result = await handle.result();
      expect(result).toMatchObject({ approved: true });
    }, 20_000);
  });
});
