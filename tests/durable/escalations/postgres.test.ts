/**
 * Proves hmsh_escalations works as a Durable condition() primitive.
 *
 * condition(signalId, queueConfig) writes one complete row to
 * public.hmsh_escalations at suspension time. No second write. No
 * proxyActivity overhead. Resolvers call client.escalations.resolve()
 * which atomically marks resolved and delivers the signal.
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

describe('DURABLE | escalations | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;
  let client: InstanceType<typeof Client>;
  const connection = { class: Postgres, options: postgres_options };
  const orderId = guid();
  const region = 'us-east';

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

  describe('Connection', () => {
    it('should connect', async () => {
      const conn = await Connection.connect({ class: Postgres, options: postgres_options });
      expect(conn).toBeDefined();
    });
  });

  describe('Client', () => {
    it('should connect a client and queue a workflow (bootstraps durable schema)', async () => {
      client = new Client({ connection });
      handle = await client.workflow.start({
        args: [orderId, region],
        taskQueue: 'escalation-test',
        workflowName: 'approvalWorkflow',
        workflowId: guid(),
      });
      expect(handle.workflowId).toBeDefined();
    }, 15_000);
  });

  describe('Worker', () => {
    it('should create a worker and process the workflow (durable schema already exists)', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.approvalWorkflow,
      });
      await worker.run();
      // Wait for the condition() to fire and write the escalation row
      await sleepFor(2000);
    }, 15_000);
  });

  describe('single-write proof', () => {
    it('should have written an hmsh_escalations row with full routing context', async () => {
      const list = await client.escalations.list({ role: 'approver', status: 'pending' });
      const esc = list.find((e) => (e.metadata as any)?.orderId === orderId);
      expect(esc).toBeDefined();
      expect(esc!.signal_key).not.toBeNull();
      expect(esc!.workflow_id).toBeDefined();
      expect(esc!.topic).toBeDefined();
      expect(esc!.task_queue).toBe('escalation-test');
      expect(esc!.role).toBe('approver');
      expect(esc!.type).toBe('order-approval');
      expect(esc!.subtype).toBe('regional');
      expect(esc!.priority).toBe(2);
      expect((esc!.metadata as any)?.orderId).toBe(orderId);
      expect((esc!.metadata as any)?.region).toBe(region);
    }, 5_000);

    it('should list by type+subtype filter', async () => {
      const list = await client.escalations.list({ type: 'order-approval', subtype: 'regional' });
      expect(list.length).toBeGreaterThanOrEqual(1);
      expect(list.every((e) => e.type === 'order-approval')).toBe(true);
    }, 5_000);
  });

  describe('claim lifecycle', () => {
    let escalationId: string;

    it('should claim the escalation — status becomes claimed', async () => {
      const list = await client.escalations.list({ role: 'approver', status: 'pending' });
      const esc = list.find((e) => (e.metadata as any)?.orderId === orderId);
      escalationId = esc!.id;

      const result = await client.escalations.claim({
        id: escalationId,
        assignee: 'reviewer-1',
        durationMinutes: 5,
      });
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.entry.status).toBe('claimed');
        expect(result.entry.assigned_to).toBe('reviewer-1');
      }
    }, 5_000);

    it('should return conflict when claiming an already-claimed row', async () => {
      const result = await client.escalations.claim({
        id: escalationId,
        assignee: 'reviewer-2',
        durationMinutes: 5,
      });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe('conflict');
    }, 5_000);

    it('should return not-found for a nonexistent id', async () => {
      const result = await client.escalations.claim({
        id: '00000000-0000-0000-0000-000000000000',
        assignee: 'reviewer-1',
      });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe('not-found');
    }, 5_000);

    it('should list claimed escalations by assignedTo filter', async () => {
      const list = await client.escalations.list({ assignedTo: 'reviewer-1', status: 'claimed' });
      expect(list.length).toBeGreaterThanOrEqual(1);
      expect(list[0].assigned_to).toBe('reviewer-1');
    }, 5_000);

    it('should return wrong-assignee when releasing with wrong assignee', async () => {
      const result = await client.escalations.release({
        id: escalationId,
        assignee: 'not-reviewer-1',
      });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe('wrong-assignee');
    }, 5_000);

    it('should append milestones to the escalation', async () => {
      const updated = await client.escalations.appendMilestones({
        id: escalationId,
        milestones: [{ name: 'reviewed', value: 'documents checked' }],
      });
      expect(updated).not.toBeNull();
      expect((updated!.milestones as any[]).length).toBe(1);
      expect((updated!.milestones as any[])[0].name).toBe('reviewed');
    }, 5_000);

    it('should update the escalation description and priority', async () => {
      const updated = await client.escalations.update({
        id: escalationId,
        description: 'Urgent: expedite approval',
        priority: 1,
      });
      expect(updated).not.toBeNull();
      expect(updated!.description).toBe('Urgent: expedite approval');
      expect(updated!.priority).toBe(1);
    }, 5_000);

    it('should resolve the escalation and resume the workflow, returning the result', async () => {
      const resultPromise = handle.result();

      const result = await client.escalations.resolve({
        id: escalationId,
        resolverPayload: { approved: true, approvedBy: 'reviewer-1' },
      });
      expect(result.ok).toBe(true);

      const output = await resultPromise;
      expect((output as any).approved).toBe(true);
      expect((output as any).approvedBy).toBe('reviewer-1');
    }, 15_000);

    it('should return already-resolved when resolving again', async () => {
      const result = await client.escalations.resolve({ id: escalationId });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe('already-resolved');
    }, 5_000);
  });

  describe('cancel lifecycle', () => {
    it('should cancel a pending escalation', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        description: 'Ticket to cancel',
        metadata: { ticketId: 'T-cancel-1' },
      });
      const result = await client.escalations.cancel(esc.id);
      expect(result.ok).toBe(true);
    }, 5_000);

    it('should return already-terminal when cancelling a cancelled row', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { ticketId: 'T-cancel-2' },
      });
      await client.escalations.cancel(esc.id);
      const result = await client.escalations.cancel(esc.id);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe('already-terminal');
    }, 5_000);
  });

  describe('escalateToRole', () => {
    it('should reassign an escalation to a different role', async () => {
      const esc = await client.escalations.create({
        type: 'review',
        role: 'analyst',
        description: 'Needs senior review',
        metadata: { caseId: 'C-001' },
      });
      const updated = await client.escalations.escalateToRole({ id: esc.id, targetRole: 'senior-analyst' });
      expect(updated).not.toBeNull();
      expect(updated!.role).toBe('senior-analyst');
      expect(updated!.status).toBe('pending');
      expect(updated!.assigned_to).toBeNull();
    }, 5_000);
  });

  describe('claimByMetadata with candidatesExist', () => {
    it('should return candidatesExist=0 when no metadata matches', async () => {
      const result = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: 'nonexistent-ticket',
        assignee: 'agent-1',
      });
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe('not-found');
        expect(result.candidatesExist).toBe(0);
      }
    }, 5_000);

    it('should return conflict with candidatesExist>0 when all candidates are claimed', async () => {
      const esc = await client.escalations.create({
        type: 'review',
        role: 'agent',
        metadata: { ticketId: 'T-conflict' },
      });
      // Claim it
      await client.escalations.claim({ id: esc.id, assignee: 'agent-1', durationMinutes: 10 });
      // Now try to claim by metadata — should see conflict
      const result = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: 'T-conflict',
        assignee: 'agent-2',
      });
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe('conflict');
        expect(result.candidatesExist).toBeGreaterThan(0);
      }
    }, 5_000);
  });

  describe('standalone escalation (no signal_key)', () => {
    it('should create a manual escalation with null signal_key', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        description: 'Manual support ticket',
        metadata: { ticketId: 'T-456' },
      });
      expect(esc.signal_key).toBeNull();
      expect(esc.type).toBe('support');
      expect(esc.status).toBe('pending');
    }, 5_000);
  });
});
