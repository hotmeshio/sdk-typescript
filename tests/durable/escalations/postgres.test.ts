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
import { guid, sleepFor, uuid } from '../../../modules/utils';
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
    it('should create workers and process the workflow (durable schema already exists)', async () => {
      const approvalWorker = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.approvalWorkflow,
      });
      await approvalWorker.run();
      const cancelWorker = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.cancelAwareWorkflow,
      });
      await cancelWorker.run();
      const slaWorker = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.slaGatedWorkflow,
      });
      await slaWorker.run();
      const slaHookParent = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.slaHookGatedWorkflow,
      });
      await slaHookParent.run();
      const slaCycled = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.slaCycledWorkflow,
      });
      await slaCycled.run();
      const slaCollated = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.slaCollatedWorkflow,
      });
      await slaCollated.run();
      const slaGate = await Worker.create({
        connection,
        taskQueue: 'escalation-test',
        workflow: workflows.slaGateHook,
      });
      await slaGate.run();
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

  describe('Leg1 metadata atomicity contract', () => {
    it('should expose config.metadata on the row from its first visible moment', async () => {
      // The escalation INSERT (metadata included) commits inside the Leg1
      // checkpoint transaction. A tight poll from workflow start therefore
      // always sees a COMPLETE row: the first sighting carries metadata and
      // full routing context. A two-commit write (row first, facets later)
      // would hand this poller a metadata-less row.
      const probeOrderId = guid();
      const probe = await client.workflow.start({
        args: [probeOrderId, region],
        taskQueue: 'escalation-test',
        workflowName: 'approvalWorkflow',
        workflowId: guid(),
      });

      let first: Awaited<ReturnType<typeof client.escalations.list>>[number] | undefined;
      const deadline = Date.now() + 10_000;
      while (!first && Date.now() < deadline) {
        const rows = await client.escalations.list({ role: 'approver', status: 'pending' });
        first = rows.find((e) => (e.metadata as any)?.orderId === probeOrderId
          || (e.description ?? '').includes(probeOrderId));
        if (!first) await sleepFor(25);
      }
      expect(first).toBeDefined();
      // First sighting is already complete — metadata facets AND routing.
      expect((first!.metadata as any)?.orderId).toBe(probeOrderId);
      expect((first!.metadata as any)?.region).toBe(region);
      expect(first!.signal_key).not.toBeNull();
      expect(first!.topic).toBeDefined();
      expect(first!.workflow_id).toBeDefined();
      expect(first!.task_queue).toBe('escalation-test');

      // Leave no parked probe behind: resolve it and confirm completion.
      const resolved = await client.escalations.resolve({
        id: first!.id,
        resolverPayload: { approved: true, approvedBy: 'atomicity-probe' },
      });
      expect(resolved.ok).toBe(true);
      const output = await probe.result();
      expect((output as any).approvedBy).toBe('atomicity-probe');
    }, 20_000);
  });

  describe('claim lifecycle', () => {
    let escalationId: string;

    it('should claim the escalation — implicit model: status stays pending, assigned_to is set', async () => {
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
        // Implicit claim model: status stays 'pending'; claim expressed via assigned_to + assigned_until
        expect(result.entry.status).toBe('pending');
        expect(result.entry.assigned_to).toBe('reviewer-1');
        expect(result.entry.assigned_until).not.toBeNull();
        // Fresh claim — not an extension of a prior claim by the same assignee
        expect(result.isExtension).toBe(false);
      }
    }, 5_000);

    it('should return conflict when claiming an already-claimed row', async () => {
      const result = await client.escalations.claim({
        id: escalationId,
        assignee: 'reviewer-2',
        durationMinutes: 5,
      });
      expect(result.ok).toBe(false);
      if (result.ok === false) expect(result.reason).toBe('conflict');
    }, 5_000);

    it('should return not-found for a nonexistent id', async () => {
      const result = await client.escalations.claim({
        id: '00000000-0000-0000-0000-000000000000',
        assignee: 'reviewer-1',
      });
      expect(result.ok).toBe(false);
      if (result.ok === false) expect(result.reason).toBe('not-found');
    }, 5_000);

    it('should list claimed escalations by assignedTo filter (no status filter needed)', async () => {
      const list = await client.escalations.list({ assignedTo: 'reviewer-1' });
      expect(list.length).toBeGreaterThanOrEqual(1);
      expect(list[0].assigned_to).toBe('reviewer-1');
    }, 5_000);

    it('should return wrong-assignee when releasing with wrong assignee', async () => {
      const result = await client.escalations.release({
        id: escalationId,
        assignee: 'not-reviewer-1',
      });
      expect(result.ok).toBe(false);
      if (result.ok === false) expect(result.reason).toBe('wrong-assignee');
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
      if (result.ok === false) expect(result.reason).toBe('already-resolved');
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
      if (result.ok === false) expect(result.reason).toBe('already-terminal');
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

  describe('claimByMetadata with candidatesExist + isExtension', () => {
    it('should return candidatesExist=0 when no metadata matches', async () => {
      const result = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: 'nonexistent-ticket',
        assignee: 'agent-1',
      });
      expect(result.ok).toBe(false);
      if (result.ok === false) {
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
      // Claim it with agent-1
      await client.escalations.claim({ id: esc.id, assignee: 'agent-1', durationMinutes: 10 });
      // agent-2 tries to claim by metadata — should see conflict
      const result = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: 'T-conflict',
        assignee: 'agent-2',
      });
      expect(result.ok).toBe(false);
      if (result.ok === false) {
        expect(result.reason).toBe('conflict');
        expect(result.candidatesExist).toBeGreaterThan(0);
      }
    }, 5_000);

    it('should return isExtension=false on a fresh claim', async () => {
      const esc = await client.escalations.create({
        type: 'review',
        role: 'agent',
        metadata: { ticketId: `T-ext-fresh-${guid()}` },
      });
      const result = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: (esc.metadata as any).ticketId,
        assignee: 'agent-ext',
        durationMinutes: 10,
      });
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.isExtension).toBe(false);
        expect(result.entry.assigned_to).toBe('agent-ext');
      }
    }, 5_000);

    it('should return isExtension=true when the same assignee re-claims (extends expiry)', async () => {
      const ticketId = `T-ext-${guid()}`;
      await client.escalations.create({
        type: 'review',
        role: 'agent',
        metadata: { ticketId },
      });
      // First claim
      const first = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: ticketId,
        assignee: 'agent-extend',
        durationMinutes: 1,
      });
      expect(first.ok).toBe(true);
      if (first.ok) expect(first.isExtension).toBe(false);

      // Re-claim by same assignee — isExtension must be true
      const second = await client.escalations.claimByMetadata({
        key: 'ticketId',
        value: ticketId,
        assignee: 'agent-extend',
        durationMinutes: 5,
      });
      expect(second.ok).toBe(true);
      if (second.ok) {
        expect(second.isExtension).toBe(true);
        expect(second.entry.assigned_to).toBe('agent-extend');
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

  describe('available field + filter', () => {
    it('list() should include available=true for unclaimed rows', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'avail-test',
        metadata: { availTest: `avail-${guid()}` },
      });
      const list = await client.escalations.list({ role: 'avail-test' });
      const row = list.find((e) => e.id === esc.id);
      expect(row).toBeDefined();
      expect(row!.available).toBe(true);
    }, 5_000);

    it('list() should include available=false for actively claimed rows', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'avail-test-2',
        metadata: { availTest2: `avail2-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'tester', durationMinutes: 30 });
      const list = await client.escalations.list({ role: 'avail-test-2' });
      const row = list.find((e) => e.id === esc.id);
      expect(row).toBeDefined();
      expect(row!.available).toBe(false);
    }, 5_000);

    it('list({ available: true }) should return only unclaimed rows', async () => {
      const tag = guid();
      const [free, claimed] = await Promise.all([
        client.escalations.create({ role: 'avail-filter', metadata: { tag } }),
        client.escalations.create({ role: 'avail-filter', metadata: { tag } }),
      ]);
      await client.escalations.claim({ id: claimed.id, assignee: 'holder', durationMinutes: 30 });

      const available = await client.escalations.list({ role: 'avail-filter', available: true });
      const ids = available.map((e) => e.id);
      expect(ids).toContain(free.id);
      expect(ids).not.toContain(claimed.id);
    }, 5_000);

    it('list({ available: false }) should return only actively claimed rows', async () => {
      const tag = guid();
      const [free2, claimed2] = await Promise.all([
        client.escalations.create({ role: 'avail-filter2', metadata: { tag } }),
        client.escalations.create({ role: 'avail-filter2', metadata: { tag } }),
      ]);
      await client.escalations.claim({ id: claimed2.id, assignee: 'holder2', durationMinutes: 30 });

      const notAvailable = await client.escalations.list({ role: 'avail-filter2', available: false });
      const ids = notAvailable.map((e) => e.id);
      expect(ids).toContain(claimed2.id);
      expect(ids).not.toContain(free2.id);
    }, 5_000);
  });

  describe('sort parameters', () => {
    it('list({ sortBy: "priority", sortOrder: "desc" }) should return highest priority first', async () => {
      const tag = `sort-${guid()}`;
      await Promise.all([
        client.escalations.create({ role: 'sort-test', priority: 3, metadata: { tag } }),
        client.escalations.create({ role: 'sort-test', priority: 1, metadata: { tag } }),
        client.escalations.create({ role: 'sort-test', priority: 5, metadata: { tag } }),
      ]);
      const list = await client.escalations.list({
        role: 'sort-test',
        sortBy: 'priority',
        sortOrder: 'desc',
      });
      const tagged = list.filter((e) => (e.metadata as any)?.tag === tag);
      expect(tagged.length).toBe(3);
      // Descending: 5, 3, 1
      expect(tagged[0].priority).toBeGreaterThanOrEqual(tagged[1].priority);
      expect(tagged[1].priority).toBeGreaterThanOrEqual(tagged[2].priority);
    }, 5_000);

    it('default sort (priority ASC, created_at DESC) — lower priority number first', async () => {
      const tag = `sort-default-${guid()}`;
      await Promise.all([
        client.escalations.create({ role: 'sort-default', priority: 5, metadata: { tag } }),
        client.escalations.create({ role: 'sort-default', priority: 1, metadata: { tag } }),
      ]);
      const list = await client.escalations.list({ role: 'sort-default' });
      const tagged = list.filter((e) => (e.metadata as any)?.tag === tag);
      expect(tagged.length).toBe(2);
      expect(tagged[0].priority).toBeLessThanOrEqual(tagged[1].priority);
    }, 5_000);
  });

  describe('count()', () => {
    it('should return count of rows matching filters', async () => {
      const tag = `count-${guid()}`;
      await Promise.all([
        client.escalations.create({ role: 'count-role', metadata: { tag } }),
        client.escalations.create({ role: 'count-role', metadata: { tag } }),
        client.escalations.create({ role: 'count-role', metadata: { tag } }),
      ]);
      const total = await client.escalations.count({ role: 'count-role' });
      expect(total).toBeGreaterThanOrEqual(3);
    }, 5_000);

    it('should return 0 for a filter that matches nothing', async () => {
      const total = await client.escalations.count({ role: `no-such-role-${guid()}` });
      expect(total).toBe(0);
    }, 5_000);
  });

  describe('roles[] multi-role filter', () => {
    it('list({ roles }) should return rows matching any of the provided roles', async () => {
      const tag = `roles-${guid()}`;
      await Promise.all([
        client.escalations.create({ role: 'role-alpha', metadata: { tag } }),
        client.escalations.create({ role: 'role-beta',  metadata: { tag } }),
        client.escalations.create({ role: 'role-gamma', metadata: { tag } }),
      ]);
      const list = await client.escalations.list({ roles: ['role-alpha', 'role-beta'] });
      const taggedRoles = list
        .filter((e) => (e.metadata as any)?.tag === tag)
        .map((e) => e.role);
      expect(taggedRoles).toContain('role-alpha');
      expect(taggedRoles).toContain('role-beta');
      expect(taggedRoles).not.toContain('role-gamma');
    }, 5_000);

    it('count({ roles }) should count across multiple roles', async () => {
      const tag = `roles-count-${guid()}`;
      await Promise.all([
        client.escalations.create({ role: 'rcount-a', metadata: { tag } }),
        client.escalations.create({ role: 'rcount-b', metadata: { tag } }),
      ]);
      const total = await client.escalations.count({ roles: ['rcount-a', 'rcount-b'] });
      expect(total).toBeGreaterThanOrEqual(2);
    }, 5_000);
  });

  describe('concurrent resolve (TOCTOU safety)', () => {
    /**
     * Two callers fire resolve() simultaneously on the same row.
     * store.resolveEscalation() uses FOR UPDATE inside its CTE, serializing
     * concurrent callers at the DB level. The second caller blocks on the
     * row lock, reads 'already-resolved' after the first commits, and
     * returns { ok: false, reason: 'already-resolved' } — exactly one
     * signal is delivered, never two.
     */
    it('should resolve exactly once when two callers race', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        description: 'Concurrent resolve test',
        metadata: { ticketId: `T-concurrent-${guid()}` },
      });

      // Fire both resolves simultaneously — no await between them.
      const [r1, r2] = await Promise.all([
        client.escalations.resolve({ id: esc.id, resolverPayload: { winner: 1 } }),
        client.escalations.resolve({ id: esc.id, resolverPayload: { winner: 2 } }),
      ]);

      const successes = [r1, r2].filter((r) => r.ok).length;
      const alreadyResolved = [r1, r2].filter((r) => !r.ok && (r as any).reason === 'already-resolved').length;

      // Exactly one caller wins; the other sees already-resolved.
      expect(successes).toBe(1);
      expect(alreadyResolved).toBe(1);

      // Row is definitively resolved — not stuck in pending.
      const final = await client.escalations.get(esc.id);
      expect(final?.status).toBe('resolved');
    }, 10_000);
  });

  describe('resolve() / release() return entry', () => {
    it('resolve() should return entry with status resolved', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { resolveEntry: `re-${guid()}` },
      });
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entry.id).toBe(esc.id);
      expect(result.entry.status).toBe('resolved');
      expect((result.entry.resolver_payload as any)?.approved).toBe(true);
    }, 5_000);

    it('release() should return entry with cleared assignment', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { releaseEntry: `rele-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'alice@example.com', durationMinutes: 5 });
      const result = await client.escalations.release({ id: esc.id, assignee: 'alice@example.com' });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entry.id).toBe(esc.id);
      expect(result.entry.status).toBe('pending');
      expect(result.entry.assigned_to).toBeNull();
      expect(result.entry.assigned_until).toBeNull();
    }, 5_000);

  });

  describe('resolve() assertClaim guard', () => {
    /** Backdates a claim so `assigned_until` is in the past — an expired claim. */
    const expireClaim = async (id: string) => {
      await postgresClient.query(
        `UPDATE public.hmsh_escalations
         SET assigned_until = NOW() - INTERVAL '1 minute'
         WHERE id = $1`,
        [id],
      );
    };

    it('resolves an unclaimed row when assertClaim is provided', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(true);
    }, 5_000);

    it('resolves when the asserting assignee holds a live claim', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'alice@example.com', durationMinutes: 30 });
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(true);
    }, 5_000);

    it('blocks with claim-expired when the asserting assignee\'s claim has lapsed', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'alice@example.com', durationMinutes: 30 });
      await expireClaim(esc.id);

      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.reason).toBe('claim-expired');

      //the row is untouched — still pending, resolvable after re-claim
      const row = await client.escalations.get(esc.id);
      expect(row?.status).toBe('pending');

      const reclaim = await client.escalations.claim({ id: esc.id, assignee: 'alice@example.com', durationMinutes: 30 });
      expect(reclaim.ok).toBe(true);
      const retry = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(retry.ok).toBe(true);
    }, 5_000);

    it('blocks with claimed-by-other when a different assignee holds a live claim', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'bob@example.com', durationMinutes: 30 });

      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(false);
      if (result.ok) return;
      expect(result.reason).toBe('claimed-by-other');

      const row = await client.escalations.get(esc.id);
      expect(row?.status).toBe('pending');
      expect(row?.assigned_to).toBe('bob@example.com');
    }, 5_000);

    it('resolves when a different assignee\'s claim window has lapsed — the lock is gone', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'bob@example.com', durationMinutes: 30 });
      await expireClaim(esc.id);

      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(true);
    }, 5_000);

    it('resolves a durable pre-assignment (assigned_to with no expiry window)', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      //pre-assignment shape: a workflow targets a named user with no TTL
      //window — an assignment for routing/RBAC, not a lock
      await postgresClient.query(
        `UPDATE public.hmsh_escalations
         SET assigned_to = 'alice@example.com', assigned_until = NULL
         WHERE id = $1`,
        [esc.id],
      );

      //the assignee resolves it
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
        assertClaim: 'alice@example.com',
      });
      expect(result.ok).toBe(true);
    }, 5_000);

    it('omitting assertClaim preserves existing semantics — expired claim does not block', async () => {
      const esc = await client.escalations.create({
        role: 'claim-guard',
        metadata: { guardTest: `g-${guid()}` },
      });
      await client.escalations.claim({ id: esc.id, assignee: 'alice@example.com', durationMinutes: 30 });
      await expireClaim(esc.id);

      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true },
      });
      expect(result.ok).toBe(true);
    }, 5_000);
  });

  describe('resolve() augments GIN-indexed metadata', () => {
    it('merges resolution metadata into the queryable surface, separate from resolverPayload', async () => {
      const orderTag = `gin-${guid()}`;
      const esc = await client.escalations.create({
        type: 'order-approval',
        role: 'approver',
        metadata: { orderId: orderTag, region: 'us-east' }, // creation: "what was intended"
      });
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { approved: true }, // signal payload — NOT merged into metadata
        metadata: { outcome: 'approved', approver: 'alice' }, // resolution: "what happened"
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;

      // (a) merged row carries BOTH creation and resolution keys
      expect((result.entry.metadata as any).orderId).toBe(orderTag);
      expect((result.entry.metadata as any).region).toBe('us-east');
      expect((result.entry.metadata as any).outcome).toBe('approved');
      expect((result.entry.metadata as any).approver).toBe('alice');

      // separation: resolution keys do NOT leak into resolver_payload
      expect((result.entry.resolver_payload as any).approved).toBe(true);
      expect((result.entry.resolver_payload as any).outcome).toBeUndefined();

      // (b) discoverable via the GIN @> containment path
      const byOutcome = await client.escalations.list({ metadata: { outcome: 'approved' } });
      expect(byOutcome.some((e) => e.id === esc.id)).toBe(true);
      const byIntersection = await client.escalations.list({
        metadata: { orderId: orderTag, outcome: 'approved' },
      });
      expect(byIntersection.some((e) => e.id === esc.id)).toBe(true);
    }, 5_000);

    it('re-resolve does not merge metadata (loser writes nothing)', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { idem: `idem-${guid()}` },
      });
      await client.escalations.resolve({ id: esc.id, metadata: { outcome: 'first' } });
      const second = await client.escalations.resolve({ id: esc.id, metadata: { outcome: 'second' } });
      expect(second.ok).toBe(false);
      if (second.ok) return;
      expect(second.reason).toBe('already-resolved');
      const row = await client.escalations.get(esc.id);
      expect((row?.metadata as any).outcome).toBe('first'); // loser did not overwrite
    }, 5_000);

    it('resolveByMetadata merges a separate metadata patch alongside the selector', async () => {
      const sel = `rbm-${guid()}`;
      const esc = await client.escalations.create({
        type: 'order-approval',
        role: 'rbm-approver',
        metadata: { selector: sel },
      });
      const result = await client.escalations.resolveByMetadata({
        key: 'selector',
        value: sel,
        resolverPayload: { approved: true },
        metadata: { outcome: 'resolved-by-metadata' },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entry.id).toBe(esc.id);
      expect((result.entry.metadata as any).selector).toBe(sel);
      expect((result.entry.metadata as any).outcome).toBe('resolved-by-metadata');
    }, 5_000);

    it('resolveMany merges metadata into every winning row', async () => {
      const tag = `bulkmeta-${guid()}`;
      const created = await Promise.all([
        client.escalations.create({ role: 'bulk-meta', metadata: { tag } }),
        client.escalations.create({ role: 'bulk-meta', metadata: { tag } }),
      ]);
      const ids = created.map((e) => e.id);
      const resolved = await client.escalations.resolveMany({ ids, metadata: { sweptBy: 'triage' } });
      expect(resolved.length).toBe(2);
      resolved.forEach((r) => {
        expect((r.metadata as any).tag).toBe(tag); // creation key preserved
        expect((r.metadata as any).sweptBy).toBe('triage');
      });
      const found = await client.escalations.list({ metadata: { sweptBy: 'triage', tag } });
      expect(found.length).toBe(2);
    }, 5_000);
  });

  describe('signal-first transaction', () => {
    it('should resolve a standalone (no signal_key) escalation cleanly', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { sfTest: `sf-${guid()}` },
      });
      const result = await client.escalations.resolve({
        id: esc.id,
        resolverPayload: { done: true },
      });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entry.status).toBe('resolved');
      const row = await client.escalations.get(esc.id);
      expect(row?.status).toBe('resolved');
      expect((row?.resolver_payload as any)?.done).toBe(true);
    }, 5_000);

    it('store.resolveEscalation() commits the DB row atomically (CTE path)', async () => {
      // Tests the store layer in isolation: the CTE always commits regardless of
      // what the client does with signal delivery afterward.
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { sfFail: `sf-fail-${guid()}` },
      });
      const hm = await (client as any).getHotMeshClient(null, undefined);
      const result = await hm.engine.store.resolveEscalation({ id: esc.id, resolverPayload: { done: true } });
      expect(result.ok).toBe(true);
      expect(result.entry).toBeDefined();
      expect(result.entry.status).toBe('resolved');
      const row = await client.escalations.get(esc.id);
      expect(row?.status).toBe('resolved');
    }, 5_000);
  });

  describe('workflow termination cancels pending escalations', () => {
    it('interrupting a workflow atomically cancels its pending escalation row', async () => {
      const terminateHandle = await client.workflow.start({
        args: [guid(), 'us-west'],
        taskQueue: 'escalation-test',
        workflowName: 'approvalWorkflow',
        workflowId: guid(),
      });

      // Wait for the condition() suspension to write the escalation row.
      await sleepFor(2000);

      const before = await client.escalations.list({
        workflowId: terminateHandle.workflowId,
        status: 'pending',
      });
      expect(before.length).toBe(1);

      // Terminate — this must cancel the escalation row.
      await terminateHandle.terminate({ suppress: true });

      const after = await client.escalations.list({
        workflowId: terminateHandle.workflowId,
        status: 'cancelled',
      });
      expect(after.length).toBe(1);
      expect(after[0].status).toBe('cancelled');

      // Row must no longer appear as pending.
      const stillPending = await client.escalations.list({
        workflowId: terminateHandle.workflowId,
        status: 'pending',
      });
      expect(stillPending.length).toBe(0);
    }, 20_000);
  });

  describe('cancel() delivers null to waiting condition', () => {
    it('cancelling an escalation resumes condition() with null', async () => {
      const cancelHandle = await client.workflow.start({
        args: [guid()],
        taskQueue: 'escalation-test',
        workflowName: 'cancelAwareWorkflow',
        workflowId: guid(),
      });

      // Wait for the condition() to suspend and write the escalation row.
      await sleepFor(2000);

      const pending = await client.escalations.list({
        workflowId: cancelHandle.workflowId,
        status: 'pending',
      });
      expect(pending.length).toBe(1);

      // Cancel the escalation — this delivers __escalation_cancelled signal.
      const cancelResult = await client.escalations.cancel(pending[0].id);
      expect(cancelResult.ok).toBe(true);

      // The workflow should resume and return null from condition().
      const output = await cancelHandle.result<null>();
      expect(output).toBeNull();
    }, 20_000);
  });

  describe('migration (full-fidelity UUID + state preservation)', () => {
    it('should insert a row preserving the original UUID and all lifecycle state', async () => {
      // Migration requires standard UUIDs — the id column is PostgreSQL UUID type.
      const originalId = uuid();
      const createdAt = new Date('2024-06-01T10:00:00Z');
      const resolvedAt = new Date('2024-06-01T11:30:00Z');

      const entry = await client.escalations.migrate({
        id: originalId,
        status: 'resolved',
        type: 'order-approval',
        role: 'approver',
        description: 'Migrated from lt_escalations',
        priority: 3,
        resolvedAt,
        resolverPayload: { approved: true },
        createdAt,
        metadata: { legacy: true, orderId: 'ORD-001' },
      });

      expect(entry).not.toBeNull();
      expect(entry!.id).toBe(originalId);
      expect(entry!.status).toBe('resolved');
      expect(entry!.type).toBe('order-approval');
      expect(entry!.role).toBe('approver');
      expect(entry!.priority).toBe(3);
      expect(entry!.resolver_payload).toMatchObject({ approved: true });
      // Timestamps are preserved — not overwritten with NOW()
      expect(new Date(entry!.created_at).toISOString()).toBe(createdAt.toISOString());
      expect(new Date(entry!.resolved_at!).toISOString()).toBe(resolvedAt.toISOString());
    }, 5_000);

    it('should return null on a second call with the same id (idempotent)', async () => {
      const originalId = uuid();
      await client.escalations.migrate({
        id: originalId,
        type: 'support',
        role: 'agent',
      });

      // Second call — same id — must not throw, must return null.
      const duplicate = await client.escalations.migrate({
        id: originalId,
        type: 'support',
        role: 'agent',
        description: 'This should not overwrite the original',
      });

      expect(duplicate).toBeNull();

      // Original row is untouched — description still null from first insert.
      const row = await client.escalations.get(originalId);
      expect(row?.description).toBeNull();
    }, 5_000);
  });

  describe('claim() isExtension', () => {
    it('fresh claim returns isExtension:false', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { iext: `iext-${guid()}` },
      });
      const result = await client.escalations.claim({ id: esc.id, assignee: 'bob', durationMinutes: 5 });
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.isExtension).toBe(false);
    }, 5_000);

    it('same assignee re-claiming returns isExtension:true and extends assigned_until', async () => {
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { iext2: `iext2-${guid()}` },
      });
      const first = await client.escalations.claim({ id: esc.id, assignee: 'carol', durationMinutes: 1 });
      expect(first.ok).toBe(true);
      if (!first.ok) return;
      const firstExpiry = first.entry.assigned_until;

      const second = await client.escalations.claim({ id: esc.id, assignee: 'carol', durationMinutes: 10 });
      expect(second.ok).toBe(true);
      if (!second.ok) return;
      expect(second.isExtension).toBe(true);
      // Expiry is extended
      expect(new Date(second.entry.assigned_until!).getTime()).toBeGreaterThan(
        new Date(firstExpiry!).getTime(),
      );
    }, 5_000);
  });

  describe('claimByMetadata() metadata merge', () => {
    it('should merge metadata into the claimed row in the same statement', async () => {
      const key = guid();
      await client.escalations.create({
        type: 'support',
        role: 'agent',
        metadata: { orderId: key, existing: 'value' },
      });

      const result = await client.escalations.claimByMetadata({
        key: 'orderId',
        value: key,
        assignee: 'dave',
        durationMinutes: 5,
        metadata: { claimedBy: 'dave', workstation: 'ws-1' },
      });

      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.entry.metadata).toMatchObject({
        orderId: key,
        existing: 'value',
        claimedBy: 'dave',
        workstation: 'ws-1',
      });

      // Persisted — re-read confirms the merge
      const row = await client.escalations.get(result.entry.id);
      expect(row?.metadata).toMatchObject({ claimedBy: 'dave', workstation: 'ws-1' });
    }, 5_000);
  });

  describe('list()/count() new filters', () => {
    let ids: string[];
    const filterType = `filter-test-${guid()}`;

    beforeAll(async () => {
      // Create 3 rows: priorities 1, 2, 3; different metadata and task_ids
      const a = await client.escalations.create({ type: filterType, role: 'agent', priority: 3, metadata: { tag: 'alpha' }, taskId: `task-a-${guid()}` });
      const b = await client.escalations.create({ type: filterType, role: 'agent', priority: 1, metadata: { tag: 'beta' } });
      const c = await client.escalations.create({ type: filterType, role: 'agent', priority: 2, metadata: { tag: 'alpha' } });
      ids = [a.id, b.id, c.id];
    });

    it('orderBy: priority ASC, created_at ASC returns lowest priority first', async () => {
      const rows = await client.escalations.list({
        type: filterType,
        orderBy: [{ column: 'priority', direction: 'asc' }, { column: 'created_at', direction: 'asc' }],
      });
      expect(rows[0].priority).toBe(1);
      expect(rows[1].priority).toBe(2);
      expect(rows[2].priority).toBe(3);
    }, 5_000);

    it('priority filter returns only exact match', async () => {
      const rows = await client.escalations.list({ type: filterType, priority: 2 });
      expect(rows.length).toBe(1);
      expect(rows[0].priority).toBe(2);
    }, 5_000);

    it('metadata containment filter narrows correctly', async () => {
      const rows = await client.escalations.list({ type: filterType, metadata: { tag: 'alpha' } });
      expect(rows.length).toBe(2);
      rows.forEach(r => expect((r.metadata as any).tag).toBe('alpha'));
    }, 5_000);

    it('ids filter returns only specified rows', async () => {
      const rows = await client.escalations.list({ ids: [ids[0], ids[2]] });
      expect(rows.length).toBe(2);
      const returnedIds = rows.map(r => r.id).sort();
      expect(returnedIds).toEqual([ids[0], ids[2]].sort());
    }, 5_000);

    it('count() respects the same filters', async () => {
      const total = await client.escalations.count({ type: filterType });
      expect(total).toBe(3);
      const filtered = await client.escalations.count({ type: filterType, priority: 1 });
      expect(filtered).toBe(1);
    }, 5_000);
  });

  describe('task_id column', () => {
    it('create with taskId → get() returns task_id, list({ taskId }) finds it', async () => {
      const taskId = `task-${guid()}`;
      const esc = await client.escalations.create({
        type: 'support',
        role: 'agent',
        taskId,
        metadata: { taskTest: taskId },
      });

      // Field is round-tripped
      expect(esc.task_id).toBe(taskId);
      const fetched = await client.escalations.get(esc.id);
      expect(fetched?.task_id).toBe(taskId);

      // Filter works
      const rows = await client.escalations.list({ taskId });
      expect(rows.length).toBeGreaterThanOrEqual(1);
      expect(rows.every(r => r.task_id === taskId)).toBe(true);
    }, 5_000);

    it('omitting taskId yields task_id:null', async () => {
      const esc = await client.escalations.create({ type: 'support', role: 'agent' });
      expect(esc.task_id).toBeNull();
    }, 5_000);
  });

  describe('bulk operations', () => {
    const bulkType = `bulk-${guid()}`;
    let bulkIds: string[];

    beforeAll(async () => {
      const rows = await Promise.all([
        client.escalations.create({ type: bulkType, role: 'agent', priority: 5 }),
        client.escalations.create({ type: bulkType, role: 'agent', priority: 5 }),
        client.escalations.create({ type: bulkType, role: 'agent', priority: 5 }),
      ]);
      bulkIds = rows.map(r => r.id);
    });

    it('claimMany: claims eligible rows and reports skipped', async () => {
      const result = await client.escalations.claimMany({
        ids: bulkIds,
        assignee: 'bulk-user',
        durationMinutes: 5,
      });
      expect(result.claimed).toBe(3);
      expect(result.skipped).toBe(0);
    }, 5_000);

    it('claimMany: skips rows under active foreign claim', async () => {
      // bulkIds[0] is claimed by 'bulk-user'; try claiming with different assignee
      const result = await client.escalations.claimMany({
        ids: [bulkIds[0]],
        assignee: 'other-user',
        durationMinutes: 5,
      });
      expect(result.claimed).toBe(0);
      expect(result.skipped).toBe(1);
    }, 5_000);

    it('updateManyPriority: updates only pending rows', async () => {
      const count = await client.escalations.updateManyPriority({
        ids: bulkIds,
        priority: 1,
      });
      expect(count).toBe(3);
      const rows = await client.escalations.list({ ids: bulkIds });
      rows.forEach(r => expect(r.priority).toBe(1));
    }, 5_000);

    it('escalateManyToRole: reassigns and clears claims', async () => {
      const count = await client.escalations.escalateManyToRole({
        ids: bulkIds,
        targetRole: 'supervisor',
      });
      expect(count).toBe(3);
      const rows = await client.escalations.list({ ids: bulkIds });
      rows.forEach(r => {
        expect(r.role).toBe('supervisor');
        expect(r.assigned_to).toBeNull();
      });
    }, 5_000);

    it('resolveMany: returns resolved rows; no signal delivery', async () => {
      const resolved = await client.escalations.resolveMany({
        ids: bulkIds,
        resolverPayload: { bulk: true },
      });
      expect(resolved.length).toBe(3);
      resolved.forEach(r => {
        expect(r.status).toBe('resolved');
        expect((r.resolver_payload as any)?.bulk).toBe(true);
      });

      // Verify persisted
      const rows = await client.escalations.list({ ids: bulkIds });
      rows.forEach(r => expect(r.status).toBe('resolved'));
    }, 5_000);
  });

  describe('stats() + listDistinctTypes()', () => {
    const statsType = `stats-${guid()}`;

    beforeAll(async () => {
      await Promise.all([
        client.escalations.create({ type: statsType, role: 'agent', priority: 5 }),
        client.escalations.create({ type: statsType, role: 'manager', priority: 3 }),
        client.escalations.create({ type: statsType, role: 'agent', priority: 2 }),
      ]);
    });

    it('stats() returns non-negative counts for pending rows', async () => {
      const s = await client.escalations.stats();
      expect(s.pending).toBeGreaterThanOrEqual(3);
      expect(s.claimed).toBeGreaterThanOrEqual(0);
      expect(s.created).toBeGreaterThanOrEqual(0);
      expect(s.resolved).toBeGreaterThanOrEqual(0);
      expect(Array.isArray(s.by_role)).toBe(true);
      expect(Array.isArray(s.by_type)).toBe(true);
    }, 5_000);

    it('stats() with roles=[] returns all-zero result', async () => {
      const s = await client.escalations.stats({ roles: [] });
      expect(s.pending).toBe(0);
      expect(s.claimed).toBe(0);
      expect(s.by_role).toHaveLength(0);
      expect(s.by_type).toHaveLength(0);
    }, 5_000);

    it('listDistinctTypes() includes the stats test type', async () => {
      const types = await client.escalations.listDistinctTypes();
      expect(types).toContain(statsType);
      // Should be sorted
      const sorted = [...types].sort();
      expect(types).toEqual(sorted);
    }, 5_000);
  });

  describe('stats() bounded-source semantics', () => {
    // Unique role/type names isolate these rows from everything else the
    // suite creates, so exact-count assertions hold.
    const roleA = `stats-role-a-${guid()}`;
    const roleB = `stats-role-b-${guid()}`;
    const typeX = `stats-type-x-${guid()}`;
    const typeY = `stats-type-y-${guid()}`;

    beforeAll(async () => {
      // roleA/typeX: two pending, one of them claimed.
      // roleB/typeY: one row, resolved immediately (backlog empty, window hit).
      const [a1, , b1] = await Promise.all([
        client.escalations.create({ type: typeX, role: roleA, priority: 5 }),
        client.escalations.create({ type: typeX, role: roleA, priority: 5 }),
        client.escalations.create({ type: typeY, role: roleB, priority: 5 }),
      ]);
      await client.escalations.claim({
        id: a1.id,
        assignee: 'stats-agent',
        durationMinutes: 10,
      });
      await client.escalations.resolve({ id: b1.id, resolverPayload: { ok: true } });
    }, 10_000);

    it('scopes every count to the roles filter and detects active claims', async () => {
      const s = await client.escalations.stats({ roles: [roleA] });
      expect(s.pending).toBe(2);
      expect(s.claimed).toBe(1);
      expect(s.created).toBe(2);
      expect(s.resolved).toBe(0);
      expect(s.by_role).toEqual([{ role: roleA, pending: 2, claimed: 1 }]);
      expect(s.by_type).toEqual([
        { type: typeX, pending: 2, claimed: 1, resolved: 0 },
      ]);
    }, 5_000);

    it('counts window-resolved rows into totals and by_type', async () => {
      const s = await client.escalations.stats({ roles: [roleB] });
      expect(s.pending).toBe(0);
      expect(s.claimed).toBe(0);
      expect(s.created).toBe(1);
      expect(s.resolved).toBe(1);
      expect(s.by_type).toEqual([
        { type: typeY, pending: 0, claimed: 0, resolved: 1 },
      ]);
    }, 5_000);

    it('by_role reflects the live backlog: roles with no pending rows emit no entry', async () => {
      const s = await client.escalations.stats({ roles: [roleA, roleB] });
      expect(s.by_role).toEqual([{ role: roleA, pending: 2, claimed: 1 }]);
      expect(s.pending).toBe(2);
      expect(s.resolved).toBe(1);
    }, 5_000);
  });

  describe('SLA-gated wait — condition(signalId, { ...config, timeout })', () => {
    const findRow = async (orderId: string) => {
      for (let i = 0; i < 30; i++) {
        const list = await client.escalations.list({ role: 'sla-approver' });
        const esc = list.find((e) => (e.metadata as any)?.orderId === orderId);
        if (esc) return esc;
        await sleepFor(500);
      }
      return undefined;
    };

    it('timer fires first: workflow resumes false and the row expires', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '3 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaGatedWorkflow',
        workflowId: guid(),
      });

      // One wait produced BOTH artifacts: the pending worklist row...
      const esc = await findRow(orderId);
      expect(esc).toBeDefined();
      expect(esc!.status).toBe('pending');
      expect(esc!.signal_key).not.toBeNull();

      // ...and the armed resume timer: the workflow comes back with false.
      const output = await h.result();
      expect((output as any).outcome).toBe('timed-out');

      // The engine transitioned the row pending → expired in the timeout path.
      let row = esc;
      for (let i = 0; i < 30; i++) {
        [row] = await client.escalations.list({ ids: [esc!.id] });
        if (row?.status === 'expired') break;
        await sleepFor(500);
      }
      expect(row!.status).toBe('expired');

      // A post-deadline resolve is refused by name — the payload would have
      // nowhere to go, and the operator must learn the SLA already fired.
      const late = await client.escalations.resolve({
        id: esc!.id,
        resolverPayload: { approved: true },
      });
      expect(late.ok).toBe(false);
      if (late.ok === false) expect(late.reason).toBe('already-expired');
    }, 60_000);

    it('signal first: payload delivered, row resolved, and the timer is inert', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '90 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaGatedWorkflow',
        workflowId: guid(),
      });

      const esc = await findRow(orderId);
      expect(esc).toBeDefined();

      const resolved = await client.escalations.resolve({
        id: esc!.id,
        resolverPayload: { approved: true, approvedBy: 'sla-reviewer' },
      });
      expect(resolved.ok).toBe(true);

      const output = await h.result();
      expect((output as any).outcome).toBe('resolved');
      expect((output as any).payload?.approved).toBe(true);

      const [row] = await client.escalations.list({ ids: [esc!.id] });
      expect(row.status).toBe('resolved');
    }, 60_000);
  });

  describe('SLA-gated wait at a dimensional address (execHook)', () => {
    // The waiter here executes below the root — the same topology an
    // interceptor-wrapped host workflow produces. Both legs of the race
    // must behave identically to the root-path suite above.
    const findHookRow = async (orderId: string) => {
      for (let i = 0; i < 30; i++) {
        const list = await client.escalations.list({ role: 'sla-hook-approver' });
        const esc = list.find((e) => (e.metadata as any)?.orderId === orderId);
        if (esc) return esc;
        await sleepFor(500);
      }
      return undefined;
    };

    it('timer fires below the root: workflow resumes false and the row expires', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '3 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaHookGatedWorkflow',
        workflowId: guid(),
      });

      const esc = await findHookRow(orderId);
      expect(esc).toBeDefined();
      expect(esc!.status).toBe('pending');

      const output = await h.result();
      expect((output as any).outcome).toBe('timed-out');

      let row = esc;
      for (let i = 0; i < 30; i++) {
        [row] = await client.escalations.list({ ids: [esc!.id] });
        if (row?.status === 'expired') break;
        await sleepFor(500);
      }
      expect(row!.status).toBe('expired');
    }, 60_000);

    it('the row carries its signal_key at depth (aid-matched hook rule)', async () => {
      // Leg1 derivation must select the hook rule targeting THIS activity —
      // positionally-first rules reference a sibling branch's output and
      // yield key-less (unaddressable) rows for every branch but the first.
      const list = await client.escalations.list({ role: 'sla-hook-approver' });
      expect(list.length).toBeGreaterThan(0);
      expect(list.every((e) => e.signal_key != null)).toBe(true);
    }, 10_000);
  });

  describe('SLA-gated wait at a cycled dimension (the interceptor topology)', () => {
    // Durable operations before the wait advance the main-loop cycle index,
    // so the waiter sits at dad ',0,N,0,0' — the address shape produced by
    // host apps that wrap workflows in an interceptor. Both race outcomes
    // must match the root-path suite exactly.
    const findCycledRow = async (orderId: string) => {
      for (let i = 0; i < 40; i++) {
        const list = await client.escalations.list({ role: 'sla-cycled-approver' });
        const esc = list.find((e) => (e.metadata as any)?.orderId === orderId);
        if (esc) return esc;
        await sleepFor(500);
      }
      return undefined;
    };

    it('timer fires at a cycled address: workflow resumes false and the row expires', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '3 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaCycledWorkflow',
        workflowId: guid(),
      });

      const esc = await findCycledRow(orderId);
      expect(esc).toBeDefined();
      expect(esc!.status).toBe('pending');
      expect(esc!.signal_key).not.toBeNull();

      const output = await h.result();
      expect((output as any).outcome).toBe('timed-out');

      let row = esc;
      for (let i = 0; i < 30; i++) {
        [row] = await client.escalations.list({ ids: [esc!.id] });
        if (row?.status === 'expired') break;
        await sleepFor(500);
      }
      expect(row!.status).toBe('expired');
    }, 90_000);

    it('signal first at a cycled address: payload delivered, row resolved, timer inert', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '90 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaCycledWorkflow',
        workflowId: guid(),
      });

      const esc = await findCycledRow(orderId);
      expect(esc).toBeDefined();

      const resolved = await client.escalations.resolve({
        id: esc!.id,
        resolverPayload: { approved: true },
      });
      expect(resolved.ok).toBe(true);

      const output = await h.result();
      expect((output as any).outcome).toBe('resolved');
      expect((output as any).payload?.approved).toBe(true);

      const [row] = await client.escalations.list({ ids: [esc!.id] });
      expect(row.status).toBe('resolved');
    }, 90_000);
  });

  describe('SLA-gated wait in a collated Promise.all (the collator reentry path)', () => {
    // Two escalation-bearing waits collated together: item A carries the SLA,
    // item B is open-ended. A's timer must settle A alone (false + expired
    // row) at A's per-item dimension, B's row stays live, and Promise.all
    // completes once B resolves through the collator's wfs.signal reentry.
    const findItem = async (orderId: string, item: string) => {
      for (let i = 0; i < 40; i++) {
        const list = await client.escalations.list({ role: 'sla-collated-approver' });
        const esc = list.find((e) => {
          const m = e.metadata as any;
          return m?.orderId === orderId && m?.item === item;
        });
        if (esc) return esc;
        await sleepFor(500);
      }
      return undefined;
    };

    it('per-item timer: A expires and settles false, B survives, resolve of B completes the collation', async () => {
      const orderId = guid();
      const h = await client.workflow.start({
        args: [orderId, '4 seconds'],
        taskQueue: 'escalation-test',
        workflowName: 'slaCollatedWorkflow',
        workflowId: guid(),
      });

      const escA = await findItem(orderId, 'a');
      const escB = await findItem(orderId, 'b');
      expect(escA).toBeDefined();
      expect(escB).toBeDefined();
      expect(escA!.signal_key).not.toBeNull();

      // A's timer fires: A's row expires; B's row is untouched and claimable.
      let rowA = escA;
      for (let i = 0; i < 30; i++) {
        [rowA] = await client.escalations.list({ ids: [escA!.id] });
        if (rowA?.status === 'expired') break;
        await sleepFor(500);
      }
      expect(rowA!.status).toBe('expired');
      const [rowBWhileWaiting] = await client.escalations.list({ ids: [escB!.id] });
      expect(rowBWhileWaiting.status).toBe('pending');

      // Resolve B — the collation completes with A=false, B=payload.
      const resolved = await client.escalations.resolve({
        id: escB!.id,
        resolverPayload: { approved: true },
      });
      expect(resolved.ok).toBe(true);

      const output = await h.result();
      expect((output as any).a).toBe('timed-out');
      expect((output as any).b).toBe('resolved');
      expect((output as any).payloadB?.approved).toBe(true);
    }, 120_000);
  });
});
