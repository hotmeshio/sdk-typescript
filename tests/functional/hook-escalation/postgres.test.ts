/**
 * Proves hmsh_escalations is a HotMesh core primitive.
 *
 * A YAML DAG `hook` activity with an `escalation:` block writes one row to
 * public.hmsh_escalations at suspension time — signal_key = job ID, topic =
 * hook topic. Resolvers call hotMesh.signal(topic, { id: jobId, ...data })
 * to resume the workflow. No Durable layer involved.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import config from '../../$setup/config';
import { HotMesh, HotMeshConfig } from '../../../index';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { JobOutput } from '../../../types/job';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { dropTables } from '../../$setup/postgres';

describe('FUNCTIONAL | Hook Escalation | Postgres', () => {
  const options = {
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    user: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    database: config.POSTGRES_DB,
  };

  let hotMesh: HotMesh;

  beforeAll(async () => {
    const postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, options)
    ).getClient();
    await dropTables(postgresClient);

    const hmshConfig: HotMeshConfig = {
      appId: 'hook-escalation',
      logLevel: HMSH_LOGLEVEL,
      engine: { connection: { class: Postgres, options } },
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy('/app/tests/$setup/apps/hook-escalation/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  }, 15_000);

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  // HotMesh engine uses 'hmsh' as the default namespace (not the appId).
  // All hmsh_escalations rows are stored under namespace='hmsh' unless overridden.
  const NS = 'hmsh';
  const store = () => hotMesh.engine.store as any;

  describe('hmsh_escalations written by raw YAML DAG hook activity', () => {
    let jobId: string;
    const orderId = guid();
    const region = 'us-west';
    let completionResult: JobOutput | null = null;

    it('should start a workflow and return the job ID', async () => {
      jobId = await hotMesh.pub('hook-escalation.test', { orderId, region });
      expect(jobId).toBeDefined();
      await sleepFor(2000);
    }, 10_000);

    it('should have written an hmsh_escalations row with resolved metadata', async () => {
      const esc = await store().getEscalationBySignalKey(jobId, NS);
      expect(esc).not.toBeNull();
      expect(esc.signal_key).toBe(jobId);
      expect(esc.workflow_id).toBe(jobId);
      expect(esc.topic).toBe('hook-escalation.approve');
      expect(esc.status).toBe('pending');
      expect(esc.role).toBe('approver');
      expect(esc.type).toBe('order-approval');
      expect(esc.subtype).toBe('regional');
      expect(esc.priority).toBe(2);
      // metadata @pipe expressions resolved against live job context
      expect(esc.metadata).toMatchObject({ orderId, region });
    }, 5_000);

    it('should list escalations filtered by role', async () => {
      const list = await store().listEscalations({ namespace: NS, role: 'approver' });
      expect(list.length).toBeGreaterThanOrEqual(1);
      const found = list.find((e: any) => e.signal_key === jobId);
      expect(found).toBeDefined();
    }, 5_000);

    it('should claim the escalation', async () => {
      const esc = await store().getEscalationBySignalKey(jobId, NS);
      const result = await store().claimEscalation({
        id: esc.id,
        namespace: NS,
        assignee: 'human-agent-1',
        durationMinutes: 5,
      });
      expect(result.ok).toBe(true);
      expect(result.entry.assigned_to).toBe('human-agent-1');
    }, 5_000);

    it('should resume the workflow by signalling via the hook topic', async () => {
      await hotMesh.sub(`hook-escalation.tested.${jobId}`, (_topic, message: JobOutput) => {
        completionResult = message;
      });

      // Signal payload: id= is used for routing, the rest maps to job output.
      await hotMesh.signal('hook-escalation.approve', {
        id: jobId,
        approved: true,
        approvedBy: 'human-agent-1',
      });

      for (let i = 0; i < 20 && !completionResult; i++) {
        await sleepFor(300);
      }
      await hotMesh.unsub(`hook-escalation.tested.${jobId}`);

      expect(completionResult).not.toBeNull();
      expect((completionResult as any).data?.approved).toBe(true);
      expect((completionResult as any).data?.approvedBy).toBe('human-agent-1');
    }, 15_000);

    it('hmsh_escalations row persists after workflow completes', async () => {
      // Rows are not auto-deleted on signal delivery — they persist for auditing.
      // The row was claimed (assigned_to set) but status stays 'pending' in the
      // implicit claim model; callers resolve() to transition to 'resolved'.
      const esc = await store().getEscalationBySignalKey(jobId, NS);
      expect(esc).not.toBeNull();
      expect(esc.status).toBe('pending');
      expect(esc.assigned_to).toBe('human-agent-1');
    }, 5_000);
  });

  describe('hook activity without escalation block does not write to hmsh_escalations', () => {
    it('should run without creating an hmsh_escalations row when escalation: block is absent', async () => {
      await hotMesh.deploy(`
app:
  id: hook-escalation
  version: '2'
  graphs:
    - subscribes: hook-escalation.test
      publishes: hook-escalation.tested
      expire: 300
      activities:
        t1:
          type: trigger
        a1:
          type: hook
          job:
            maps:
              approved: '{a1.hook.data.approved}'
      transitions:
        t1:
          - to: a1
      hooks:
        hook-escalation.approve:
          - to: a1
            conditions:
              match:
                - expected: '{$job.metadata.jid}'
                  actual: '{$self.hook.data.id}'
`);
      await hotMesh.activate('2');

      const jobId2 = await hotMesh.pub('hook-escalation.test', {});
      await sleepFor(1500);

      const esc = await store().getEscalationBySignalKey(jobId2, NS);
      expect(esc).toBeNull();

      await hotMesh.signal('hook-escalation.approve', { id: jobId2, approved: false });
      await sleepFor(500);
    }, 20_000);
  });

  describe('standalone escalation (no signal_key) can coexist', () => {
    it('should create a manual escalation with null signal_key', async () => {
      const esc = await store().createEscalation({
        namespace: NS,
        appId: 'hook-escalation',
        type: 'support',
        role: 'agent',
        description: 'Manual support request',
        metadata: { ticketId: 'T-123' },
      });
      expect(esc).toBeDefined();
      expect(esc.signal_key).toBeNull();
      expect(esc.type).toBe('support');
      expect(esc.role).toBe('agent');
      expect(esc.status).toBe('pending');
    }, 5_000);
  });
});
