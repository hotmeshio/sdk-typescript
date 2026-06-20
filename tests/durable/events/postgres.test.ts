/**
 * Proves the system-event emission surface introduced in 0.22.4.
 *
 * The event hook must fire exactly once per durable transition, post-commit,
 * with the full committed row as `data` and a collision-proof `event_id`.
 *
 * Scenarios:
 *   A — EscalationClientService standalone: create/claim/release/claim/resolve
 *         proves all 6 verbs fire, event_id is unique across repeated claim verbs
 *   B — Durable.Client.escalations (getHotMeshClient path): events wire through
 *   C — claimMany / resolveMany: per-row emit, counts match
 *   D — engine start fires system.engine.{appId}.started
 *   E — deploy fires system.engine.{appId}.deployed
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { Durable } from '../../../services/durable';
import { EscalationClientService } from '../../../services/escalations/client';
import { HotMesh } from '../../../services/hotmesh';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { SystemEvent } from '../../../types/system_events';

const { Client } = Durable;
const APP_ID = 'events-test';
const connection = { class: Postgres, options: postgres_options };

describe('DURABLE | system-event emission | Postgres', () => {
  let pgRaw: any;

  beforeAll(async () => {
    pgRaw = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(pgRaw);

    // Boot HotMesh once so the schema is deployed (no events hook here — tested separately).
    const hm = await HotMesh.init({
      appId: APP_ID,
      logLevel: HMSH_LOGLEVEL,
      engine: { connection },
    });
    hm.stop();
  }, 30_000);

  afterAll(async () => {
    await Durable.shutdown();
    await HotMesh.stop();
  });

  // ─── Scenario A: EscalationClientService standalone events ─────────────────

  describe('Scenario A — EscalationClientService standalone all 6 verbs', () => {
    const collected: SystemEvent[] = [];
    let client: EscalationClientService;
    let escalationId: string;

    it('constructs client with events hook', () => {
      client = new EscalationClientService({
        connection,
        events: { publish: (e) => { collected.push(e); } },
      });
      expect(client).toBeTruthy();
    });

    it('create fires system.escalation.*.created', async () => {
      const before = collected.length;
      const entry = await client.create({
        namespace: APP_ID,
        role: 'agent',
        type: 'test-ticket',
        priority: 3,
        metadata: { ticketId: guid() },
      });
      escalationId = entry.id;
      expect(collected.length).toBe(before + 1);
      const evt = collected[collected.length - 1];
      expect(evt.type).toBe(`system.escalation.${escalationId}.created`);
      expect(evt.namespace).toBe(APP_ID);
      expect((evt.data as any).id).toBe(escalationId);
    }, 10_000);

    it('claim fires system.escalation.*.claimed', async () => {
      const before = collected.length;
      const result = await client.claim({ id: escalationId, assignee: 'alice', durationMinutes: 1, namespace: APP_ID });
      expect(result.ok).toBe(true);
      expect(collected.length).toBe(before + 1);
      const evt = collected[collected.length - 1];
      expect(evt.type).toBe(`system.escalation.${escalationId}.claimed`);
    }, 10_000);

    it('release fires system.escalation.*.released', async () => {
      const before = collected.length;
      const result = await client.release({ id: escalationId, namespace: APP_ID });
      expect(result.ok).toBe(true);
      expect(collected.length).toBe(before + 1);
      const evt = collected[collected.length - 1];
      expect(evt.type).toBe(`system.escalation.${escalationId}.released`);
    }, 10_000);

    it('re-claim gets a different event_id (claim→release→claim collision-proof)', async () => {
      const claimEvts = collected.filter(e => e.type.endsWith('.claimed'));
      expect(claimEvts.length).toBeGreaterThanOrEqual(1);
      const before = collected.length;
      const result = await client.claim({ id: escalationId, assignee: 'bob', durationMinutes: 1, namespace: APP_ID });
      expect(result.ok).toBe(true);
      expect(collected.length).toBe(before + 1);
      const newClaim = collected[collected.length - 1];
      const oldClaim = claimEvts[claimEvts.length - 1];
      // event_ids must differ because updated_at changed
      expect(newClaim.event_id).not.toBe(oldClaim.event_id);
      expect(newClaim.type).toBe(`system.escalation.${escalationId}.claimed`);
    }, 10_000);

    it('resolve fires system.escalation.*.resolved', async () => {
      const before = collected.length;
      const result = await client.resolve({ id: escalationId, resolverPayload: { verdict: 'done' }, namespace: APP_ID });
      expect(result.ok).toBe(true);
      expect(collected.length).toBe(before + 1);
      const evt = collected[collected.length - 1];
      expect(evt.type).toBe(`system.escalation.${escalationId}.resolved`);
      expect((evt.data as any).status).toBe('resolved');
    }, 10_000);

    it('cancel fires system.escalation.*.cancelled on a new pending row', async () => {
      const entry = await client.create({ namespace: APP_ID, role: 'agent', type: 'cancel-test' });
      const before = collected.length;
      const result = await client.cancel(entry.id, APP_ID);
      expect(result.ok).toBe(true);
      expect(collected.length).toBe(before + 1);
      const evt = collected[collected.length - 1];
      expect(evt.type).toBe(`system.escalation.${entry.id}.cancelled`);
    }, 10_000);

    it('no ok=false result emits an event', async () => {
      const before = collected.length;
      // Claim something that doesn't exist
      await client.claim({ id: '00000000-0000-0000-0000-000000000000', assignee: 'ghost', namespace: APP_ID });
      expect(collected.length).toBe(before); // no event
    }, 10_000);

    it('event data carries the full row', () => {
      const evt = collected.find(e => e.type.endsWith('.resolved'));
      expect(evt).toBeTruthy();
      const row = evt!.data as any;
      expect(row.id).toBeTruthy();
      expect(row.namespace).toBe(APP_ID);
      expect(row.status).toBe('resolved');
      expect(row.resolver_payload).toEqual({ verdict: 'done' });
    });

    it('event_id format is {id}:{verb}:{updated_at}', () => {
      const evt = collected.find(e => e.type.endsWith('.created'));
      expect(evt).toBeTruthy();
      const parts = evt!.event_id.split(':');
      // UUID has 5 parts, total: uuid(5 parts via hyphen) + verb + iso → check starts with escalationId
      expect(evt!.event_id.startsWith(escalationId + ':created:')).toBe(true);
    });
  });

  // ─── Scenario B: Durable.Client.escalations (getHotMeshClient path) ────────

  describe('Scenario B — Durable.Client.escalations wires events', () => {
    const collected: SystemEvent[] = [];
    let durableClient: InstanceType<typeof Client>;

    it('constructs Durable.Client with events', () => {
      durableClient = new Client({
        connection,
        events: { publish: (e) => { collected.push(e); } },
      });
      expect(durableClient.escalations).toBeTruthy();
    });

    it('create via Durable.Client.escalations fires event', async () => {
      const entry = await durableClient.escalations.create({
        namespace: APP_ID,
        role: 'supervisor',
        type: 'durable-client-test',
      });
      const evt = collected.find(e => e.type.includes(entry.id));
      expect(evt).toBeTruthy();
      expect(evt!.type).toBe(`system.escalation.${entry.id}.created`);
    }, 10_000);
  });

  // ─── Scenario C: bulk ops per-row emit ─────────────────────────────────────

  describe('Scenario C — claimMany / resolveMany emit per affected row', () => {
    const collected: SystemEvent[] = [];
    let client: EscalationClientService;
    const ids: string[] = [];

    beforeAll(async () => {
      client = new EscalationClientService({
        connection,
        events: { publish: (e) => { collected.push(e); } },
      });
      // Create 3 rows
      for (let i = 0; i < 3; i++) {
        const e = await client.create({ namespace: APP_ID, role: 'bulk-tester', type: 'bulk' });
        ids.push(e.id);
      }
    }, 20_000);

    it('claimMany emits one event per claimed row', async () => {
      const before = collected.filter(e => e.type.endsWith('.claimed')).length;
      const result = await client.claimMany({ ids, assignee: 'batch-worker', durationMinutes: 5, namespace: APP_ID });
      expect(result.claimed).toBe(3);
      expect(result.skipped).toBe(0);
      const after = collected.filter(e => e.type.endsWith('.claimed')).length;
      expect(after - before).toBe(3);
    }, 10_000);

    it('claimMany skipped rows emit nothing', async () => {
      // Pass a non-existent id as well as already-claimed rows (same assignee extends, so claimed=3)
      const fakeId = '00000000-0000-0000-0000-000000000001';
      const before = collected.length;
      const result = await client.claimMany({ ids: [fakeId], assignee: 'nobody', durationMinutes: 1, namespace: APP_ID });
      expect(result.claimed).toBe(0);
      expect(result.skipped).toBe(1);
      expect(collected.length).toBe(before); // no event
    }, 10_000);

    it('resolveMany emits one event per resolved row', async () => {
      const before = collected.filter(e => e.type.endsWith('.resolved')).length;
      const entries = await client.resolveMany({ ids, resolverPayload: { done: true }, namespace: APP_ID });
      expect(entries.length).toBe(3);
      const after = collected.filter(e => e.type.endsWith('.resolved')).length;
      expect(after - before).toBe(3);
    }, 10_000);
  });

  // ─── Scenario D: engine.started fires on HotMesh.init ──────────────────────

  describe('Scenario D — system.engine.{appId}.started fires on init', () => {
    const collected: SystemEvent[] = [];

    it('HotMesh.init with events fires engine.started', async () => {
      const appId = `evt-engine-${guid().slice(0, 8)}`;
      const hm = await HotMesh.init({
        appId,
        logLevel: HMSH_LOGLEVEL,
        engine: { connection },
        events: { publish: (e) => { collected.push(e); } },
      });
      hm.stop();

      const started = collected.find(e => e.type === `system.engine.${appId}.started`);
      expect(started).toBeTruthy();
      expect(started!.app_id).toBe(appId);
      expect(started!.data).toMatchObject({ appId });
    }, 20_000);
  });

  // ─── Scenario E: engine.deployed fires on kvTables.deploy ──────────────────

  describe('Scenario E — system.engine.{appId}.deployed fires on deploy', () => {
    const collected: SystemEvent[] = [];

    it('deploy fires system.engine.{appId}.deployed', async () => {
      const appId = `evt-deploy-${guid().slice(0, 8)}`;
      const hm = await HotMesh.init({
        appId,
        logLevel: HMSH_LOGLEVEL,
        engine: { connection },
        events: { publish: (e) => { collected.push(e); } },
      });

      // Trigger deploy explicitly (happens during init anyway, but re-deploying is idempotent)
      await (hm.engine.store as any).kvTables.deploy(appId);
      hm.stop();

      const deployed = collected.find(e => e.type === `system.engine.${appId}.deployed`);
      expect(deployed).toBeTruthy();
      expect(deployed!.app_id).toBe(appId);
    }, 20_000);
  });
});
