/**
 * Proves that migrate() correctly handles every supported upgrade path.
 *
 * Each scenario manipulates the live schema to match a specific "old" version,
 * then calls kvTables.deploy() a second time to exercise migrate() (not
 * createTables() — tablesExist is true throughout, so only migrate() runs).
 *
 * Scenarios covered:
 *   A — pre-0.22.x: hmsh_escalations absent; jobs missing origin_id/parent_id
 *   B — 0.22.0:     idx_hmsh_esc_assigned has wrong 'claimed' predicate;
 *                   idx_hmsh_esc_claim_expiry still exists; task_id absent
 *   C — 0.22.2:     task_id column and idx_hmsh_esc_task absent
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh } from '../../../index';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

// safeName('migration-test') = 'migration_test'
const APP_ID = 'migration-test';
const APP_SCHEMA = 'migration_test';

describe('DURABLE | migrate() upgrade paths | Postgres', () => {
  let hotMesh: HotMesh;
  let store: any;
  let pg: any; // dedicated raw client for DDL manipulation and schema verification

  // ─── Schema query helpers ───────────────────────────────────────────────────

  /** Returns true if the column exists on the given table. */
  const col = async (
    schema: string,
    table: string,
    column: string,
  ): Promise<boolean> => {
    const { rows } = await pg.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 LIMIT 1`,
      [schema, table, column],
    );
    return rows.length > 0;
  };

  /** Returns the indexdef string for the named index, or null if absent. */
  const idx = async (
    name: string,
    schema = 'public',
  ): Promise<string | null> => {
    const { rows } = await pg.query(
      `SELECT indexdef FROM pg_indexes
       WHERE indexname = $1 AND schemaname = $2 LIMIT 1`,
      [name, schema],
    );
    return rows.length > 0 ? rows[0].indexdef : null;
  };

  /** Returns true if the table exists. */
  const tableExists = async (
    schema: string,
    table: string,
  ): Promise<boolean> => {
    const { rows } = await pg.query(
      `SELECT to_regclass($1) AS t`,
      [`${schema}.${table}`],
    );
    return rows[0].t !== null;
  };

  // ─── Suite setup ────────────────────────────────────────────────────────────

  beforeAll(async () => {
    pg = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(pg);

    // Initial deploy: creates all tables + indexes via createTables().
    hotMesh = await HotMesh.init({
      appId: APP_ID,
      logLevel: HMSH_LOGLEVEL,
      engine: { connection: { class: Postgres, options: postgres_options } },
    });
    store = hotMesh.engine.store as any;
  }, 20_000);

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  // ─── Scenario A: simulate upgrade from pre-0.22.x ──────────────────────────
  // hmsh_escalations did not exist before 0.22.x. The per-app jobs table lacked
  // origin_id and parent_id. tablesExist() returns true (the checked tables all
  // exist), so createTables() is skipped and only migrate() runs.

  describe('Scenario A — pre-0.22.x: hmsh_escalations absent, jobs missing lineage columns', () => {
    it('sets up old state: drop hmsh_escalations and lineage columns', async () => {
      await pg.query(`DROP TABLE IF EXISTS public.hmsh_escalations CASCADE;`);
      await pg.query(
        `ALTER TABLE ${APP_SCHEMA}.jobs DROP COLUMN IF EXISTS origin_id;`,
      );
      await pg.query(
        `ALTER TABLE ${APP_SCHEMA}.jobs DROP COLUMN IF EXISTS parent_id;`,
      );

      expect(await tableExists('public', 'hmsh_escalations')).toBe(false);
      expect(await col(APP_SCHEMA, 'jobs', 'origin_id')).toBe(false);
      expect(await col(APP_SCHEMA, 'jobs', 'parent_id')).toBe(false);
    });

    it('re-deploy() runs migrate() and recovers all missing schema', async () => {
      await store.kvTables.deploy(APP_ID);
    }, 15_000);

    it('hmsh_escalations table is recreated', async () => {
      expect(await tableExists('public', 'hmsh_escalations')).toBe(true);
    });

    it('task_id column exists on hmsh_escalations', async () => {
      expect(await col('public', 'hmsh_escalations', 'task_id')).toBe(true);
    });

    it('idx_hmsh_esc_signal_key (unique) exists', async () => {
      expect(await idx('idx_hmsh_esc_signal_key')).not.toBeNull();
    });

    it('idx_hmsh_esc_available exists with status=pending predicate', async () => {
      const def = await idx('idx_hmsh_esc_available');
      expect(def).not.toBeNull();
      expect(def).toContain("'pending'");
    });

    it('idx_hmsh_esc_assigned exists with status=pending predicate', async () => {
      const def = await idx('idx_hmsh_esc_assigned');
      expect(def).not.toBeNull();
      expect(def).toContain("'pending'");
      expect(def).not.toContain("'claimed'");
    });

    it('idx_hmsh_esc_claim_expiry does NOT exist (dropped in 0.22.2)', async () => {
      expect(await idx('idx_hmsh_esc_claim_expiry')).toBeNull();
    });

    it('idx_hmsh_esc_metadata GIN index exists', async () => {
      const def = await idx('idx_hmsh_esc_metadata');
      expect(def).not.toBeNull();
      expect(def!.toLowerCase()).toContain('gin');
    });

    it('idx_hmsh_esc_task exists', async () => {
      expect(await idx('idx_hmsh_esc_task')).not.toBeNull();
    });

    it('idx_hmsh_esc_stats_pending exists with status=pending predicate (0.25.0)', async () => {
      const def = await idx('idx_hmsh_esc_stats_pending');
      expect(def).not.toBeNull();
      expect(def).toContain("'pending'");
      expect(def).toContain('role, type, assigned_to, assigned_until, namespace');
    });

    it('idx_hmsh_esc_stats_created exists led by created_at (0.25.0)', async () => {
      const def = await idx('idx_hmsh_esc_stats_created');
      expect(def).not.toBeNull();
      expect(def).toContain('created_at, role, namespace');
    });

    it('idx_hmsh_esc_stats_resolved exists with status=resolved predicate (0.25.0)', async () => {
      const def = await idx('idx_hmsh_esc_stats_resolved');
      expect(def).not.toBeNull();
      expect(def).toContain("'resolved'");
      expect(def).toContain('resolved_at, type, role, namespace');
    });

    it('origin_id column added to jobs', async () => {
      expect(await col(APP_SCHEMA, 'jobs', 'origin_id')).toBe(true);
    });

    it('parent_id column added to jobs', async () => {
      expect(await col(APP_SCHEMA, 'jobs', 'parent_id')).toBe(true);
    });

    it('idx_migration_test_jobs_origin exists', async () => {
      expect(
        await idx('idx_migration_test_jobs_origin', APP_SCHEMA),
      ).not.toBeNull();
    });

    it('idx_migration_test_jobs_parent exists', async () => {
      expect(
        await idx('idx_migration_test_jobs_parent', APP_SCHEMA),
      ).not.toBeNull();
    });
  });

  // ─── Scenario B: simulate upgrade from 0.22.0 ──────────────────────────────
  // 0.22.0 used status='claimed' on idx_hmsh_esc_assigned (the implicit-claim
  // model was not yet in place). idx_hmsh_esc_claim_expiry existed. No task_id.

  describe("Scenario B — 0.22.0: wrong 'claimed' predicate, stale claim_expiry index, no task_id", () => {
    it('sets up 0.22.0 state', async () => {
      // Reinstall idx_hmsh_esc_assigned with the old wrong predicate
      await pg.query(`DROP INDEX IF EXISTS idx_hmsh_esc_assigned;`);
      await pg.query(`
        CREATE INDEX idx_hmsh_esc_assigned
          ON public.hmsh_escalations(assigned_to, assigned_until, created_at DESC)
          WHERE status = 'claimed' AND assigned_to IS NOT NULL;
      `);

      // Reinstate the stale claim_expiry sweeper index from 0.22.0
      await pg.query(`
        CREATE INDEX IF NOT EXISTS idx_hmsh_esc_claim_expiry
          ON public.hmsh_escalations(claim_expires_at)
          WHERE status = 'claimed';
      `);

      // Remove task_id (did not exist in 0.22.0)
      await pg.query(
        `ALTER TABLE public.hmsh_escalations DROP COLUMN IF EXISTS task_id;`,
      );
      await pg.query(`DROP INDEX IF EXISTS idx_hmsh_esc_task;`);

      // Verify the degraded state before migration
      const def = await idx('idx_hmsh_esc_assigned');
      expect(def).toContain("'claimed'");
      expect(await idx('idx_hmsh_esc_claim_expiry')).not.toBeNull();
      expect(await col('public', 'hmsh_escalations', 'task_id')).toBe(false);
    });

    it('re-deploy() runs migrate() and fixes all stale state', async () => {
      await store.kvTables.deploy(APP_ID);
    }, 15_000);

    it('idx_hmsh_esc_assigned now has status=pending predicate', async () => {
      const def = await idx('idx_hmsh_esc_assigned');
      expect(def).not.toBeNull();
      expect(def).toContain("'pending'");
      expect(def).not.toContain("'claimed'");
    });

    it('idx_hmsh_esc_claim_expiry is dropped', async () => {
      expect(await idx('idx_hmsh_esc_claim_expiry')).toBeNull();
    });

    it('task_id column is added', async () => {
      expect(await col('public', 'hmsh_escalations', 'task_id')).toBe(true);
    });

    it('idx_hmsh_esc_task is created', async () => {
      expect(await idx('idx_hmsh_esc_task')).not.toBeNull();
    });
  });

  // ─── Scenario C: simulate upgrade from 0.22.2 ──────────────────────────────
  // 0.22.2 had correct index predicates but no task_id column or index.

  describe('Scenario C — 0.22.2: task_id column and idx_hmsh_esc_task absent', () => {
    it('sets up 0.22.2 state: drop task_id and its index', async () => {
      await pg.query(`DROP INDEX IF EXISTS idx_hmsh_esc_task;`);
      await pg.query(
        `ALTER TABLE public.hmsh_escalations DROP COLUMN IF EXISTS task_id;`,
      );

      expect(await col('public', 'hmsh_escalations', 'task_id')).toBe(false);
      expect(await idx('idx_hmsh_esc_task')).toBeNull();
    });

    it('re-deploy() adds task_id column and index', async () => {
      await store.kvTables.deploy(APP_ID);
    }, 10_000);

    it('task_id column is present', async () => {
      expect(await col('public', 'hmsh_escalations', 'task_id')).toBe(true);
    });

    it('idx_hmsh_esc_task is present with correct partial predicate', async () => {
      const def = await idx('idx_hmsh_esc_task');
      expect(def).not.toBeNull();
      expect(def).toContain('task_id');
    });

    it('previously correct indexes are untouched', async () => {
      const def = await idx('idx_hmsh_esc_assigned');
      expect(def).toContain("'pending'");
      expect(await idx('idx_hmsh_esc_claim_expiry')).toBeNull();
    });
  });
});
