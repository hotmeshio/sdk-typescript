import {
  HMSH_DEPLOYMENT_DELAY,
  HMSH_DEPLOYMENT_PAUSE,
} from '../../../../modules/enums';
import { sleepFor } from '../../../../modules/utils';
import {
  PostgresClientType,
  PostgresPoolClientType,
} from '../../../../types/postgres';

import type { PostgresStoreService } from './postgres';

export const KVTables = (context: PostgresStoreService) => ({
  /**
   * Deploys the necessary tables with the specified naming strategy.
   * @param appName - The name of the application.
   */
  async deploy(appName: string): Promise<void> {
    const transactionClient = context.pgClient as any;

    let client: any;
    let releaseClient = false;

    if (
      transactionClient?.totalCount !== undefined &&
      transactionClient?.idleCount !== undefined
    ) {
      // It's a Pool, need to acquire a client
      client = await (transactionClient as PostgresPoolClientType).connect();
      releaseClient = true;
    } else {
      // Assume it's a connected Client
      client = transactionClient as PostgresClientType;
    }

    try {
      const tablesExist = await this.checkIfTablesExist(client, appName);

      // Acquire advisory lock for ALL DDL: table creation and
      // migrations. CREATE INDEX IF NOT EXISTS is not atomic under
      // concurrent transactions — two sessions can both see the
      // index as absent and both attempt creation, causing a
      // unique_violation on pg_class_relname_nsp_index.
      const lockId = this.getAdvisoryLockId(appName);
      const lockResult = await client.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [lockId],
      );

      if (lockResult.rows[0].locked) {
        try {
          if (!tablesExist) {
            // Begin transaction
            await client.query('BEGIN');

            // Double-check tables don't exist (race condition safety)
            const tablesStillMissing = !(await this.checkIfTablesExist(
              client,
              appName,
            ));
            if (tablesStillMissing) {
              await this.createTables(client, appName);
            }

            // Commit transaction
            await client.query('COMMIT');
          }

          // Always run migrations under the lock
          await this.migrate(client, appName);
        } finally {
          await client.query('SELECT pg_advisory_unlock($1)', [lockId]);
        }
      } else {
        // Release the client before waiting
        if (releaseClient && client.release) {
          await client.release();
          releaseClient = false;
        }

        // Wait for the deploy process to complete
        await this.waitForTablesCreation(lockId, appName);
      }
    } catch (error) {
      console.error(error);
      context.logger.error('Error deploying tables', { error });
      throw error;
    } finally {
      if (releaseClient && client.release) {
        await client.release();
      }
    }

    // Fire system.engine.{appId}.deployed post-deploy (best-effort).
    if (context.eventsPublish) {
      const ts = new Date().toISOString();
      void Promise.resolve(context.eventsPublish({
        event_id: `${appName}:deployed:${ts}`,
        type: `system.engine.${appName}.deployed`,
        ts,
        namespace: appName,
        app_id: appName,
        data: { appId: appName },
      })).catch(() => { /* best-effort */ });
    }
  },

  getAdvisoryLockId(appName: string): number {
    return this.hashStringToInt(appName);
  },

  hashStringToInt(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0; // Convert to 32-bit integer
    }
    return Math.abs(hash);
  },

  async waitForTablesCreation(lockId: number, appName: string): Promise<void> {
    let retries = 0;
    const maxRetries = Math.round(
      HMSH_DEPLOYMENT_DELAY / HMSH_DEPLOYMENT_PAUSE,
    );

    while (retries < maxRetries) {
      await sleepFor(HMSH_DEPLOYMENT_PAUSE);

      let client: any;
      let releaseClient = false;
      const transactionClient = context.pgClient as any;

      if (
        transactionClient?.totalCount !== undefined &&
        transactionClient?.idleCount !== undefined
      ) {
        // It's a Pool, need to acquire a client
        client = await (transactionClient as PostgresPoolClientType).connect();
        releaseClient = true;
      } else {
        // Assume it's a connected Client
        client = transactionClient as PostgresClientType;
      }

      try {
        // Check if tables exist directly (most efficient check)
        const tablesExist = await this.checkIfTablesExist(client, appName);
        if (tablesExist) {
          // Tables now exist, deployment is complete
          return;
        }

        // Fallback: check if the lock has been released (indicates completion)
        const lockCheck = await client.query(
          "SELECT NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint) AS unlocked",
          [lockId],
        );
        if (lockCheck.rows[0].unlocked) {
          // Lock has been released, tables should exist now
          const tablesExistAfterLock = await this.checkIfTablesExist(
            client,
            appName,
          );
          if (tablesExistAfterLock) {
            return;
          }
        }
      } finally {
        if (releaseClient && client.release) {
          await client.release();
        }
      }

      retries++;
    }
    console.error('table-create-timeout', { appName });
    throw new Error('Timeout waiting for table creation');
  },

  async checkIfTablesExist(
    client: PostgresClientType,
    appName: string,
  ): Promise<boolean> {
    const tableNames = this.getTableNames(appName);

    const checkTablePromises = tableNames.map((tableName) =>
      client.query(`SELECT to_regclass('${tableName}') AS table`),
    );

    const results = await Promise.all(checkTablePromises);
    return results.every((res) => res.rows[0].table !== null);
  },

  async migrate(
    client: PostgresClientType | PostgresPoolClientType,
    appName: string,
  ): Promise<void> {
    const schemaName = context.storeClient.safeName(appName);
    const jobsTable = `${schemaName}.jobs`;

    // v0.14.5: track updated_at on job status changes
    const { rows } = await client.query(
      `SELECT 1 FROM pg_trigger WHERE tgname = 'trg_update_jobs_updated_at' LIMIT 1`,
    );
    if (rows.length === 0) {
      await client.query(`
        CREATE OR REPLACE FUNCTION ${schemaName}.update_jobs_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
          IF NEW.status <> OLD.status THEN
            NEW.updated_at = NOW();
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `);
      await client.query(`
        DROP TRIGGER IF EXISTS trg_update_jobs_updated_at ON ${jobsTable};
        CREATE TRIGGER trg_update_jobs_updated_at
        BEFORE UPDATE ON ${jobsTable}
        FOR EACH ROW EXECUTE FUNCTION ${schemaName}.update_jobs_updated_at();
      `);
    }

    // v0.17.6: add dedicated is_live partial index for hot-path queries
    const { rows: idxRows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE indexname = 'idx_jobs_key_live' AND schemaname = $1 LIMIT 1`,
      [schemaName],
    );
    if (idxRows.length === 0) {
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_jobs_key_live
        ON ${jobsTable} (key) WHERE is_live;
      `);
    }

    // v0.22.0: origin_id, parent_id on per-app jobs table (workflow lineage).
    // ADD COLUMN IF NOT EXISTS on a nullable column with no DEFAULT is catalog-only
    // in PG11+ — no table rewrite, brief AccessExclusiveLock only.
    await client.query(`
      ALTER TABLE ${jobsTable} ADD COLUMN IF NOT EXISTS origin_id TEXT;
    `);
    await client.query(`
      ALTER TABLE ${jobsTable} ADD COLUMN IF NOT EXISTS parent_id TEXT;
    `);
    // Partial indexes — only cover the opt-in rows so build is near-instant.
    const { rows: originIdxRows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE indexname = $1 AND schemaname = $2 LIMIT 1`,
      [`idx_${schemaName}_jobs_origin`, schemaName],
    );
    if (originIdxRows.length === 0) {
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${schemaName}_jobs_origin
          ON ${jobsTable}(origin_id) WHERE origin_id IS NOT NULL;
      `);
    }
    const { rows: parentIdxRows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE indexname = $1 AND schemaname = $2 LIMIT 1`,
      [`idx_${schemaName}_jobs_parent`, schemaName],
    );
    if (parentIdxRows.length === 0) {
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_${schemaName}_jobs_parent
          ON ${jobsTable}(parent_id) WHERE parent_id IS NOT NULL;
      `);
    }

    // ─── v0.22.0+: public.hmsh_escalations global table migrations ───────────
    // Use the fixed advisory XACT lock so concurrent deployments of DIFFERENT
    // appIds (each holding their own per-appId session lock) don't race on the
    // shared public table DDL. Transaction-level lock auto-releases on commit/rollback.
    await client.query('BEGIN');
    await client.query(
      'SELECT pg_advisory_xact_lock($1)',
      [0x484D5348], // 'HMSH' — same constant used in createTables()
    );

    // v0.22.0: create hmsh_escalations if missing (upgrade from pre-0.22.x).
    // Column list matches createTables() exactly; task_id is NOT here because
    // the ADD COLUMN below is the canonical way to add it to both fresh and
    // existing tables in a single idempotent statement.
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.hmsh_escalations (
        id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        namespace        TEXT NOT NULL,
        app_id           TEXT NOT NULL,
        signal_key       TEXT,
        topic            TEXT,
        workflow_id      TEXT,
        task_queue       TEXT,
        workflow_type    TEXT,
        type             TEXT,
        subtype          TEXT,
        entity           TEXT,
        description      TEXT,
        role             TEXT,
        status           TEXT NOT NULL DEFAULT 'pending',
        priority         INT  NOT NULL DEFAULT 5,
        assigned_to      TEXT,
        assigned_until   TIMESTAMPTZ,
        claimed_at       TIMESTAMPTZ,
        claim_expires_at TIMESTAMPTZ,
        resolved_at      TIMESTAMPTZ,
        escalation_payload JSONB,
        resolver_payload   JSONB,
        envelope           JSONB,
        metadata           JSONB,
        origin_id          TEXT,
        parent_id          TEXT,
        initiated_by       TEXT,
        created_by         TEXT,
        milestones         JSONB NOT NULL DEFAULT '[]',
        trace_id           TEXT,
        span_id            TEXT,
        expires_at         TIMESTAMPTZ,
        created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    // v0.22.3: task_id column — catalog-only ADD COLUMN (nullable TEXT, no DEFAULT,
    // no table rewrite). IF NOT EXISTS makes this idempotent across all upgrade paths.
    await client.query(`
      ALTER TABLE public.hmsh_escalations ADD COLUMN IF NOT EXISTS task_id TEXT;
    `);

    // Ensure signal_key unique index exists.
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_hmsh_esc_signal_key
        ON public.hmsh_escalations(namespace, app_id, signal_key)
        WHERE signal_key IS NOT NULL;
    `);

    // v0.22.0: idx_hmsh_esc_available — predicate must be status='pending'.
    // Fresh 0.22.0 installs may have this with the correct predicate already;
    // ensure it exists.
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_available
        ON public.hmsh_escalations(namespace, app_id, role, priority ASC, created_at ASC)
        WHERE status = 'pending';
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_available_expiry
        ON public.hmsh_escalations(namespace, app_id, role, assigned_until, created_at DESC);
    `);

    // v0.22.2: idx_hmsh_esc_assigned predicate changed from status='claimed' to
    // status='pending' to match the implicit claim model. If the old predicate
    // is present, drop and recreate so claimEscalation / list queries use it.
    const { rows: assignedDefRows } = await client.query(`
      SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_hmsh_esc_assigned' LIMIT 1
    `);
    if (
      assignedDefRows.length > 0 &&
      assignedDefRows[0].indexdef.includes("status = 'claimed'")
    ) {
      await client.query(`DROP INDEX IF EXISTS idx_hmsh_esc_assigned;`);
    }
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_assigned
        ON public.hmsh_escalations(assigned_to, assigned_until, created_at DESC)
        WHERE status = 'pending' AND assigned_to IS NOT NULL;
    `);

    // v0.22.2: claim_expiry sweeper index is obsolete (releaseExpiredEscalations
    // is a no-op in the implicit-claim model — availability is query-time computed).
    await client.query(`DROP INDEX IF EXISTS idx_hmsh_esc_claim_expiry;`);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_entity
        ON public.hmsh_escalations(namespace, app_id, entity, created_at DESC)
        WHERE entity IS NOT NULL;
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_workflow
        ON public.hmsh_escalations(workflow_id)
        WHERE workflow_id IS NOT NULL;
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_origin
        ON public.hmsh_escalations(origin_id)
        WHERE origin_id IS NOT NULL;
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_metadata
        ON public.hmsh_escalations USING GIN(metadata jsonb_path_ops);
    `);
    // v0.22.3: task_id partial index.
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_hmsh_esc_task
        ON public.hmsh_escalations(task_id)
        WHERE task_id IS NOT NULL;
    `);

    await client.query('COMMIT');
  },

  async createTables(
    client: PostgresClientType | PostgresPoolClientType,
    appName: string,
  ): Promise<void> {
    try {
      await client.query('BEGIN');
      const schemaName = context.storeClient.safeName(appName);
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName};`);
      const tableDefinitions = this.getTableDefinitions(appName);

      for (const tableDef of tableDefinitions) {
        const fullTableName = `${tableDef.schema}.${tableDef.name}`;

        switch (tableDef.type) {
          case 'relational_app':
            // Public tables are shared across all appIds. Use a fixed
            // advisory lock to prevent concurrent CREATE TABLE races
            // from different appId deployments (each has its own
            // per-appId lock, but those don't overlap).
            await client.query(
              'SELECT pg_advisory_xact_lock($1)',
              [0x484D5348], // 'HMSH' — fixed lock for public tables
            );
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                app_id TEXT PRIMARY KEY,
                version TEXT NOT NULL DEFAULT '1',
                active BOOLEAN DEFAULT TRUE,
                settings JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
              );
            `);
            await client.query(`
              CREATE TABLE IF NOT EXISTS public.hmsh_application_versions (
                app_id TEXT NOT NULL REFERENCES public.hmsh_applications(app_id) ON DELETE CASCADE,
                version TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'deployed',
                deployed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (app_id, version)
              );
            `);
            await client.query(`
              CREATE TABLE IF NOT EXISTS public.hmsh_escalations (
                id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                namespace        TEXT NOT NULL,
                app_id           TEXT NOT NULL,
                signal_key       TEXT,
                topic            TEXT,
                workflow_id      TEXT,
                task_queue       TEXT,
                workflow_type    TEXT,
                type             TEXT,
                subtype          TEXT,
                entity           TEXT,
                description      TEXT,
                role             TEXT,
                status           TEXT NOT NULL DEFAULT 'pending',
                priority         INT  NOT NULL DEFAULT 5,
                assigned_to      TEXT,
                assigned_until   TIMESTAMPTZ,
                claimed_at       TIMESTAMPTZ,
                claim_expires_at TIMESTAMPTZ,
                resolved_at      TIMESTAMPTZ,
                escalation_payload JSONB,
                resolver_payload   JSONB,
                envelope           JSONB,
                metadata           JSONB,
                origin_id          TEXT,
                parent_id          TEXT,
                initiated_by       TEXT,
                created_by         TEXT,
                milestones         JSONB NOT NULL DEFAULT '[]',
                trace_id           TEXT,
                span_id            TEXT,
                expires_at         TIMESTAMPTZ,
                created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
              );
            `);
            await client.query(`
              CREATE UNIQUE INDEX IF NOT EXISTS idx_hmsh_esc_signal_key
                ON public.hmsh_escalations(namespace, app_id, signal_key)
                WHERE signal_key IS NOT NULL;
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_available
                ON public.hmsh_escalations(namespace, app_id, role, priority ASC, created_at ASC)
                WHERE status = 'pending';
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_available_expiry
                ON public.hmsh_escalations(namespace, app_id, role, assigned_until, created_at DESC);
            `);
            // idx_hmsh_esc_assigned: implicit-claim model uses status='pending', not 'claimed'
            await client.query(`DROP INDEX IF EXISTS idx_hmsh_esc_assigned;`);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_assigned
                ON public.hmsh_escalations(assigned_to, assigned_until, created_at DESC)
                WHERE status = 'pending' AND assigned_to IS NOT NULL;
            `);
            // idx_hmsh_esc_claim_expiry: no longer used (releaseExpiredEscalations is a no-op)
            await client.query(`DROP INDEX IF EXISTS idx_hmsh_esc_claim_expiry;`);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_entity
                ON public.hmsh_escalations(namespace, app_id, entity, created_at DESC)
                WHERE entity IS NOT NULL;
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_workflow
                ON public.hmsh_escalations(workflow_id)
                WHERE workflow_id IS NOT NULL;
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_origin
                ON public.hmsh_escalations(origin_id)
                WHERE origin_id IS NOT NULL;
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_metadata
                ON public.hmsh_escalations USING GIN(metadata jsonb_path_ops);
            `);
            // task_id: nullable passthrough column for downstream compat (no FK)
            await client.query(`
              ALTER TABLE public.hmsh_escalations ADD COLUMN IF NOT EXISTS task_id TEXT;
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_hmsh_esc_task
                ON public.hmsh_escalations(task_id)
                WHERE task_id IS NOT NULL;
            `);
            break;

          case 'relational_connection':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                guid TEXT NOT NULL,
                app_id TEXT NOT NULL,
                role TEXT NOT NULL,
                version TEXT NOT NULL,
                connected_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (guid, app_id)
              );
            `);
            break;

          case 'string':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT PRIMARY KEY,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE
              );
            `);
            if (tableDef.name === 'signal_registry') {
              await client.query(`
                CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expiry
                  ON ${fullTableName} (expiry)
                  WHERE expiry IS NOT NULL;
              `);
            }
            break;

          case 'hash':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                field TEXT NOT NULL,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, field)
              );
            `);
            break;

          case 'jobhash':
            // Create the enum type in the schema
            await client.query(`
              DO $$
              BEGIN
                IF NOT EXISTS (
                  SELECT 1 FROM pg_type t
                  JOIN pg_namespace n ON n.oid = t.typnamespace
                  WHERE t.typname = 'type_enum' AND n.nspname = '${schemaName}'
                ) THEN
                  CREATE TYPE ${schemaName}.type_enum AS ENUM ('jmark', 'hmark', 'status', 'jdata', 'adata', 'udata', 'other');
                END IF;
              END$$;
            `);

            // Create the main jobs table with partitioning on id
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                id UUID DEFAULT gen_random_uuid(),
                key TEXT NOT NULL,
                entity TEXT,
                origin_id TEXT,
                parent_id TEXT,
                status INTEGER NOT NULL,
                context JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expired_at TIMESTAMP WITH TIME ZONE,
                pruned_at TIMESTAMP WITH TIME ZONE,
                is_live BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (id)
              ) PARTITION BY HASH (id);
            `);

            // Create GIN index for full JSONB search
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_context_gin 
              ON ${fullTableName} USING GIN (context);
            `);

            // Create partitions with fillfactor 70: reserves 30% page space
            // for HOT updates (status update cycle in setStatusAndCollateGuid)
            await client.query(`
              DO $$
              BEGIN
                FOR i IN 0..7 LOOP
                  EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS ${fullTableName}_part_%s PARTITION OF ${fullTableName}
                    FOR VALUES WITH (modulus 8, remainder %s)
                    WITH (fillfactor = 70)',
                    i, i
                  );
                END LOOP;
              END$$;
            `);

            // Create optimized indexes
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expired_at
              ON ${fullTableName} (key, expired_at) INCLUDE (is_live);
            `);

            // Dedicated partial index for the hot-path (WHERE key = $1 AND is_live).
            // Covers the uniqueness trigger, hget, hgetall, hincrbyfloat, and
            // the upsert — all of which use AND is_live directly.
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_live
              ON ${fullTableName} (key)
              WHERE is_live;
            `);

            // status in INCLUDE (not key) so semaphore increments don't
            // force index key updates — allows HOT on the hottest write path.
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_status
              ON ${fullTableName} (entity) INCLUDE (status);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_expired_at
              ON ${fullTableName} (expired_at);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_pruned_at
              ON ${fullTableName} (pruned_at) WHERE pruned_at IS NULL;
            `);

            // Index for paginated entity listing with sort (dashboard entity queries)
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_entity_created
              ON ${fullTableName} (entity, created_at DESC)
              WHERE entity IS NOT NULL;
            `);

            // Create function to update is_live flag in the schema
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_is_live()
              RETURNS TRIGGER AS $$
              BEGIN
                NEW.is_live := NEW.expired_at IS NULL OR NEW.expired_at > NOW();
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for is_live updates
            await client.query(`
              CREATE TRIGGER trg_update_is_live
              BEFORE INSERT OR UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE PROCEDURE ${schemaName}.update_is_live();
            `);

            // Create function to enforce uniqueness of live jobs
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.enforce_live_job_uniqueness()
              RETURNS TRIGGER AS $$
              BEGIN
                IF (NEW.expired_at IS NULL OR NEW.expired_at > NOW()) THEN
                  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.key, 0));
                  IF EXISTS (
                    SELECT 1 FROM ${fullTableName}
                    WHERE key = NEW.key
                    AND is_live
                    AND id <> NEW.id
                  ) THEN
                    RAISE EXCEPTION 'A live job with key % already exists.', NEW.key;
                  END IF;
                END IF;
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for uniqueness enforcement
            await client.query(`
              CREATE TRIGGER trg_enforce_live_job_uniqueness
              BEFORE INSERT OR UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE PROCEDURE ${schemaName}.enforce_live_job_uniqueness();
            `);

            // Create function to update updated_at on status changes
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_jobs_updated_at()
              RETURNS TRIGGER AS $$
              BEGIN
                IF NEW.status <> OLD.status THEN
                  NEW.updated_at = NOW();
                END IF;
                RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for updated_at on job status changes
            await client.query(`
              DROP TRIGGER IF EXISTS trg_update_jobs_updated_at ON ${fullTableName};
              CREATE TRIGGER trg_update_jobs_updated_at
              BEFORE UPDATE ON ${fullTableName}
              FOR EACH ROW EXECUTE FUNCTION ${schemaName}.update_jobs_updated_at();
            `);

            // Create the attributes table with partitioning
            const attributesTableName = `${fullTableName}_attributes`;
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${attributesTableName} (
                job_id UUID NOT NULL,
                symbol TEXT NOT NULL,
                dimension TEXT NOT NULL DEFAULT '',
                value TEXT,
                type ${schemaName}.type_enum NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (job_id, symbol, dimension),
                FOREIGN KEY (job_id) REFERENCES ${fullTableName} (id) ON DELETE CASCADE
              ) PARTITION BY HASH (job_id);
            `);

            // Create trigger function for updating updated_at only for mutable types
            await client.query(`
              CREATE OR REPLACE FUNCTION ${schemaName}.update_attributes_updated_at()
              RETURNS TRIGGER AS $$
              BEGIN
                  IF NEW.type IN ('udata', 'jdata', 'hmark', 'jmark') AND 
                     (OLD.value IS NULL OR NEW.value <> OLD.value) THEN
                      NEW.updated_at = NOW();
                  END IF;
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
            `);

            // Create trigger for updated_at updates
            await client.query(`
              DROP TRIGGER IF EXISTS trg_update_attributes_updated_at ON ${attributesTableName};
              CREATE TRIGGER trg_update_attributes_updated_at
                  BEFORE UPDATE ON ${attributesTableName}
                  FOR EACH ROW
                  EXECUTE FUNCTION ${schemaName}.update_attributes_updated_at();
            `);

            // Create partitions with fillfactor 70: reserves 30% page space
            // for HOT updates (frequent upserts in hincrbyfloat/collateLeg2Entry)
            await client.query(`
              DO $$
              BEGIN
                FOR i IN 0..7 LOOP
                  EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS ${attributesTableName}_part_%s PARTITION OF ${attributesTableName}
                    FOR VALUES WITH (modulus 8, remainder %s)
                    WITH (fillfactor = 70)',
                    i, i
                  );
                END LOOP;
              END$$;
            `);

            // Create indexes for attributes table
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_type_symbol
              ON ${attributesTableName} (type, symbol);
            `);

            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_attributes_symbol
              ON ${attributesTableName} (symbol);
            `);
            break;

          case 'list':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                index BIGINT NOT NULL,
                value TEXT,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, index)
              );
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_expiry 
              ON ${fullTableName} (key, expiry);
            `);
            break;

          case 'sorted_set':
            await client.query(`
              CREATE TABLE IF NOT EXISTS ${fullTableName} (
                key TEXT NOT NULL,
                member TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                expiry TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (key, member)
              );
            `);
            await client.query(`
              CREATE INDEX IF NOT EXISTS idx_${tableDef.name}_key_score_member
              ON ${fullTableName} (key, score, member);
            `);
            break;

          default:
            context.logger.warn(`Unknown table type for ${tableDef.name}`);
            break;
        }
      }

      // Commit transaction
      await client.query('COMMIT');
    } catch (error) {
      context.logger.error('postgres-create-tables-error', { error });
      await client.query('ROLLBACK');
      throw error;
    }
  },

  getTableNames(appName: string): string[] {
    const tableNames = [];

    // Public relational tables
    tableNames.push(
      'public.hmsh_applications',
      'public.hmsh_application_versions',
      'public.hmsh_connections',
    );

    // Other tables with appName
    const tablesWithAppName = [
      'throttles',
      'roles',
      'task_priorities',
      'task_lists',
      'events',
      'jobs',
      'stats_counted',
      'stats_indexed',
      'stats_ordered',
      'versions',
      'signal_patterns',
      'signal_registry',
      'symbols',
    ];

    tablesWithAppName.forEach((table) => {
      tableNames.push(`${context.storeClient.safeName(appName)}.${table}`);
    });

    return tableNames;
  },

  getTableDefinitions(
    appName: string,
  ): Array<{ schema: string; name: string; type: string }> {
    const schemaName = context.storeClient.safeName(appName);

    const tableDefinitions = [
      {
        schema: 'public',
        name: 'hmsh_applications',
        type: 'relational_app',
      },
      {
        schema: 'public',
        name: 'hmsh_connections',
        type: 'relational_connection',
      },
      {
        schema: schemaName,
        name: 'throttles',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'roles',
        type: 'string',
      },
      {
        schema: schemaName,
        name: 'task_priorities',
        type: 'sorted_set',
      },
      {
        schema: schemaName,
        name: 'task_lists',
        type: 'list',
      },
      {
        schema: schemaName,
        name: 'events',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'jobs',
        type: 'jobhash', // Adds partitioning, indexes, and enum type
      },
      {
        schema: schemaName,
        name: 'stats_counted',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'stats_ordered',
        type: 'sorted_set',
      },
      {
        schema: schemaName,
        name: 'stats_indexed',
        type: 'list',
      },
      {
        schema: schemaName,
        name: 'versions',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'signal_patterns',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'symbols',
        type: 'hash',
      },
      {
        schema: schemaName,
        name: 'signal_registry',
        type: 'string',
      },
    ];

    return tableDefinitions;
  },
});
