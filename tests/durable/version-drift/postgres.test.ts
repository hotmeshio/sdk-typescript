/**
 * Proves the durable app schema hot-swaps when an OLDER version is already
 * deployed (the SDK-upgrade path). Regression guard for the bug where an
 * existing, active durable app at version N never picked up the SDK's newer
 * schema at version N+1 — silently disabling schema-encoded features like the
 * condition() escalation hook on every persistent (non-fresh) database.
 *
 * Scenario: deploy + activate the durable app at an older version, then create a
 * Durable.Worker (which runs WorkerService.activateWorkflow). The worker must
 * detect the drift and deploy + activate the current APP_VERSION.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh } from '../../../index';
import { Durable } from '../../../services/durable';
import { guid, sleepFor } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import {
  getWorkflowYAML,
  APP_VERSION,
  APP_ID,
} from '../../../services/durable/schemas/factory';

const { Connection, Worker } = Durable;
const OLD_VERSION = String(Number(APP_VERSION) - 1);

async function noop(): Promise<string> {
  return 'ok';
}

describe('DURABLE | version drift redeploy | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let hotMesh: HotMesh;
  const connection = { class: Postgres, options: postgres_options };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 15_000);

  it('deploys the durable app at the previous (older) version', async () => {
    await Connection.connect(connection);
    hotMesh = await HotMesh.init({
      appId: APP_ID,
      logLevel: HMSH_LOGLEVEL,
      engine: { connection },
    });
    await hotMesh.deploy(getWorkflowYAML(APP_ID, OLD_VERSION));
    await hotMesh.activate(OLD_VERSION);

    const app = await hotMesh.engine.store.getApp(APP_ID);
    expect(app.version).toBe(OLD_VERSION);
    expect(app.active).toBe(true);
  }, 20_000);

  it('hot-swaps to the current APP_VERSION when a worker activates', async () => {
    // Worker.create → WorkerService.activateWorkflow sees deployed < APP_VERSION
    // and redeploys + activates the new schema version.
    const worker = await Worker.create({
      connection,
      taskQueue: 'version-drift-test',
      workflow: noop,
    });
    await worker.run();

    // Read the registry directly — a long-lived HotMesh instance caches the app
    // record, so assert against the authoritative store row.
    const { rows } = await postgresClient.query(
      'SELECT version, active FROM hmsh_applications WHERE app_id = $1',
      [APP_ID],
    );
    expect(rows[0].version).toBe(APP_VERSION);
    expect(Number(rows[0].version)).toBeGreaterThan(Number(OLD_VERSION));
    expect(rows[0].active).toBe(true);

    // Both versions remain deployed — in-flight jobs drain on the old schema.
    const { rows: versions } = await postgresClient.query(
      'SELECT version FROM hmsh_application_versions WHERE app_id = $1 ORDER BY version',
      [APP_ID],
    );
    expect(versions.map((r) => r.version)).toEqual([OLD_VERSION, APP_VERSION]);
  }, 30_000);
});
