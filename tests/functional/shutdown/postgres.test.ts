/**
 * HotMesh.stop() stops every stream poller in the process, including the
 * ones on engines that never became the app's consuming router (a second
 * engine for the same app, a client's engine). Before the disconnect no
 * scout loop is left polling a closed client.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { HMNS } from '../../../modules/key';
import { guid } from '../../../modules/utils';
import { HotMesh, HotMeshConfig } from '../../../index';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { StreamService } from '../../../services/stream';
import { ProviderNativeClient } from '../../../types/provider';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Shutdown | Postgres', () => {
  let postgresClient: ProviderNativeClient;

  const config = (): HotMeshConfig => ({
    appId: 'shutdown',
    namespace: HMNS,
    engine: { connection: { class: Postgres, options: postgres_options } },
  });

  const scoutOf = (hotMesh: HotMesh) => (hotMesh.engine as any).stream.scoutManager;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  }, 30_000);

  it('stops the scout of every engine for the app, consuming or not', async () => {
    const first = await HotMesh.init(config());
    const second = await HotMesh.init(config());
    expect(scoutOf(first)).toBeTruthy();
    expect(scoutOf(second)).toBeTruthy();
    expect(scoutOf(second).isStopped()).toBe(false);

    await HotMesh.stop();

    expect(scoutOf(first).isStopped()).toBe(true);
    expect(scoutOf(second).isStopped()).toBe(true);
    expect((StreamService as any).running.size).toBe(0);
  }, 30_000);
});
