import { describe, it, expect, afterAll, vi } from 'vitest';

//read at module load by modules/enums; set before any HotMesh import
vi.hoisted(() => {
  process.env.HMSH_PG_RESILIENT = 'false';
});

import { Client } from 'pg';

import { postgres_options } from '../../$setup/postgres';
import { guid } from '../../../modules/utils';
import { HMSH_PG_RESILIENT } from '../../../modules/enums';
import { PostgresConnection } from '../../../services/connector/providers/postgres';

describe('FUNCTIONAL | HMSH_PG_RESILIENT=false', () => {
  afterAll(async () => {
    await PostgresConnection.disconnectAll();
    delete process.env.HMSH_PG_RESILIENT;
  });

  it('restores the native pg.Client exactly as it was created before', async () => {
    expect(HMSH_PG_RESILIENT).toBe(false);
    const client: any = (
      await PostgresConnection.connect(guid(), Client as any, { ...postgres_options } as any)
    ).getClient();
    expect(Object.getPrototypeOf(client)).toBe(Client.prototype);
    expect(client.connectionGeneration).toBeUndefined();
    //no defaults are injected on the native path
    const { rows } = await client.query(
      `SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()`,
    );
    expect(rows[0].application_name).toBe('');
  });
});
