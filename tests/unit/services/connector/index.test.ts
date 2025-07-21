import { Client as Postgres } from 'pg';

import config from '../../../$setup/config';
import { ConnectorService } from '../../../../services/connector/factory';
import { PostgresConnection } from '../../../../services/connector/providers/postgres';
import { HotMeshEngine, HotMeshWorker } from '../../../../types/hotmesh';
import { PostgresClientOptions } from '../../../../types/postgres';

describe('ConnectorService Functional Test', () => {
  let target: HotMeshEngine;
  const postgresOptions: PostgresClientOptions = {
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    user: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    database: config.POSTGRES_DB,
  };
  const PostgresClass = Postgres;

  beforeEach(() => {
    target = {} as HotMeshEngine;
  });

  it('should initialize clients if not already present', async () => {
    const target: HotMeshEngine | HotMeshWorker = {
      connection: {
        class: PostgresClass,
        options: postgresOptions,
      },
      store: undefined,
      stream: undefined,
      sub: undefined,
    };
    await ConnectorService.initClients(target);

    // Verify that the target object has store, stream, and sub properties
    expect(target.store).toBeDefined();
    expect(target.stream).toBeDefined();
    expect(target.sub).toBeDefined();

    // Verify they can actually interact with the backend
    await target?.store?.set('testKeyStore', 'testValue');
    const valueStore = await target?.store?.get('testKeyStore');
    expect(valueStore).toBe('testValue');
  });

  // Disconnect from Postgres after all tests
  afterAll(async () => {
    await PostgresConnection.disconnectAll();
  });
});
