import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import config from '../../$setup/config';
import { guid } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { HotMesh, HotMeshConfig } from '../../../services/hotmesh';
import type { WorkerCredential, WorkerCredentialInfo } from '../../../services/hotmesh';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables } from '../../$setup/postgres';

describe('DURABLE | credentials | Worker Credential Lifecycle | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  let hotMesh: HotMesh;
  let credential: WorkerCredential;

  // Use 'durable' as appId so the Postgres schema matches the standard
  // Durable namespace used by the credential functions.
  const appId = 'durable';
  const namespace = appId;

  const postgres_options = {
    user: config.POSTGRES_USER,
    host: config.POSTGRES_HOST,
    database: config.POSTGRES_DB,
    password: config.POSTGRES_PASSWORD,
    port: config.POSTGRES_PORT,
  };

  const connection = {
    class: Postgres,
    options: postgres_options,
  };

  beforeAll(async () => {
    // Initialize Postgres and drop tables from prior tests
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);

    // Initialize HotMesh with an engine to create the schema + tables
    // (this includes the worker_credentials table)
    const hmConfig: HotMeshConfig = {
      appId,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        connection: {
          store: { class: Postgres, options: postgres_options },
          stream: { class: Postgres, options: postgres_options },
          sub: { class: Postgres, options: postgres_options },
        },
      },
      workers: [
        {
          topic: 'durable.credential.test',
          connection: {
            store: { class: Postgres, options: postgres_options },
            stream: { class: Postgres, options: postgres_options },
            sub: { class: Postgres, options: postgres_options },
          },
          callback: async (streamData: StreamData) => ({
            status: StreamStatus.SUCCESS,
            metadata: { ...streamData.metadata },
            data: { done: true },
          }),
        },
      ],
    };
    hotMesh = await HotMesh.init(hmConfig);
  }, 30_000);

  afterAll(async () => {
    // Clean up test role
    if (credential?.roleName) {
      try {
        await postgresClient.query(
          `DROP ROLE IF EXISTS ${credential.roleName};`,
        );
      } catch {
        // Ignore
      }
    }
    hotMesh.stop();
    await HotMesh.stop();
    await postgresClient.end();
  });

  describe('re-export chain', () => {
    it('Durable credential methods should reference the same functions as HotMesh', () => {
      expect(Durable.provisionWorkerRole).toBe(HotMesh.provisionWorkerRole);
      expect(Durable.rotateWorkerPassword).toBe(HotMesh.rotateWorkerPassword);
      expect(Durable.revokeWorkerRole).toBe(HotMesh.revokeWorkerRole);
      expect(Durable.listWorkerRoles).toBe(HotMesh.listWorkerRoles);
    });
  });

  describe('Durable.provisionWorkerRole()', () => {
    it('should provision a scoped Postgres role via Durable re-export', async () => {
      credential = await Durable.provisionWorkerRole({
        connection,
        namespace,
        streamNames: ['durable.credential.test'],
      });

      expect(credential).toBeDefined();
      expect(credential.roleName).toMatch(/^hmsh_wrk_/);
      expect(credential.password).toBeDefined();
      expect(credential.password.length).toBeGreaterThan(0);
    });

    it('should create a role that can login', async () => {
      const result = await postgresClient.query(
        `SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].rolcanlogin).toBe(true);
    });
  });

  describe('Durable.listWorkerRoles()', () => {
    it('should list provisioned worker roles', async () => {
      const roles: WorkerCredentialInfo[] = await Durable.listWorkerRoles({
        connection,
        namespace,
      });

      expect(roles.length).toBeGreaterThanOrEqual(1);
      const found = roles.find((r) => r.roleName === credential.roleName);
      expect(found).toBeDefined();
      expect(found.streamNames).toContain('durable.credential.test');
      expect(found.revokedAt).toBeNull();
    });
  });

  describe('Durable.rotateWorkerPassword()', () => {
    it('should rotate the password via Durable', async () => {
      const result = await Durable.rotateWorkerPassword({
        connection,
        namespace,
        roleName: credential.roleName,
      });

      expect(result).toBeDefined();
      expect(result.password).toBeDefined();
      expect(result.password).not.toBe(credential.password);
    });
  });

  describe('Durable.revokeWorkerRole()', () => {
    it('should revoke the worker role via Durable', async () => {
      await Durable.revokeWorkerRole({
        connection,
        namespace,
        roleName: credential.roleName,
      });

      const result = await postgresClient.query(
        `SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].rolcanlogin).toBe(false);
    });

    it('should appear revoked in the list', async () => {
      const roles = await Durable.listWorkerRoles({
        connection,
        namespace,
      });
      const found = roles.find((r) => r.roleName === credential.roleName);
      expect(found).toBeDefined();
      expect(found.revokedAt).toBeInstanceOf(Date);
    });
  });
});
