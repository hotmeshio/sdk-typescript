import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh } from '../../../services/hotmesh';
import { HotMeshConfig } from '../../../types/hotmesh';
import type { WorkerCredential, WorkerCredentialInfo } from '../../../services/hotmesh';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Worker Credentials', () => {
  const appId = 'cred-test';
  // The Postgres schema is derived from appId (hyphens → underscores)
  const namespace = 'cred_test';
  let postgresClient: ProviderNativeClient;
  let hotMesh: HotMesh;
  let credential: WorkerCredential;

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

    // Initialize HotMesh with an engine to create the schema and tables
    // (worker_credentials table is created during schema init)
    const config: HotMeshConfig = {
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
          topic: 'cred.test.topic',
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
    hotMesh = await HotMesh.init(config);
  }, 30_000);

  afterAll(async () => {
    // Clean up: drop the test role if it exists
    if (credential?.roleName) {
      try {
        await postgresClient.query(
          `DROP ROLE IF EXISTS ${credential.roleName};`,
        );
      } catch {
        // Ignore — role may not exist
      }
    }
    hotMesh.stop();
    await HotMesh.stop();
    await postgresClient.end();
  });

  describe('HotMesh.provisionWorkerRole()', () => {
    it('should provision a scoped Postgres role', async () => {
      credential = await HotMesh.provisionWorkerRole({
        connection,
        namespace,
        streamNames: ['cred.test.topic'],
      });

      expect(credential).toBeDefined();
      expect(credential.roleName).toMatch(/^hmsh_wrk_/);
      expect(credential.password).toBeDefined();
      expect(credential.password.length).toBeGreaterThan(0);
    });

    it('should create a role that can login to Postgres', async () => {
      // Verify the role exists and can login
      const result = await postgresClient.query(
        `SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].rolcanlogin).toBe(true);
    });

    it('should record the credential in the worker_credentials table', async () => {
      const result = await postgresClient.query(
        `SELECT role_name, stream_names, revoked_at
         FROM ${namespace}.worker_credentials
         WHERE role_name = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].role_name).toBe(credential.roleName);
      expect(result.rows[0].stream_names).toContain('cred.test.topic');
      expect(result.rows[0].revoked_at).toBeNull();
    });

    it('should be idempotent — re-provisioning updates password', async () => {
      const updated = await HotMesh.provisionWorkerRole({
        connection,
        namespace,
        streamNames: ['cred.test.topic'],
        password: 'new-test-password',
      });

      expect(updated.roleName).toBe(credential.roleName);
      expect(updated.password).toBe('new-test-password');

      // Role should still exist and be able to login
      const result = await postgresClient.query(
        `SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`,
        [credential.roleName],
      );
      expect(result.rows[0].rolcanlogin).toBe(true);

      // Restore the original credential reference
      credential = updated;
    });
  });

  describe('HotMesh.listWorkerRoles()', () => {
    it('should list provisioned worker roles', async () => {
      const roles: WorkerCredentialInfo[] = await HotMesh.listWorkerRoles({
        connection,
        namespace,
      });

      expect(roles.length).toBeGreaterThanOrEqual(1);
      const found = roles.find((r) => r.roleName === credential.roleName);
      expect(found).toBeDefined();
      expect(found.streamNames).toContain('cred.test.topic');
      expect(found.revokedAt).toBeNull();
      expect(found.createdAt).toBeInstanceOf(Date);
      expect(found.lastRotatedAt).toBeInstanceOf(Date);
    });
  });

  describe('HotMesh.rotateWorkerPassword()', () => {
    it('should rotate the password for a worker role', async () => {
      const result = await HotMesh.rotateWorkerPassword({
        connection,
        namespace,
        roleName: credential.roleName,
      });

      expect(result).toBeDefined();
      expect(result.password).toBeDefined();
      expect(result.password.length).toBeGreaterThan(0);
      // Password should differ from the previous one
      expect(result.password).not.toBe(credential.password);
    });

    it('should accept an explicit new password', async () => {
      const result = await HotMesh.rotateWorkerPassword({
        connection,
        namespace,
        roleName: credential.roleName,
        newPassword: 'explicit-rotated-pw',
      });

      expect(result.password).toBe('explicit-rotated-pw');
    });

    it('should update last_rotated_at in worker_credentials', async () => {
      const result = await postgresClient.query(
        `SELECT last_rotated_at FROM ${namespace}.worker_credentials
         WHERE role_name = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].last_rotated_at).toBeInstanceOf(Date);
    });
  });

  describe('HotMesh.revokeWorkerRole()', () => {
    it('should revoke a worker role (disable login)', async () => {
      await HotMesh.revokeWorkerRole({
        connection,
        namespace,
        roleName: credential.roleName,
      });

      // Role should still exist but cannot login
      const result = await postgresClient.query(
        `SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].rolcanlogin).toBe(false);
    });

    it('should mark revoked_at in worker_credentials', async () => {
      const result = await postgresClient.query(
        `SELECT revoked_at FROM ${namespace}.worker_credentials
         WHERE role_name = $1`,
        [credential.roleName],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].revoked_at).toBeInstanceOf(Date);
    });

    it('should show revoked status in listWorkerRoles', async () => {
      const roles = await HotMesh.listWorkerRoles({
        connection,
        namespace,
      });
      const found = roles.find((r) => r.roleName === credential.roleName);
      expect(found).toBeDefined();
      expect(found.revokedAt).toBeInstanceOf(Date);
    });
  });

  describe('provisionWorkerRole() with multiple streams', () => {
    let multiCredential: WorkerCredential;

    afterAll(async () => {
      if (multiCredential?.roleName) {
        try {
          await postgresClient.query(
            `DROP ROLE IF EXISTS ${multiCredential.roleName};`,
          );
        } catch {
          // Ignore
        }
      }
    });

    it('should provision a role scoped to multiple stream names', async () => {
      multiCredential = await HotMesh.provisionWorkerRole({
        connection,
        namespace,
        streamNames: ['stream.alpha', 'stream.beta'],
      });

      expect(multiCredential.roleName).toMatch(/^hmsh_wrk_/);
      expect(multiCredential.roleName).toContain('stream_alpha__stream_beta');

      // Verify allowed_streams config
      const result = await postgresClient.query(
        `SELECT rolname FROM pg_roles WHERE rolname = $1`,
        [multiCredential.roleName],
      );
      expect(result.rows.length).toBe(1);
    });

    it('should record multiple stream names in worker_credentials', async () => {
      const result = await postgresClient.query(
        `SELECT stream_names FROM ${namespace}.worker_credentials
         WHERE role_name = $1`,
        [multiCredential.roleName],
      );
      expect(result.rows.length).toBe(1);
      const streams = result.rows[0].stream_names;
      expect(streams).toContain('stream.alpha');
      expect(streams).toContain('stream.beta');
    });
  });
});
