import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';
import { scryptSync, createCipheriv, createDecipheriv, randomBytes } from 'crypto';

import config from '../../$setup/config';
import { guid, sleepFor } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { HotMesh } from '../../../services/hotmesh';
import type { PayloadCodec } from '../../../services/hotmesh';
import { SerializerService } from '../../../services/serializer';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables } from '../../$setup/postgres';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

/** AES-256-GCM encryption codec */
function createEncryptionCodec(passphrase: string): PayloadCodec {
  const key = scryptSync(passphrase, 'hotmesh-codec-salt', 32);
  return {
    encode(json: string): string {
      const iv = randomBytes(12);
      const cipher = createCipheriv('aes-256-gcm', key, iv);
      const enc = Buffer.concat([
        cipher.update(json, 'utf8'),
        cipher.final(),
      ]);
      const tag = cipher.getAuthTag();
      return Buffer.concat([iv, tag, enc]).toString('base64');
    },
    decode(encoded: string): string {
      const buf = Buffer.from(encoded, 'base64');
      const iv = buf.subarray(0, 12);
      const tag = buf.subarray(12, 28);
      const data = buf.subarray(28);
      const decipher = createDecipheriv('aes-256-gcm', key, iv);
      decipher.setAuthTag(tag);
      return Buffer.concat([
        decipher.update(data),
        decipher.final(),
      ]).toString('utf8');
    },
  };
}

describe('DURABLE | codec | Encrypted Workflow Data | Postgres', () => {
  let postgresClient: ProviderNativeClient;
  const namespace = 'durable';

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
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  }, 30_000);

  afterAll(async () => {
    SerializerService.registerCodec(null);
    await sleepFor(1500);
    await Durable.shutdown();
    await postgresClient.end();
  }, 10_000);

  describe('Durable.registerCodec()', () => {
    it('should delegate to the same function as HotMesh.registerCodec', () => {
      expect(Durable.registerCodec).toBe(HotMesh.registerCodec);
    });

    it('should register the codec globally', () => {
      const codec = createEncryptionCodec('test-passphrase');
      Durable.registerCodec(codec);
      expect(SerializerService.getCodec()).toBe(codec);
    });
  });

  describe('encrypted workflow execution', () => {
    const workflowId = 'codec-wf-' + guid();

    it('should connect with codec enabled', async () => {
      // Register the encryption codec before connecting
      Durable.registerCodec(createEncryptionCodec('durable-test-secret'));

      const conn = await Connection.connect(connection);
      expect(conn).toBeDefined();
    });

    it('should create a worker', async () => {
      const worker = await Worker.create({
        connection,
        taskQueue: 'codec-durable-test',
        workflow: workflows.codecWorkflow,
      });
      await worker.run();
    }, 15_000);

    it('should execute a workflow and return correct results', async () => {
      const client = new Client({ connection });
      const handle = await client.workflow.start({
        args: ['sensitive-data', 21],
        taskQueue: 'codec-durable-test',
        workflowName: 'codecWorkflow',
        workflowId,
        expire: 120,
        signalIn: false,
      });

      const result = await handle.result() as Record<string, any>;
      expect(result.processed).toBe(true);
      expect(result.name).toBe('sensitive-data');
      expect(result.doubled).toBe(42);
      expect(result.nested).toEqual({
        deep: { secret: 'encoded-at-rest' },
      });
    }, 15_000);

    it('should store encrypted values in the database', async () => {
      // Query jobs_attributes for /b encoded values
      const result = await postgresClient.query(
        `SELECT value FROM ${namespace}.jobs_attributes
         WHERE type = 'adata' AND value LIKE '/b%'
         LIMIT 10`,
      );

      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        expect(row.value).toMatch(/^\/b/);
        // Plaintext should NOT appear in the stored value
        expect(row.value).not.toContain('sensitive-data');
        expect(row.value).not.toContain('encoded-at-rest');
      }
    });

    it('should NOT contain plaintext in any /b values across the job', async () => {
      // Broader check: no /b value should contain our sensitive strings
      const result = await postgresClient.query(
        `SELECT value FROM ${namespace}.jobs_attributes
         WHERE value LIKE '/b%'`,
      );

      for (const row of result.rows) {
        expect(row.value).not.toContain('sensitive-data');
        expect(row.value).not.toContain('encoded-at-rest');
      }
    });

    it('should still store scalar values as plaintext (no codec on scalars)', async () => {
      // Scalars (/t, /f, /d, /n, plain strings) should NOT be encoded
      const result = await postgresClient.query(
        `SELECT value FROM ${namespace}.jobs_attributes
         WHERE value LIKE '/d%' OR value LIKE '/t' OR value LIKE '/f'
         LIMIT 5`,
      );
      // There should be some scalar values (numbers, booleans)
      expect(result.rows.length).toBeGreaterThan(0);
    });
  });
});
