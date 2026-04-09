import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { Client as Postgres } from 'pg';
import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
  scryptSync,
} from 'crypto';

import { HotMesh } from '../../../services/hotmesh';
import type { PayloadCodec } from '../../../services/hotmesh';
import { HotMeshConfig } from '../../../types/hotmesh';
import { SerializerService } from '../../../services/serializer';
import { guid } from '../../../modules/utils';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../../types/stream';
import { JobOutput } from '../../../types/job';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';

// ---- Test codecs ----

/** Simple base64 codec — easy to verify in DB */
const base64Codec: PayloadCodec = {
  encode(json: string): string {
    return Buffer.from(json, 'utf8').toString('base64');
  },
  decode(encoded: string): string {
    return Buffer.from(encoded, 'base64').toString('utf8');
  },
};

/** AES-256-GCM encryption codec — production-grade */
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

describe('FUNCTIONAL | PayloadCodec | HotMesh', () => {
  const appId = 'codec-test';
  let postgresClient: ProviderNativeClient;
  let hotMesh: HotMesh;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();
    await dropTables(postgresClient);
  }, 30_000);

  afterAll(async () => {
    SerializerService.registerCodec(null);
    hotMesh?.stop();
    await HotMesh.stop();
    await postgresClient.end();
  });

  afterEach(() => {
    SerializerService.registerCodec(null);
  });

  // ---- Unit-level: SerializerService ----

  describe('SerializerService codec integration', () => {
    it('should store objects as /s when no codec is registered', () => {
      const result = SerializerService.toString({ name: 'test' });
      expect(result).toMatch(/^\/s/);
      expect(result).toBe('/s{"name":"test"}');
    });

    it('should store objects as /b when a codec is registered', () => {
      SerializerService.registerCodec(base64Codec);
      const result = SerializerService.toString({ name: 'test' });
      expect(result).toMatch(/^\/b/);
      expect(result).not.toContain('{"name":"test"}');
      // Verify the encoded payload is base64 of the JSON
      const encoded = result!.slice(2);
      expect(Buffer.from(encoded, 'base64').toString('utf8')).toBe(
        '{"name":"test"}',
      );
    });

    it('should decode /b values back to original objects', () => {
      SerializerService.registerCodec(base64Codec);
      const original = { name: 'test', count: 42, nested: { a: [1, 2] } };
      const stored = SerializerService.toString(original);
      const restored = SerializerService.fromString(stored);
      expect(restored).toEqual(original);
    });

    it('should still decode legacy /s values when codec is registered', () => {
      SerializerService.registerCodec(base64Codec);
      // Simulate reading legacy data stored without a codec
      const legacy = '/s{"name":"legacy"}';
      const restored = SerializerService.fromString(legacy);
      expect(restored).toEqual({ name: 'legacy' });
    });

    it('should preserve Date objects through codec round-trip', () => {
      SerializerService.registerCodec(base64Codec);
      const date = new Date('2026-04-09T12:00:00.000Z');
      const stored = SerializerService.toString(date);
      expect(stored).toMatch(/^\/b/);
      const restored = SerializerService.fromString(stored);
      expect(restored).toBeInstanceOf(Date);
      expect(restored.toISOString()).toBe('2026-04-09T12:00:00.000Z');
    });

    it('should not affect scalar types', () => {
      SerializerService.registerCodec(base64Codec);
      expect(SerializerService.toString('hello')).toBe('hello');
      expect(SerializerService.toString(42)).toBe('/d42');
      expect(SerializerService.toString(true)).toBe('/t');
      expect(SerializerService.toString(false)).toBe('/f');
      expect(SerializerService.toString(null)).toBe('/n');
      expect(SerializerService.toString(undefined)).toBeUndefined();
    });

    it('should work with AES-256-GCM encryption codec', () => {
      const codec = createEncryptionCodec('my-secret-passphrase');
      SerializerService.registerCodec(codec);
      const original = {
        ssn: '123-45-6789',
        creditCard: '4111111111111111',
        nested: { secret: true },
      };
      const stored = SerializerService.toString(original);
      expect(stored).toMatch(/^\/b/);
      // Encoded value should NOT contain plaintext
      expect(stored).not.toContain('123-45-6789');
      expect(stored).not.toContain('4111111111111111');
      const restored = SerializerService.fromString(stored);
      expect(restored).toEqual(original);
    });

    it('should support removing a codec with null', () => {
      SerializerService.registerCodec(base64Codec);
      expect(SerializerService.toString({ a: 1 })).toMatch(/^\/b/);
      SerializerService.registerCodec(null);
      expect(SerializerService.toString({ a: 1 })).toMatch(/^\/s/);
      expect(SerializerService.getCodec()).toBeNull();
    });
  });

  // ---- Integration: HotMesh.registerCodec ----

  describe('HotMesh.registerCodec()', () => {
    it('should delegate to SerializerService', () => {
      HotMesh.registerCodec(base64Codec);
      expect(SerializerService.getCodec()).toBe(base64Codec);
      HotMesh.registerCodec(null);
      expect(SerializerService.getCodec()).toBeNull();
    });
  });

  // ---- End-to-end: workflow with codec ----

  describe('end-to-end workflow with base64 codec', () => {
    it('should initialize HotMesh and run a workflow with codec enabled', async () => {
      // Register codec BEFORE init so all serialization uses it
      HotMesh.registerCodec(base64Codec);

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
            topic: 'codec.echo',
            connection: {
              store: { class: Postgres, options: postgres_options },
              stream: { class: Postgres, options: postgres_options },
              sub: { class: Postgres, options: postgres_options },
            },
            callback: async (streamData: StreamData) => {
              return {
                status: StreamStatus.SUCCESS,
                metadata: { ...streamData.metadata },
                data: {
                  echoed: true,
                  input: streamData.data,
                  nested: { deep: { value: 'codec-works' } },
                },
              } as StreamDataResponse;
            },
          },
        ],
      };
      hotMesh = await HotMesh.init(config);

      // Deploy and activate a simple workflow
      await hotMesh.deploy('/app/tests/$setup/seeds/codec.yaml');
      await hotMesh.activate('1');
    }, 15_000);

    it('should execute a workflow that completes through the codec', async () => {
      const payload = {
        id: `codec_${guid()}`,
        message: 'hello-codec',
        numbers: [1, 2, 3],
      };
      const jobId = await hotMesh.pub('codec.test', payload);
      expect(jobId).toBeDefined();
      // Wait for the workflow to complete
      const { sleepFor } = await import('../../../modules/utils');
      await sleepFor(2_000);
      const status = await hotMesh.getStatus(jobId);
      expect(status).toBe(0); // 0 = completed
    }, 10_000);

    it('should not store any /s object values when codec is active', async () => {
      // In a YAML HotMesh workflow, most values are scalars (strings,
      // numbers). But any object values that DO flow through the
      // serializer should use /b, never /s.
      const schemaName = appId.replace(/[^a-zA-Z0-9_]/g, '_');
      const result = await postgresClient.query(
        `SELECT value FROM ${schemaName}.jobs_attributes
         WHERE value LIKE '/s%'
         LIMIT 1`,
      );
      expect(result.rows.length).toBe(0);
    });
  });
});
