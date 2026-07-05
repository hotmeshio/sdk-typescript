import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { Client } from 'pg';

import { HMNS } from '../../../../../modules/key';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { LoggerService } from '../../../../../services/logger';
import { PostgresStreamService } from '../../../../../services/stream/providers/postgres/postgres';
import { PostgresClientType } from '../../../../../types/postgres';
import {
  ProviderNativeClient,
  ProviderClient,
} from '../../../../../types/provider';
import { dropTables } from '../../../../$setup/postgres';

/**
 * Job-liveness protections for the Postgres stream provider:
 *
 * 1. expireJobMessages(jid) — soft-deletes every live stream row that
 *    belongs to a job, across worker and engine streams. Called by the
 *    engine when a job is interrupted so its queued/reserved/retry
 *    messages are never delivered again.
 * 2. Delivery liveness guard — a claimed worker-stream message that is a
 *    REDELIVERY (prior reservation lapsed) or a RETRY (retry_attempt > 0)
 *    is checked against the jobs table; when its job exists and is dead
 *    (status <= 0), the message is expired and dropped instead of
 *    executed. First deliveries skip the check (zero hot-path cost) —
 *    rows that exist at interrupt time are handled by expireJobMessages.
 */
describe('FUNCTIONAL | PostgresStreamService | job liveness', () => {
  let postgresClient: ProviderNativeClient;
  let service: PostgresStreamService;
  const APP_ID = 'mytestapp';
  const SCHEMA = APP_ID;
  const JOB_KEY_PREFIX = `${HMNS}:${APP_ID}:j:`;
  const WORKER_STREAM = 'livenessStream';
  const ENGINE_STREAM_KEY = `${HMNS}:${APP_ID}:x:`; //trailing ':' = engine
  const GROUP = 'WORKER';
  const CONSUMER = 'livenessConsumer';
  const DEAD_STATUS = -1_000_000_000;

  const workerMsg = (jid: string, idx: number, extra = {}): string =>
    JSON.stringify({
      type: 'worker',
      metadata: { jid, topic: WORKER_STREAM },
      data: { idx },
      ...extra,
    });

  const insertJob = async (jid: string, status: number): Promise<void> => {
    await postgresClient.query(
      `INSERT INTO ${SCHEMA}.jobs (key, status, is_live) VALUES ($1, $2, TRUE)`,
      [`${JOB_KEY_PREFIX}${jid}`, status],
    );
  };

  const lapseReservations = async (tableName: string): Promise<void> => {
    await postgresClient.query(
      `UPDATE ${SCHEMA}.${tableName}
       SET reserved_at = NOW() - INTERVAL '600 seconds', visible_at = NOW()
       WHERE expired_at IS NULL`,
    );
  };

  const liveRows = async (
    tableName: string,
    jid: string,
  ): Promise<number> => {
    const res = await postgresClient.query(
      `SELECT COUNT(*)::int AS count FROM ${SCHEMA}.${tableName}
       WHERE jid = $1 AND expired_at IS NULL AND dead_lettered_at IS NULL`,
      [jid],
    );
    return res.rows[0].count;
  };

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect('test', Client, {
        user: 'postgres',
        host: 'postgres',
        database: 'hotmesh',
        password: 'password',
        port: 5432,
      })
    ).getClient();

    await dropTables(postgresClient);
  });

  beforeEach(async () => {
    if (service) {
      try {
        await service.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
    }

    service = new PostgresStreamService(
      postgresClient as PostgresClientType & ProviderClient,
      {} as ProviderClient,
    );
    await service.init(HMNS, APP_ID, new LoggerService());

    try {
      await service.deleteStream('*');
    } catch (error) {
      // Stream might not exist; ignore error
    }

    //a minimal jobs table (the store provider owns the real one); the
    //guard only reads (key, status, is_live)
    await postgresClient.query(
      `CREATE TABLE IF NOT EXISTS ${SCHEMA}.jobs (
        key TEXT NOT NULL,
        status INTEGER NOT NULL,
        is_live BOOLEAN DEFAULT TRUE
      )`,
    );
    await postgresClient.query(`TRUNCATE ${SCHEMA}.jobs`);
  });

  afterEach(async () => {
    if (service) {
      try {
        await service.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
    }
  });

  afterAll(async () => {
    await postgresClient.end();
  });

  describe('expireJobMessages', () => {
    it('should expire every live row for a jid across worker and engine streams', async () => {
      //jidA: one queued, one scheduled retry (future visible_at), one engine row
      await service.publishMessages(WORKER_STREAM, [
        workerMsg('jidA', 1),
        workerMsg('jidA', 2, { _visibilityDelayMs: 60_000 }),
        workerMsg('jidB', 3),
      ]);
      await service.publishMessages(ENGINE_STREAM_KEY, [workerMsg('jidA', 4)]);

      //reserve jidA's queued message (in-flight, as during an interrupt)
      const claimed = await service.consumeMessages(
        WORKER_STREAM,
        GROUP,
        CONSUMER,
        { batchSize: 1 },
      );
      expect(claimed).toHaveLength(1);

      const expired = await service.expireJobMessages('jidA');
      expect(expired).toBe(3);

      expect(await liveRows('worker_streams', 'jidA')).toBe(0);
      expect(await liveRows('engine_streams', 'jidA')).toBe(0);
      expect(await liveRows('worker_streams', 'jidB')).toBe(1);

      //idempotent: everything is already expired
      expect(await service.expireJobMessages('jidA')).toBe(0);
    });
  });

  describe('delivery liveness guard', () => {
    it('should expire and drop a redelivered message whose job is dead', async () => {
      await insertJob('jidA', DEAD_STATUS);
      await service.publishMessages(WORKER_STREAM, [workerMsg('jidA', 1)]);

      //first delivery skips the guard (interrupt-time purge owns rows
      //that already exist when a job dies)
      const first = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(first).toHaveLength(1);

      //the reservation lapses (slow activity, worker restart) — the
      //redelivery is checked and the zombie is dropped, not executed
      await lapseReservations('worker_streams');
      const second = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(second).toHaveLength(0);
      expect(await liveRows('worker_streams', 'jidA')).toBe(0);
    });

    it('should expire and drop a scheduled retry row for a dead job', async () => {
      await insertJob('jidB', DEAD_STATUS);
      await service.publishMessages(WORKER_STREAM, [workerMsg('jidB', 1)]);
      await postgresClient.query(
        `UPDATE ${SCHEMA}.worker_streams SET retry_attempt = 1
         WHERE stream_name = $1 AND expired_at IS NULL`,
        [WORKER_STREAM],
      );

      const consumed = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(consumed).toHaveLength(0);
      expect(await liveRows('worker_streams', 'jidB')).toBe(0);
    });

    it('should deliver redelivered messages for a live job', async () => {
      await insertJob('jidC', 5);
      await service.publishMessages(WORKER_STREAM, [workerMsg('jidC', 1)]);

      const first = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(first).toHaveLength(1);

      await lapseReservations('worker_streams');
      const second = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(second).toHaveLength(1);
      expect(await liveRows('worker_streams', 'jidC')).toBe(1);
    });

    it('should deliver redelivered messages when the job row is missing', async () => {
      //a missing row means the job may still be mid-creation; only a row
      //that exists AND is dead marks a message as a zombie
      await service.publishMessages(WORKER_STREAM, [workerMsg('ghost', 1)]);

      const first = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(first).toHaveLength(1);

      await lapseReservations('worker_streams');
      const second = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(second).toHaveLength(1);
    });

    it('should deliver engine-stream messages regardless of job status', async () => {
      //the guard is scoped to worker streams (activity side effects);
      //engine messages are state transitions the engine itself validates
      await insertJob('jidE', DEAD_STATUS);
      await service.publishMessages(ENGINE_STREAM_KEY, [workerMsg('jidE', 1)]);

      const first = await service.consumeMessages(ENGINE_STREAM_KEY, 'ENGINE', CONSUMER);
      expect(first).toHaveLength(1);

      await lapseReservations('engine_streams');
      const second = await service.consumeMessages(ENGINE_STREAM_KEY, 'ENGINE', CONSUMER);
      expect(second).toHaveLength(1);
    });

    it('should keep delivering when the jobs table is unavailable', async () => {
      await postgresClient.query(`DROP TABLE ${SCHEMA}.jobs`);

      await service.publishMessages(WORKER_STREAM, [workerMsg('jidF', 1)]);
      await postgresClient.query(
        `UPDATE ${SCHEMA}.worker_streams SET retry_attempt = 1
         WHERE stream_name = $1 AND expired_at IS NULL`,
        [WORKER_STREAM],
      );

      //guard self-disables (fail open); delivery continues unharmed
      const first = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(first).toHaveLength(1);

      await lapseReservations('worker_streams');
      const second = await service.consumeMessages(WORKER_STREAM, GROUP, CONSUMER);
      expect(second).toHaveLength(1);
    });
  });
});
