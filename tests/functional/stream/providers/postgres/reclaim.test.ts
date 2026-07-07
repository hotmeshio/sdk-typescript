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
 * Reservation reclaim discovery. Under notification-driven consumption,
 * the 30s fallback poller (notify_visible_messages) is the only thing
 * that surfaces work on a quiet stream. A reservation whose holder died
 * (heartbeats stopped, reserved_at aging) MUST be surfaced once it goes
 * stale — otherwise a process death orphans its in-flight messages
 * forever and the workflows behind them wedge permanently (field data:
 * 13 activities still reserved 31+ minutes after a SIGKILL).
 */
describe('FUNCTIONAL | PostgresStreamService | stale-reservation reclaim', () => {
  let postgresClient: ProviderNativeClient;
  let service: PostgresStreamService;
  const APP_ID = 'mytestapp';
  const SCHEMA = APP_ID;
  const STREAM = 'reclaimStream';
  const GROUP = 'WORKER';
  const CONSUMER = 'reclaimConsumer';

  const msg = (idx: number): string =>
    JSON.stringify({
      type: 'worker',
      metadata: { jid: `jid${idx}`, topic: STREAM },
      data: { idx },
    });

  const pollerCount = async (): Promise<number> => {
    const res = await postgresClient.query(
      `SELECT ${SCHEMA}.notify_visible_messages() AS count`,
    );
    return Number(res.rows[0].count);
  };

  const backdateReservations = async (
    tableName: string,
    seconds: number,
  ): Promise<void> => {
    await postgresClient.query(
      `UPDATE ${SCHEMA}.${tableName}
       SET reserved_at = NOW() - INTERVAL '${seconds} seconds'
       WHERE expired_at IS NULL AND reserved_at IS NOT NULL`,
    );
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

  it('should surface a worker reservation whose holder died', async () => {
    await service.publishMessages(STREAM, [msg(1)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    //holder dies: heartbeats stop, the reservation ages past any window
    await backdateReservations('worker_streams', 120);

    //the fallback poller must surface the stream so a live consumer
    //fetches and reclaims — this is the ONLY discovery path on a
    //quiet stream
    expect(await pollerCount()).toBeGreaterThanOrEqual(1);
  });

  it('should surface an engine reservation whose holder died', async () => {
    await service.publishMessages(`${HMNS}:${APP_ID}:x:`, [msg(2)]);
    const claimed = await service.consumeMessages(
      `${HMNS}:${APP_ID}:x:`,
      'ENGINE',
      CONSUMER,
    );
    expect(claimed).toHaveLength(1);

    await backdateReservations('engine_streams', 120);
    expect(await pollerCount()).toBeGreaterThanOrEqual(1);
  });

  it('should stay quiet for an actively held reservation', async () => {
    await service.publishMessages(STREAM, [msg(3)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    //fresh reservation (heartbeats keep reserved_at within seconds):
    //the poller must not churn notifications for healthy in-flight work
    expect(await pollerCount()).toBe(0);
  });

  it('should still surface unreserved visible messages', async () => {
    await service.publishMessages(STREAM, [msg(4)]);
    expect(await pollerCount()).toBeGreaterThanOrEqual(1);
  });
});
