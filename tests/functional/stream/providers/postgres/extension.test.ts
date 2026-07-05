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
 * Reservation extension (heartbeat support). While an activity callback
 * runs, the consumer periodically refreshes reserved_at so the message
 * stays leased past the base reservation window. The refresh is scoped
 * to the owning consumer and to live rows, so a reclaimed or expired
 * message reports 0 and the stale consumer abandons. A consumer that
 * stops heartbeating (crash) leaves reserved_at to age out, and the
 * message is rescued through the normal claim path.
 */
describe('FUNCTIONAL | PostgresStreamService | reservation extension', () => {
  let postgresClient: ProviderNativeClient;
  let service: PostgresStreamService;
  const APP_ID = 'mytestapp';
  const SCHEMA = APP_ID;
  const STREAM = 'extensionStream';
  const GROUP = 'WORKER';
  const CONSUMER = 'extConsumer';

  const msg = (idx: number): string =>
    JSON.stringify({
      type: 'worker',
      metadata: { jid: `jid${idx}`, topic: STREAM },
      data: { idx },
    });

  const backdateReservations = async (seconds: number): Promise<void> => {
    await postgresClient.query(
      `UPDATE ${SCHEMA}.worker_streams
       SET reserved_at = NOW() - INTERVAL '${seconds} seconds'
       WHERE stream_name = $1 AND expired_at IS NULL AND reserved_at IS NOT NULL`,
      [STREAM],
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

  it('should advertise reservation extension support', () => {
    const features = service.getProviderSpecificFeatures();
    expect(features.supportsReservationExtension).toBe(true);
  });

  it('should extend an owned reservation so the message stays leased', async () => {
    await service.publishMessages(STREAM, [msg(1)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    //the lease has nearly lapsed; a heartbeat refreshes it
    await backdateReservations(60);
    const extended = await service.extendReservation(
      STREAM,
      claimed[0].id,
      CONSUMER,
    );
    expect(extended).toBe(1);

    //the refreshed reservation is honored: the message is claimed, not reclaimable
    const reclaimed = await service.consumeMessages(STREAM, GROUP, 'rival');
    expect(reclaimed).toHaveLength(0);
  });

  it('should report 0 when a different consumer holds the reservation', async () => {
    await service.publishMessages(STREAM, [msg(2)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    const extended = await service.extendReservation(
      STREAM,
      claimed[0].id,
      'someoneElse',
    );
    expect(extended).toBe(0);
  });

  it('should report 0 once the message is expired', async () => {
    await service.publishMessages(STREAM, [msg(3)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    await service.ackAndDelete(STREAM, GROUP, [claimed[0].id]);
    const extended = await service.extendReservation(
      STREAM,
      claimed[0].id,
      CONSUMER,
    );
    expect(extended).toBe(0);
  });

  it('should rescue a message whose consumer stopped heartbeating', async () => {
    await service.publishMessages(STREAM, [msg(4)]);
    const claimed = await service.consumeMessages(STREAM, GROUP, CONSUMER);
    expect(claimed).toHaveLength(1);

    //the consumer crashes: heartbeats stop and the reservation ages out
    await backdateReservations(600);
    const rescued = await service.consumeMessages(STREAM, GROUP, 'rescuer');
    expect(rescued).toHaveLength(1);
    expect(rescued[0].id).toBe(claimed[0].id);
  });
});
