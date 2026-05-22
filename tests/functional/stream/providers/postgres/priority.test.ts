import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { Client } from 'pg';

import { HMNS } from '../../../../../modules/key';
import { PostgresConnection } from '../../../../../services/connector/providers/postgres';
import { LoggerService } from '../../../../../services/logger';
import { PostgresStreamService } from '../../../../../services/stream/providers/postgres/postgres';
import { StreamDataType } from '../../../../../types/stream';
import { PostgresClientType } from '../../../../../types/postgres';
import {
  ProviderNativeClient,
  ProviderClient,
} from '../../../../../types/provider';
import { dropTables } from '../../../../$setup/postgres';

describe('FUNCTIONAL | Stream Priority Ordering', () => {
  let postgresClient: ProviderNativeClient;
  let postgresStreamService: PostgresStreamService;
  const TEST_STREAM = 'priorityStream';
  const TEST_GROUP = 'WORKER';
  const TEST_CONSUMER = 'priorityConsumer';

  const typedMsg = (type: StreamDataType, id: number): string => {
    return JSON.stringify({
      metadata: { guid: `guid-${id}`, jid: `job-${id}`, aid: `a${id}` },
      type,
      data: { id },
    });
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
    if (postgresStreamService) {
      try {
        await postgresStreamService.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
    }

    postgresStreamService = new PostgresStreamService(
      postgresClient as PostgresClientType & ProviderClient,
      {} as ProviderClient,
    );
    await postgresStreamService.init(HMNS, 'priorityapp', new LoggerService());

    try {
      await postgresStreamService.deleteStream('*');
    } catch (error) {
      // Stream might not exist
    }
  });

  afterEach(async () => {
    if (postgresStreamService) {
      try {
        await postgresStreamService.cleanup();
      } catch (error) {
        // Ignore cleanup errors
      }
    }
  });

  afterAll(async () => {
    await postgresClient.end();
  });

  it('should dequeue higher-priority messages before lower-priority ones', async () => {
    await postgresStreamService.createStream(TEST_STREAM);

    // Publish in ascending priority order: AWAIT (0), WORKER (1), TRANSITION (2), WEBHOOK (4)
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.AWAIT, 1),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.WORKER, 2),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.TRANSITION, 3),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.WEBHOOK, 4),
    ]);

    // Dequeue all 4 one at a time; expect highest priority first
    const m1 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const m2 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const m3 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const m4 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );

    expect(m1).toHaveLength(1);
    expect(m2).toHaveLength(1);
    expect(m3).toHaveLength(1);
    expect(m4).toHaveLength(1);

    // WEBHOOK (4) > TRANSITION (2) > WORKER (1) > AWAIT (0)
    expect(m1[0].data.data.id).toBe(4);
    expect(m2[0].data.data.id).toBe(3);
    expect(m3[0].data.data.id).toBe(2);
    expect(m4[0].data.data.id).toBe(1);
  });

  it('should maintain FIFO order within the same priority level', async () => {
    await postgresStreamService.createStream(TEST_STREAM);

    // Publish 3 TRANSITION messages (all priority 2) in order
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.TRANSITION, 10),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.TRANSITION, 20),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.TRANSITION, 30),
    ]);

    // Dequeue; same priority should come out in insertion order
    const m1 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const m2 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const m3 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );

    expect(m1[0].data.data.id).toBe(10);
    expect(m2[0].data.data.id).toBe(20);
    expect(m3[0].data.data.id).toBe(30);
  });

  it('should prioritize signals and continuations over new entries in a mixed batch', async () => {
    await postgresStreamService.createStream(TEST_STREAM);

    // Simulate a busy system: many AWAIT triggers followed by a WEBHOOK signal
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.AWAIT, 1),
      typedMsg(StreamDataType.AWAIT, 2),
      typedMsg(StreamDataType.AWAIT, 3),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.WEBHOOK, 99),
    ]);
    await postgresStreamService.publishMessages(TEST_STREAM, [
      typedMsg(StreamDataType.RESULT, 50),
    ]);

    // First dequeue should be the WEBHOOK (priority 4), then RESULT (3), then AWAITs (0)
    const first = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    expect(first[0].data.data.id).toBe(99);

    const second = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    expect(second[0].data.data.id).toBe(50);

    // Remaining 3 AWAITs dequeue in FIFO order (one at a time to verify ordering)
    const r1 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const r2 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    const r3 = await postgresStreamService.consumeMessages(
      TEST_STREAM, TEST_GROUP, TEST_CONSUMER, { batchSize: 1 },
    );
    expect(r1[0].data.data.id).toBe(1);
    expect(r2[0].data.data.id).toBe(2);
    expect(r3[0].data.data.id).toBe(3);
  });
});
