import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { Client } from 'pg';

import { dropTables, postgres_options } from '../../$setup/postgres';
import {
  beginOutage,
  createCrashGuard,
  endOutage,
  terminateBackends,
  waitFor,
} from '../../$setup/postgres/chaos';
import { HMNS, KeyType } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { LoggerService } from '../../../services/logger';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { PostgresSubService } from '../../../services/sub/providers/postgres/postgres';
import { PostgresStreamService } from '../../../services/stream/providers/postgres/postgres';
import { PostgresClientType } from '../../../types/postgres';
import { ProviderClient } from '../../../types/provider';
import { StreamMessage } from '../../../types/stream';

type PgClient = PostgresClientType & ProviderClient;

/**
 * LISTEN is session state. After a restart replaces the session, the
 * sub service re-arms its quorum channels and the stream service
 * re-arms its stream channels and fetches at once, so messages sent
 * while the session was gone are still delivered.
 */
describe('FUNCTIONAL | LISTEN re-armed after a reconnect', () => {
  const logger = new LoggerService('hotmesh', 'resilience-test');
  const guard = createCrashGuard();
  let writer: Client;

  const connect = async (applicationName: string): Promise<PgClient> =>
    (
      await PostgresConnection.connect(guid(), Client as any, {
        ...postgres_options,
        application_name: applicationName,
      } as any)
    ).getClient() as PgClient;

  const generationOf = (client: PgClient): number => (client as any).connectionGeneration;
  const connected = (client: PgClient): boolean => (client as any).isConnected;

  beforeAll(async () => {
    guard.install();
    //a writer outside HotMesh's application_name: it stays up during outages
    writer = new Client({ ...postgres_options, application_name: 'resilience-writer' } as any);
    writer.on('error', () => undefined);
    await writer.connect();
    await dropTables(writer);
  });

  afterEach(async () => {
    await endOutage();
    expect(guard.crashes).toEqual([]);
  });

  afterAll(async () => {
    await endOutage();
    await writer.end();
    await PostgresConnection.disconnectAll();
    guard.uninstall();
  });

  it('pub/sub: a subscription receives messages published after the restart', async () => {
    const app = `sub-relisten-${guid()}`;
    const appId = `subapp${Date.now()}`;
    const eventClient = await connect(app);
    const storeClient = await connect(`${app}-store`);
    const sub = new PostgresSubService(eventClient, storeClient);
    await sub.init(HMNS, appId, 'engine1', logger);

    const received: any[] = [];
    await sub.subscribe(KeyType.QUORUM, (_topic, payload) => received.push(payload), appId);
    await sub.publish(KeyType.QUORUM, { n: 1 }, appId);
    await waitFor(() => received.length === 1, 3_000, 'first message');

    await terminateBackends(app);
    await waitFor(() => connected(eventClient) && generationOf(eventClient) === 1, 5_000, 'reconnect');
    //the re-LISTEN is issued on the reconnected event; give it a moment
    await sleepFor(200);

    await sub.publish(KeyType.QUORUM, { n: 2 }, appId);
    await waitFor(() => received.length === 2, 3_000, 'message after restart');
    expect(received).toEqual([{ n: 1 }, { n: 2 }]);
    await sub.unsubscribe(KeyType.QUORUM, appId);
  });

  it('pub/sub: a subscription made while the session is down is armed when it returns', async () => {
    const app = `sub-deferred-${guid()}`;
    const appId = `subdeferred${Date.now()}`;
    const eventClient = await connect(app);
    const storeClient = await connect(`${app}-store`);
    const sub = new PostgresSubService(eventClient, storeClient);
    await sub.init(HMNS, appId, 'engine1', logger);

    await beginOutage(app);
    await waitFor(() => !connected(eventClient), 3_000, 'loss observed');
    const received: any[] = [];
    //does not throw: the channel is kept and re-armed on reconnect
    await sub.subscribe(KeyType.QUORUM, (_topic, payload) => received.push(payload), appId);
    await endOutage();
    await waitFor(() => connected(eventClient), 5_000, 'reconnect');
    await sleepFor(200);

    await sub.publish(KeyType.QUORUM, { n: 1 }, appId);
    await waitFor(() => received.length === 1, 3_000, 'message to the deferred subscription');
    await sub.unsubscribe(KeyType.QUORUM, appId);
  });

  it('pub/sub: an async subscriber that rejects is logged, never an unhandled rejection', async () => {
    const app = `sub-async-${guid()}`;
    const appId = `subasync${Date.now()}`;
    const client = await connect(app);
    const sub = new PostgresSubService(client, client);
    await sub.init(HMNS, appId, 'engine1', logger);
    let calls = 0;
    await sub.subscribe(
      KeyType.QUORUM,
      async () => {
        calls++;
        throw new Error('subscriber fault');
      },
      appId,
    );
    await sub.publish(KeyType.QUORUM, { n: 1 }, appId);
    await waitFor(() => calls === 1, 3_000, 'subscriber called');
    await sleepFor(50);
    await sub.unsubscribe(KeyType.QUORUM, appId);
  });

  it('pub/sub: publish during an outage returns false instead of throwing', async () => {
    const app = `sub-outage-${guid()}`;
    const appId = `suboutage${Date.now()}`;
    const client = await connect(app);
    const sub = new PostgresSubService(client, client);
    await sub.init(HMNS, appId, 'engine1', logger);
    await beginOutage(app);
    await waitFor(() => !connected(client), 3_000, 'loss observed');
    await expect(sub.publish(KeyType.QUORUM, { n: 1 }, appId)).resolves.toBe(false);
    await endOutage();
    await waitFor(() => connected(client), 5_000, 'reconnect');
    await expect(sub.publish(KeyType.QUORUM, { n: 2 }, appId)).resolves.toBe(true);
  });

  it('stream: a released reservation is claimable at once, and only its owner can release it', async () => {
    const app = `stream-release-${guid()}`;
    const appId = `streamrelease${Date.now()}`;
    const client = await connect(app);
    const service = new PostgresStreamService(client, {} as ProviderClient);
    await service.init(HMNS, appId, logger);
    const streamKey = service.mintKey(KeyType.STREAMS, { appId, topic: 'release.topic' });
    await service.createStream(streamKey);
    await service.createConsumerGroup(streamKey, 'WORKER');
    await service.publishMessages(streamKey, [JSON.stringify({ n: 1 })]);

    const [claimed] = await service.consumeMessages(streamKey, 'WORKER', 'owner', {
      enableNotifications: false,
      reservationTimeout: 600,
    });
    expect(claimed).toBeDefined();
    //reserved for ten minutes: nobody else can claim it
    expect(
      await service.consumeMessages(streamKey, 'WORKER', 'rival', {
        enableNotifications: false,
        reservationTimeout: 600,
      }),
    ).toEqual([]);

    //a foreign consumer cannot release the owner's reservation
    expect(await service.releaseReservations(streamKey, [claimed.id], 'rival')).toBe(0);
    expect(await service.releaseReservations(streamKey, [claimed.id], 'owner')).toBe(1);

    const [reclaimed] = await service.consumeMessages(streamKey, 'WORKER', 'rival', {
      enableNotifications: false,
      reservationTimeout: 600,
    });
    expect(reclaimed?.id).toBe(claimed.id);
    await service.ackAndDelete(streamKey, 'WORKER', [reclaimed.id]);
    await service.cleanup();
  });

  it('stream: a message published while the consumer session is down is delivered after the restore', async () => {
    const app = `stream-relisten-${guid()}`;
    const appId = `streamapp${Date.now()}`;
    const consumerClient = await connect(app);
    const consumerService = new PostgresStreamService(consumerClient, {} as ProviderClient);
    await consumerService.init(HMNS, appId, logger);

    //the publisher rides the writer, which the outage leaves connected
    const publisherService = new PostgresStreamService(writer as any, {} as ProviderClient);
    await publisherService.init(HMNS, appId, logger);

    const streamKey = consumerService.mintKey(KeyType.STREAMS, { appId, topic: 'resilience.topic' });
    await consumerService.createStream(streamKey);
    await consumerService.createConsumerGroup(streamKey, 'WORKER');

    const delivered: StreamMessage[] = [];
    const callback = async (messages: StreamMessage[]) => {
      delivered.push(...messages);
      await consumerService.ackAndDelete(streamKey, 'WORKER', messages.map((m) => m.id));
    };
    await consumerService.consumeMessages(streamKey, 'WORKER', 'consumer-1', {
      enableNotifications: true,
      notificationCallback: callback,
    });

    await publisherService.publishMessages(streamKey, [JSON.stringify({ n: 1 })]);
    await waitFor(() => delivered.length === 1, 5_000, 'first stream message');

    await beginOutage(app);
    await waitFor(() => !connected(consumerClient), 3_000, 'loss observed');
    //NOTIFY for this insert reaches no listener: the consumer session is gone
    await publisherService.publishMessages(streamKey, [JSON.stringify({ n: 2 })]);
    await sleepFor(500);
    expect(delivered.length).toBe(1);

    await endOutage();
    const tookMs = await waitFor(() => delivered.length === 2, 8_000, 'message sent during the outage');
    //well inside the 30s fallback poller: the re-armed consumer fetched at once
    expect(tookMs).toBeLessThan(5_000);

    //and live notifications flow again on the new session
    await publisherService.publishMessages(streamKey, [JSON.stringify({ n: 3 })]);
    await waitFor(() => delivered.length === 3, 5_000, 'message after restore');

    await consumerService.stopNotificationConsumer(streamKey, 'WORKER');
    await consumerService.cleanup();
  }, 30_000);
});
