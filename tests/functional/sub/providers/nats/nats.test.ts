import { connect } from 'nats';

import { KeyType, HMNS } from '../../../../../modules/key';
import { sleepFor } from '../../../../../modules/utils';
import { NatsConnection } from '../../../../../services/connector/providers/nats';
import { LoggerService } from '../../../../../services/logger';
import { NatsSubService } from '../../../../../services/sub/providers/nats/nats';
import { SubscriptionCallback } from '../../../../../types/quorum';
import { NatsClientType } from '../../../../../types/nats';

describe('FUNCTIONAL | NatsSubService', () => {
  const appConfig = { id: 'test-app', version: '1' };
  const engineId = '9876543210';
  let natsSubClient: NatsClientType;
  let natsPubClient: NatsClientType;
  let natsSubService: NatsSubService;

  beforeAll(async () => {
    // Initialize NATS connection
    const natsConnection = await NatsConnection.connect(
      'test-connection-1',
      connect,
      { servers: ['nats:4222'] },
    );
    natsSubClient = natsConnection.getClient();
    const natsPubConnection = await NatsConnection.connect(
      'test-connection-2',
      connect,
      { servers: ['nats:4222'] },
    );
    natsPubClient = natsPubConnection.getClient();
  });

  afterAll(async () => {
    await natsSubClient.close();
    await natsPubClient.close();
  });

  beforeEach(async () => {
    natsSubService = new NatsSubService(natsSubClient, natsPubClient);
    await natsSubService.init(HMNS, appConfig.id, engineId, new LoggerService(appConfig.id, engineId, 'nats-sub-service', 'debug'));
  });

  describe('subscribe/unsubscribe', () => {
    it('subscribes and unsubscribes for an app', async () => {
      let responded = false;
      const payload = { any: 'data' };
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = natsSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
        responded = true;
      };

      await natsSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
      );

      await natsSubService.publish(KeyType.QUORUM, payload, appConfig.id);

      // Wait for the message to be processed
      await sleepFor(250);

      await natsSubService.unsubscribe(KeyType.QUORUM, appConfig.id);
      expect(responded).toBeTruthy();
    });

    it('subscribes and unsubscribes for an app engine target', async () => {
      const specificEngineId = 'cat';
      let responded = false;
      const payload = { any: 'data' };
      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = natsSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
          engineId: specificEngineId,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
        responded = true;
      };

      await natsSubService.subscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
        specificEngineId,
      );

      await natsSubService.publish(
        KeyType.QUORUM,
        payload,
        appConfig.id,
        specificEngineId,
      );

      // Wait for the message to be processed
      await sleepFor(250);

      await natsSubService.unsubscribe(
        KeyType.QUORUM,
        appConfig.id,
        specificEngineId,
      );
      expect(responded).toBeTruthy();
    });
  });

  describe('psubscribe/punsubscribe', () => {
    it('psubscribes and punsubscribes', async () => {
      const payload = { any: 'data' };
      let responded = false;
      const word = 'dog';
      const natsWildcard = '>'; // or *

      const subscriptionHandler: SubscriptionCallback = (topic, message) => {
        const topicKey = natsSubService.mintKey(KeyType.QUORUM, {
          appId: appConfig.id,
          engineId: word,
        });
        expect(topic).toEqual(topicKey);
        expect(message).toEqual(payload);
        responded = true;
      };

      await natsSubService.psubscribe(
        KeyType.QUORUM,
        subscriptionHandler,
        appConfig.id,
        natsWildcard,
      );

      await natsSubService.publish(
        KeyType.QUORUM,
        payload,
        appConfig.id,
        word,
      );

      // Wait for the message to be processed
      await sleepFor(1_000);

      await natsSubService.punsubscribe(
        KeyType.QUORUM,
        appConfig.id,
        natsWildcard,
      );
      expect(responded).toBeTruthy();
    });
  });
});
