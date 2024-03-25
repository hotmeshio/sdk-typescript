import Redis from 'ioredis';

import config from '../../$setup/config';
import { HMNS } from '../../../modules/key';
import { guid, sleepFor } from '../../../modules/utils';
import { HotMesh, HotMeshConfig } from '../../../index';
import { MathHandler } from '../../../services/pipe/functions/math';
import { RedisConnection } from '../../../services/connector/clients/ioredis';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus } from '../../../types/stream';
import { PongMessage, QuorumMessage, RollCallMessage, ThrottleMessage } from '../../../types/quorum';
import { QuorumService } from '../../../services/quorum';
import { HMSH_LOGLEVEL } from '../../../modules/enums';

describe('FUNCTIONAL | Quorum', () => {
  const appConfig = { id: 'calc', version: '1' };
  const options = {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD,
    db: config.REDIS_DATABASE,
  };
  let hotMesh: HotMesh;

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(guid(), Redis, options);
    redisConnection.getClient().flushdb();

    //init/activate HotMesh (test both `engine` and `worker` roles)
    const config: HotMeshConfig = {
      appId: appConfig.id,
      namespace: HMNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: { class: Redis, options }
      },
      workers: [
        {
          topic: 'calculation.execute',
          redis: { class: Redis, options },
          callback: async (streamData: StreamData): Promise<StreamDataResponse> => {
            const values = JSON.parse(streamData.data.values as string) as number[];
            const operation = streamData.data.operation as 'add'|'subtract'|'multiply'|'divide';
            const result = new MathHandler()[operation](values);
            return {
              status: StreamStatus.SUCCESS,
              metadata: { ...streamData.metadata },
              data: { result },
            } as StreamDataResponse;
          }
        }
      ]
    };
    hotMesh = await HotMesh.init(config);
    await hotMesh.deploy('/app/tests/$setup/apps/calc/v1/hotmesh.yaml');
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Setup', () => {
    it('activates a version', async () => {
      const isActivated = await hotMesh.activate(appConfig.version);
      expect(isActivated).toBe(true);
    });
  });

  describe('Run', () => {
    it('should run synchronous calls in parallel', async () => {
      const payload = {
        operation: 'divide',
        values: JSON.stringify([200, 4, 5]),
      };
      const [divide, b, c, d, multiply] = await Promise.all([
        hotMesh.pubsub('calculate', payload, null, 5000),
        hotMesh.pubsub('calculate', payload, null, 5000),
        hotMesh.pubsub('calculate', payload, null, 5000),
        hotMesh.pubsub('calculate', payload, null, 5000),
        hotMesh.pubsub('calculate', {
          operation: 'multiply',
          values: JSON.stringify([10, 10, 10]),
        }, null, 7_500),
      ]);
      expect(divide?.data.result).toBe(10);
      expect(multiply?.data.result).toBe(1000);
    }, 20_000);
  });

  describe('Pub Sub', () => {
    it('sends a `throttle` message targeting a worker (topic)', async () => {
      const callback = (topic: string, message: QuorumMessage) => {
        expect(['throttle', 'job'].includes(message.type)).toBeTruthy();
        expect((message as ThrottleMessage).topic).toBe('calculation.execute');
      };
      hotMesh.quorum?.sub(callback);
      const throttleMessage: ThrottleMessage = {
        type: 'throttle',
        topic: 'calculation.execute',
        throttle: 1000,
      };
      await hotMesh.quorum?.pub(throttleMessage);
      await sleepFor(1000);
      hotMesh.quorum?.unsub(callback);
    });

    it('sends a `throttle` message targeting an engine (guid)', async () => {
      const callback = (topic: string, message: QuorumMessage) => {
        expect(['throttle', 'job'].includes(message.type)).toBeTruthy();
        expect((message as ThrottleMessage).guid).toBe(hotMesh.quorum?.guid);
      };
      hotMesh.quorum?.sub(callback);
      const throttleMessage: ThrottleMessage = {
        type: 'throttle',
        guid: hotMesh.quorum?.guid,
        throttle: 1000,
      };
      await hotMesh.quorum?.pub(throttleMessage);
      await sleepFor(1000);
      hotMesh.quorum?.unsub(callback);
    });

    it('sends a `throttle` message to ALL quorum members', async () => {
      const callback = (topic: string, message: QuorumMessage) => {
        expect(['throttle', 'job'].includes(message.type)).toBeTruthy();
        expect((message as ThrottleMessage).guid).toBeUndefined();
        expect((message as ThrottleMessage).topic).toBeUndefined();
        expect((message as ThrottleMessage).throttle).toBe(500);
      };
      hotMesh.quorum?.sub(callback);
      const throttleMessage: ThrottleMessage = {
        type: 'throttle',
        throttle: 500,
      };
      await hotMesh.quorum?.pub(throttleMessage);
      await sleepFor(500);
      hotMesh.quorum?.unsub(callback);
    });

    it('sends a `rollcall` message to WORKER quorum members', async () => {
      let count = 0;
      const callback = (topic: string, message: QuorumMessage) => {
        if (message.type === 'pong' && message.profile?.worker_topic === 'calculation.execute') {
          if (message.originator === null) {
            //make sure rollcall pong message doesn't have an originator
            expect(message.profile.signature).not.toBeUndefined();
          }
          count++;
        }
      };
      hotMesh.quorum?.sub(callback);
      const rollCallMessage: RollCallMessage = {
        type: 'rollcall',
        interval: 6,
        topic: 'calculation.execute',
        guid: hotMesh.guid,
        signature: true,
        max: 2,
      };
      await hotMesh.quorum?.pub(rollCallMessage);
      await sleepFor(12_000);
      expect(count).toBe(2);
      hotMesh.quorum?.unsub(callback);
    }, 20_000);

    it('sends a `rollcall` message to ENGINE quorum members', async () => {
      let count = 0;
      const callback = (topic: string, message: QuorumMessage) => {
        if (message.type === 'pong') {
          count++;
        }
      };
      hotMesh.quorum?.sub(callback);
      const rollCallMessage: RollCallMessage = {
        type: 'rollcall',
        interval: 6,
        topic: null,
        guid: hotMesh.guid,
      };
      await hotMesh.quorum?.pub(rollCallMessage);
      await sleepFor(12_000);
      expect(count).toBeGreaterThanOrEqual(2);
      hotMesh.quorum?.unsub(callback);
    }, 20_000);

    it('requests quorum count', async () => {
      (hotMesh.quorum as QuorumService).quorum = 0;
      await hotMesh.quorum?.requestQuorum(1_000, true);
      expect(hotMesh.quorum?.quorum).toBe(2);
      expect(hotMesh.quorum?.profiles.length).toBe(2);
    });

    it('requests a quorum rollCall', async () => {
      (hotMesh.quorum as QuorumService).quorum = 0;
      await hotMesh.rollCall(1_000);
      expect(hotMesh.quorum?.profiles.length).toBe(2);
    });
  });
});
