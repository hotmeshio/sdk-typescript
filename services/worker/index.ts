import { KeyType } from '../../modules/key';
import {
  XSleepFor,
  formatISODate,
  getSystemHealth,
  identifyRedisType,
  sleepFor,
} from '../../modules/utils';
import { ConnectorService } from '../connector';
import { ILogger } from '../logger';
import { Router } from '../router';
import { StoreService } from '../store';
import { IORedisStoreService as IORedisStore } from '../store/clients/ioredis';
import { RedisStoreService as RedisStore } from '../store/clients/redis';
import { StreamService } from '../stream';
import { IORedisStreamService as IORedisStream } from '../stream/clients/ioredis';
import { RedisStreamService as RedisStream } from '../stream/clients/redis';
import { SubService } from '../sub';
import { IORedisSubService as IORedisSub } from '../sub/clients/ioredis';
import { RedisSubService as RedisSub } from '../sub/clients/redis';
import { HotMeshConfig, HotMeshWorker } from '../../types/hotmesh';
import {
  QuorumMessage,
  QuorumProfile,
  RollCallMessage,
  SubscriptionCallback,
} from '../../types/quorum';
import {
  IORedisClientType,
  RedisClient,
  RedisMulti,
  RedisRedisClientType as RedisClientType,
} from '../../types/redis';
import { StreamData, StreamRole, StreamDataResponse } from '../../types/stream';
import { HMSH_QUORUM_ROLLCALL_CYCLES } from '../../modules/enums';
import { StringStringType } from '../../types';

class WorkerService {
  namespace: string;
  appId: string;
  guid: string;
  topic: string;
  config: HotMeshConfig;
  callback: (streamData: StreamData) => Promise<StreamDataResponse | void>;
  store: StoreService<RedisClient, RedisMulti> | null;
  stream: StreamService<RedisClient, RedisMulti> | null;
  subscribe: SubService<RedisClient, RedisMulti> | null;
  router: Router | null;
  logger: ILogger;
  reporting = false;
  inited: string;
  rollCallInterval: NodeJS.Timeout;

  static async init(
    namespace: string,
    appId: string,
    guid: string,
    config: HotMeshConfig,
    logger: ILogger,
  ): Promise<WorkerService[]> {
    const services: WorkerService[] = [];
    if (Array.isArray(config.workers)) {
      for (const worker of config.workers) {
        await ConnectorService.initRedisClients(
          worker.redis?.class,
          worker.redis?.options,
          worker,
        );

        const service = new WorkerService();
        service.verifyWorkerFields(worker);
        service.namespace = namespace;
        service.appId = appId;
        service.guid = guid;
        service.callback = worker.callback;
        service.topic = worker.topic;
        service.config = config;
        service.logger = logger;

        await service.initStoreChannel(service, worker.store);
        await service.initSubChannel(service, worker.sub);
        await service.subscribe.subscribe(
          KeyType.QUORUM,
          service.subscriptionHandler(),
          appId,
        );
        await service.subscribe.subscribe(
          KeyType.QUORUM,
          service.subscriptionHandler(),
          appId,
          service.topic,
        );
        await service.subscribe.subscribe(
          KeyType.QUORUM,
          service.subscriptionHandler(),
          appId,
          service.guid,
        );
        await service.initStreamChannel(service, worker.stream);
        service.router = await service.initRouter(worker, logger);

        const key = service.stream.mintKey(KeyType.STREAMS, {
          appId: service.appId,
          topic: worker.topic,
        });
        await service.router.consumeMessages(
          key,
          'WORKER',
          service.guid,
          worker.callback,
        );
        service.inited = formatISODate(new Date());
        services.push(service);
      }
    }
    return services;
  }

  verifyWorkerFields(worker: HotMeshWorker) {
    if (
      !identifyRedisType(worker.store) ||
      !identifyRedisType(worker.stream) ||
      !identifyRedisType(worker.sub) ||
      !(worker.topic && worker.callback)
    ) {
      throw new Error(
        'worker must include `store`, `stream`, and `sub` fields along with a callback function and topic.',
      );
    }
  }

  async initStoreChannel(service: WorkerService, store: RedisClient) {
    if (identifyRedisType(store) === 'redis') {
      service.store = new RedisStore(store as RedisClientType);
    } else {
      service.store = new IORedisStore(store as IORedisClientType);
    }
    await service.store.init(service.namespace, service.appId, service.logger);
  }

  async initSubChannel(service: WorkerService, sub: RedisClient) {
    if (identifyRedisType(sub) === 'redis') {
      service.subscribe = new RedisSub(sub as RedisClientType);
    } else {
      service.subscribe = new IORedisSub(sub as IORedisClientType);
    }
    await service.subscribe.init(
      service.namespace,
      service.appId,
      service.guid,
      service.logger,
    );
  }

  async initStreamChannel(service: WorkerService, stream: RedisClient) {
    if (identifyRedisType(stream) === 'redis') {
      service.stream = new RedisStream(stream as RedisClientType);
    } else {
      service.stream = new IORedisStream(stream as IORedisClientType);
    }
    await service.stream.init(service.namespace, service.appId, service.logger);
  }

  async initRouter(worker: HotMeshWorker, logger: ILogger): Promise<Router> {
    const throttle = await this.store.getThrottleRate(worker.topic);

    return new Router(
      {
        namespace: this.namespace,
        appId: this.appId,
        guid: this.guid,
        role: StreamRole.WORKER,
        topic: worker.topic,
        reclaimDelay: worker.reclaimDelay,
        reclaimCount: worker.reclaimCount,
        throttle,
      },
      this.stream,
      this.store,
      logger,
    );
  }

  subscriptionHandler(): SubscriptionCallback {
    const self = this;
    return async (topic: string, message: QuorumMessage) => {
      self.logger.debug('worker-event-received', { topic, type: message.type });
      if (message.type === 'throttle') {
        if (message.topic !== null) {
          //undefined allows passthrough
          self.throttle(message.throttle);
        }
      } else if (message.type === 'ping') {
        self.sayPong(
          self.appId,
          self.guid,
          message.originator,
          message.details,
        );
      } else if (message.type === 'rollcall') {
        if (message.topic !== null) {
          //undefined allows passthrough
          self.doRollCall(message);
        }
      }
    };
  }

  /**
   * A quorum-wide command to broadcaset system details.
   *
   */
  async doRollCall(message: RollCallMessage) {
    let iteration = 0;
    const max = !isNaN(message.max) ? message.max : HMSH_QUORUM_ROLLCALL_CYCLES;
    if (this.rollCallInterval) clearTimeout(this.rollCallInterval);
    const base = message.interval / 2;
    const amount = base + Math.ceil(Math.random() * base);
    do {
      await sleepFor(Math.ceil(Math.random() * 1000));
      await this.sayPong(this.appId, this.guid, null, true, message.signature);
      if (!message.interval) return;
      const { promise, timerId } = XSleepFor(amount * 1000);
      this.rollCallInterval = timerId;
      await promise;
    } while (this.rollCallInterval && iteration++ < max - 1);
  }

  cancelRollCall() {
    if (this.rollCallInterval) {
      clearTimeout(this.rollCallInterval);
      delete this.rollCallInterval;
    }
  }

  stop() {
    this.cancelRollCall();
  }

  async sayPong(
    appId: string,
    guid: string,
    originator?: string,
    details = false,
    signature = false,
  ) {
    let profile: QuorumProfile;
    if (details) {
      const params = {
        appId: this.appId,
        topic: this.topic,
      };

      profile = {
        engine_id: this.guid,
        namespace: this.namespace,
        app_id: this.appId,
        worker_topic: this.topic,
        stream: this.stream.mintKey(KeyType.STREAMS, params),
        counts: this.router.counts,
        timestamp: formatISODate(new Date()),
        inited: this.inited,
        throttle: this.router.throttle,
        reclaimDelay: this.router.reclaimDelay,
        reclaimCount: this.router.reclaimCount,
        system: await getSystemHealth(),
        signature: signature ? this.callback.toString() : undefined,
      };
    }
    this.store.publish(
      KeyType.QUORUM,
      {
        type: 'pong',
        guid,
        originator,
        profile,
      },
      appId,
    );
  }

  async throttle(delayInMillis: number) {
    this.router.setThrottle(delayInMillis);
  }
}

export { WorkerService };
