import { KeyType } from "../../modules/key";
import { ILogger } from "../logger";
import { StreamSignaler } from "../signaler/stream";
import { StoreService } from '../store';
import { RedisStoreService as RedisStore } from '../store/clients/redis';
import { IORedisStoreService as IORedisStore } from '../store/clients/ioredis';
import { StreamService } from '../stream';
import { RedisStreamService as RedisStream } from '../stream/clients/redis';
import { IORedisStreamService as IORedisStream } from '../stream/clients/ioredis';
import { SubService } from '../sub';
import { IORedisSubService as IORedisSub } from '../sub/clients/ioredis';
import { RedisSubService as RedisSub } from '../sub/clients/redis';
import { RedisClientType as IORedisClientType } from '../../types/ioredisclient';
import { HotMeshConfig, HotMeshWorker } from "../../types/hotmesh";
import {
  QuorumMessage,
  SubscriptionCallback } from "../../types/quorum";
import { RedisClient, RedisMulti } from "../../types/redis";
import { RedisClientType } from '../../types/redisclient';
import { StreamRole } from "../../types/stream";
import { identifyRedisType } from "../../modules/utils";
import { ConnectorService } from "../connector";

class WorkerService {
  namespace: string;
  appId: string;
  guid: string;
  topic: string;
  config: HotMeshConfig;
  store: StoreService<RedisClient, RedisMulti> | null;
  stream: StreamService<RedisClient, RedisMulti> | null;
  subscribe: SubService<RedisClient, RedisMulti> | null;
  streamSignaler: StreamSignaler | null;
  logger: ILogger;
  reporting = false;

  static async init(
    namespace: string,
    appId: string,
    guid: string,
    config: HotMeshConfig,
    logger: ILogger
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
        service.topic = worker.topic;
        service.config = config;
        service.logger = logger;

        await service.initStoreChannel(service, worker.store);
        await service.initSubChannel(service, worker.sub);
        await service.subscribe.subscribe(KeyType.QUORUM, service.subscriptionHandler(), appId);
        await service.subscribe.subscribe(KeyType.QUORUM, service.subscriptionHandler(), appId, service.topic);
        await service.subscribe.subscribe(KeyType.QUORUM, service.subscriptionHandler(), appId, service.guid);
        await service.initStreamChannel(service, worker.stream);
        service.streamSignaler = service.initStreamSignaler(worker, logger);

        const key = service.stream.mintKey(KeyType.STREAMS, { appId: service.appId, topic: worker.topic });
        await service.streamSignaler.consumeMessages(
          key,
          'WORKER',
          service.guid,
          worker.callback
        );
        services.push(service);
      }
    }
    return services;
  }

  verifyWorkerFields(worker: HotMeshWorker) {
    if ((!identifyRedisType(worker.store) || 
      !identifyRedisType(worker.stream)||
      !identifyRedisType(worker.sub)) ||
      !(worker.topic && worker.callback)) {
      throw new Error('worker must include `store`, `stream`, and `sub` fields along with a callback function and topic.');
    }
  }

  async initStoreChannel(service: WorkerService, store: RedisClient) {
    if (identifyRedisType(store) === 'redis') {
      service.store = new RedisStore(store as RedisClientType);
    } else {
      service.store = new IORedisStore(store as IORedisClientType);
    }
    await service.store.init(
      service.namespace,
      service.appId,
      service.logger
    );
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
      service.logger
    );
  }

  async initStreamChannel(service: WorkerService, stream: RedisClient) {
    if (identifyRedisType(stream) === 'redis') {
      service.stream = new RedisStream(stream as RedisClientType);
    } else {
      service.stream = new IORedisStream(stream as IORedisClientType);
    }
    await service.stream.init(
      service.namespace,
      service.appId,
      service.logger
    );
  }

  initStreamSignaler(worker: HotMeshWorker, logger: ILogger): StreamSignaler {
    return new StreamSignaler(
      {
        namespace: this.namespace,
        appId: this.appId,
        guid: this.guid,
        role: StreamRole.WORKER,
        topic: worker.topic,
        reclaimDelay: worker.reclaimDelay,
        reclaimCount: worker.reclaimCount,
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
        self.throttle(message.throttle);
      }
    };
  }

  async throttle(delayInMillis: number) {
    this.streamSignaler.setThrottle(delayInMillis);
  }
}

export { WorkerService };
