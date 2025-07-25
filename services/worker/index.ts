import { KeyType } from '../../modules/key';
import {
  XSleepFor,
  formatISODate,
  getSystemHealth,
  identifyProvider,
  sleepFor,
} from '../../modules/utils';
import { ConnectorService } from '../connector/factory';
import { ILogger } from '../logger';
import { Router } from '../router';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import { HotMeshConfig, HotMeshWorker } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  QuorumMessage,
  QuorumProfile,
  RollCallMessage,
  SubscriptionCallback,
} from '../../types/quorum';
import { StreamData, StreamRole, StreamDataResponse } from '../../types/stream';
import { HMSH_QUORUM_ROLLCALL_CYCLES } from '../../modules/enums';
import { StreamServiceFactory } from '../stream/factory';
import { SubServiceFactory } from '../sub/factory';
import { StoreServiceFactory } from '../store/factory';

class WorkerService {
  namespace: string;
  appId: string;
  guid: string;
  topic: string;
  config: HotMeshConfig;
  callback: (streamData: StreamData) => Promise<StreamDataResponse | void>;
  store: StoreService<ProviderClient, ProviderTransaction> | null;
  stream: StreamService<ProviderClient, ProviderTransaction> | null;
  subscribe: SubService<ProviderClient> | null;
  router: Router<typeof this.stream> | null;
  logger: ILogger;
  reporting = false;
  inited: string;
  rollCallInterval: NodeJS.Timeout;

  /**
   * @private
   */
  constructor() {}

  /**
   * @private
   */
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
        // Pass taskQueue from top-level config to worker for connection pooling
        if (config.taskQueue) {
          worker.taskQueue = config.taskQueue;
        }

        await ConnectorService.initClients(worker);

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
        await service.initSubChannel(
          service,
          worker.sub,
          worker.pub ?? worker.store,
        );
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
        await service.initStreamChannel(service, worker.stream, worker.store);
        service.router = await service.initRouter(worker, logger);

        const key = service.store.mintKey(KeyType.STREAMS, {
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

  /**
   * @private
   */
  verifyWorkerFields(worker: HotMeshWorker) {
    if (
      !identifyProvider(worker.store) ||
      !identifyProvider(worker.stream) ||
      !identifyProvider(worker.sub) ||
      !(worker.topic && worker.callback)
    ) {
      throw new Error(
        'worker must include `store`, `stream`, and `sub` fields along with a callback function and topic.',
      );
    }
  }

  /**
   * @private
   */
  async initStoreChannel(service: WorkerService, store: ProviderClient) {
    service.store = await StoreServiceFactory.init(
      store,
      service.namespace,
      service.appId,
      service.logger,
    );
  }

  /**
   * @private
   */
  async initSubChannel(
    service: WorkerService,
    sub: ProviderClient,
    store: ProviderClient,
  ) {
    service.subscribe = await SubServiceFactory.init(
      sub,
      store,
      service.namespace,
      service.appId,
      service.guid,
      service.logger,
    );
  }

  /**
   * @private
   */
  async initStreamChannel(
    service: WorkerService,
    stream: ProviderClient,
    store: ProviderClient,
  ) {
    service.stream = await StreamServiceFactory.init(
      stream,
      store,
      service.namespace,
      service.appId,
      service.logger,
    );
  }

  /**
   * @private
   */
  async initRouter(
    worker: HotMeshWorker,
    logger: ILogger,
  ): Promise<Router<StreamService<ProviderClient, ProviderTransaction>>> {
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
      logger,
    );
  }

  /**
   * @private
   */
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

  /**
   * @private
   */
  stop() {
    this.cancelRollCall();
  }

  /**
   * @private
   */
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
        stream: this.store.mintKey(KeyType.STREAMS, params),
        counts: this.router?.counts,
        timestamp: formatISODate(new Date()),
        inited: this.inited,
        throttle: this.router?.throttle,
        reclaimDelay: this.router?.reclaimDelay,
        reclaimCount: this.router?.reclaimCount,
        system: await getSystemHealth(),
        signature: signature ? this.callback.toString() : undefined,
      };
    }
    this.subscribe.publish(
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

  /**
   * @private
   */
  async throttle(delayInMillis: number) {
    this.router?.setThrottle(delayInMillis);
  }
}

export { WorkerService };
