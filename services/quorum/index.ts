import {
  HMSH_ACTIVATION_MAX_RETRY,
  HMSH_QUORUM_DELAY_MS,
  HMSH_QUORUM_ROLLCALL_CYCLES,
} from '../../modules/enums';
import {
  XSleepFor,
  formatISODate,
  getSystemHealth,
  identifyProvider,
  sleepFor,
} from '../../modules/utils';
import { CompilerService } from '../compiler';
import { EngineService } from '../engine';
import { ILogger } from '../logger';
import { StoreService } from '../store';
import { SubService } from '../sub';
import { CacheMode } from '../../types/cache';
import { HotMeshConfig, KeyType } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  QuorumMessage,
  QuorumMessageCallback,
  QuorumProfile,
  RollCallMessage,
  SubscriptionCallback,
} from '../../types/quorum';
import { SubServiceFactory } from '../sub/factory';
import { StoreServiceFactory } from '../store/factory';

class QuorumService {
  namespace: string;
  appId: string;
  guid: string;
  engine: EngineService;
  profiles: QuorumProfile[] = [];
  store: StoreService<ProviderClient, ProviderTransaction> | null;
  subscribe: SubService<ProviderClient> | null;
  logger: ILogger;
  cacheMode: CacheMode = 'cache';
  untilVersion: string | null = null;
  quorum: number | null = null;
  callbacks: QuorumMessageCallback[] = [];
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
    engine: EngineService,
    logger: ILogger,
  ): Promise<QuorumService> {
    if (config.engine) {
      const instance = new QuorumService();
      instance.verifyQuorumFields(config);
      instance.namespace = namespace;
      instance.appId = appId;
      instance.guid = guid;
      instance.logger = logger;
      instance.engine = engine;

      await instance.initStoreChannel(config.engine.store);
      await instance.initSubChannel(
        config.engine.sub,
        config.engine.pub ?? config.engine.store,
      );
      //general quorum subscription
      await instance.subscribe.subscribe(
        KeyType.QUORUM,
        instance.subscriptionHandler(),
        appId,
      );
      //app-specific quorum subscription (used for pubsub one-time request/response)
      await instance.subscribe.subscribe(
        KeyType.QUORUM,
        instance.subscriptionHandler(),
        appId,
        instance.guid,
      );

      instance.engine.processWebHooks();
      instance.engine.processTimeHooks();
      return instance;
    }
  }

  /**
   * @private
   */
  verifyQuorumFields(config: HotMeshConfig) {
    if (
      !identifyProvider(config.engine.store) ||
      !identifyProvider(config.engine.sub)
    ) {
      throw new Error('quorum config must include `store` and `sub` fields.');
    }
  }

  /**
   * @private
   */
  async initStoreChannel(store: ProviderClient) {
    this.store = await StoreServiceFactory.init(
      store,
      this.namespace,
      this.appId,
      this.logger,
    );
  }

  /**
   * @private
   */
  async initSubChannel(sub: ProviderClient, store: ProviderClient) {
    this.subscribe = await SubServiceFactory.init(
      sub,
      store,
      this.namespace,
      this.appId,
      this.guid,
      this.logger,
    );
  }

  /**
   * @private
   */
  subscriptionHandler(): SubscriptionCallback {
    const self = this;
    return async (topic: string, message: QuorumMessage) => {
      self.logger.debug('quorum-event-received', { topic, type: message.type });
      if (message.type === 'activate') {
        self.engine.setCacheMode(message.cache_mode, message.until_version);
      } else if (message.type === 'ping') {
        self.sayPong(
          self.appId,
          self.guid,
          message.originator,
          message.details,
        );
      } else if (message.type === 'pong' && self.guid === message.originator) {
        self.quorum = self.quorum + 1;
        if (message.profile) {
          self.profiles.push(message.profile);
        }
      } else if (message.type === 'throttle') {
        self.engine.throttle(message.throttle);
      } else if (message.type === 'work') {
        self.engine.processWebHooks();
      } else if (message.type === 'job') {
        self.engine.routeToSubscribers(message.topic, message.job);
      } else if (message.type === 'cron') {
        self.engine.processTimeHooks();
      } else if (message.type === 'rollcall') {
        self.doRollCall(message);
      }
      //if there are any callbacks, call them
      if (self.callbacks.length > 0) {
        self.callbacks.forEach((cb) => cb(topic, message));
      }
    };
  }

  /**
   * @private
   */
  async sayPong(
    appId: string,
    guid: string,
    originator: string,
    details = false,
  ) {
    let profile: QuorumProfile;
    if (details) {
      const stream = this.engine.store.mintKey(KeyType.STREAMS, {
        appId: this.appId,
      });
      profile = {
        engine_id: this.guid,
        namespace: this.namespace,
        app_id: this.appId,
        stream,
        counts: this.engine.router.counts,
        timestamp: formatISODate(new Date()),
        inited: this.engine.inited,
        throttle: this.engine.router.throttle,
        reclaimDelay: this.engine.router.reclaimDelay,
        reclaimCount: this.engine.router.reclaimCount,
        system: await getSystemHealth(),
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
   * A quorum-wide command to request a quorum count.
   * @private
   */
  async requestQuorum(
    delay = HMSH_QUORUM_DELAY_MS,
    details = false,
  ): Promise<number> {
    const quorum = this.quorum;
    this.quorum = 0;
    this.profiles.length = 0;
    await this.subscribe.publish(
      KeyType.QUORUM,
      {
        type: 'ping',
        originator: this.guid,
        details,
      },
      this.appId,
    );
    await sleepFor(delay);
    return quorum;
  }

  /**
   * @private
   */
  async doRollCall(message: RollCallMessage) {
    let iteration = 0;
    const max = !isNaN(message.max) ? message.max : HMSH_QUORUM_ROLLCALL_CYCLES;
    if (this.rollCallInterval) clearTimeout(this.rollCallInterval);
    const base = message.interval / 2;
    const amount = base + Math.ceil(Math.random() * base);
    do {
      await sleepFor(Math.ceil(Math.random() * 1000));
      await this.sayPong(this.appId, this.guid, null, true);
      if (!message.interval) return;
      const { promise, timerId } = XSleepFor(amount * 1000);
      this.rollCallInterval = timerId;
      await promise;
    } while (this.rollCallInterval && iteration++ < max - 1);
  }

  /**
   * @private
   */
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
  async pub(quorumMessage: QuorumMessage) {
    return await this.subscribe.publish(
      KeyType.QUORUM,
      quorumMessage,
      this.appId,
      quorumMessage.topic || quorumMessage.guid,
    );
  }
  /**
   * @private
   */
  async sub(callback: QuorumMessageCallback): Promise<void> {
    this.callbacks.push(callback);
  }
  /**
   * @private
   */
  async unsub(callback: QuorumMessageCallback): Promise<void> {
    this.callbacks = this.callbacks.filter((cb) => cb !== callback);
  }
  /**
   * @private
   */
  async rollCall(delay = HMSH_QUORUM_DELAY_MS): Promise<QuorumProfile[]> {
    await this.requestQuorum(delay, true);
    const stream_depths = await this.engine.stream.getStreamDepths(
      this.profiles as { stream: string }[],
    );

    this.profiles.forEach(async (profile: QuorumProfile, index: number) => {
      //if nothing in the table, the depth will be 0
      //todo: separate table for every worker stream?
      profile.stream_depth = stream_depths?.[index]?.depth ?? 0;
    });
    return this.profiles;
  }
  /**
   * @private
   */
  async activate(
    version: string,
    delay = HMSH_QUORUM_DELAY_MS,
    count = 0,
  ): Promise<boolean> {
    version = version.toString();
    const canActivate = await this.store.reserveScoutRole(
      'activate',
      Math.ceil(delay * 6 / 1000) + 1,
    );
    if (!canActivate) {
      //another engine is already activating the app version
      this.logger.debug('quorum-activation-awaiting', { version });
      await sleepFor(delay * 6);
      const app = await this.store.getApp(this.appId, true);
      return app?.active == true && app?.version === version;
    }
    const config = await this.engine.getVID();
    await this.requestQuorum(delay);
    const q1 = await this.requestQuorum(delay);
    const q2 = await this.requestQuorum(delay);
    const q3 = await this.requestQuorum(delay);
    if (q1 && q1 === q2 && q2 === q3) {
      this.logger.info('quorum-rollcall-succeeded', { q1, q2, q3 });
      this.subscribe.publish(
        KeyType.QUORUM,
        { type: 'activate', cache_mode: 'nocache', until_version: version },
        this.appId,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      await this.store.releaseScoutRole('activate');
      //confirm we received the activation message
      if (this.engine.untilVersion === version) {
        this.logger.info('quorum-activation-succeeded', { version });
        const { id } = config;
        const compiler = new CompilerService(
          this.store,
          this.engine.stream,
          this.logger,
        );
        return await compiler.activate(id, version);
      } else {
        this.logger.error('quorum-activation-error', { version });
        throw new Error(
          `UntilVersion Not Received. Version ${version} not activated`,
        );
      }
    } else {
      this.logger.warn('quorum-rollcall-error', { q1, q2, q3, count });
      this.store.releaseScoutRole('activate');
      if (count < HMSH_ACTIVATION_MAX_RETRY) {
        //increase the delay (give the quorum time to respond) and try again
        return await this.activate(version, delay * 2, count + 1);
      }
      throw new Error(`Quorum not reached. Version ${version} not activated.`);
    }
  }
}

export { QuorumService };
