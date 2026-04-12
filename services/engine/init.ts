/**
 * Engine bootstrap — channel setup, router creation, config validation.
 *
 * Called once during `EngineService.init()` to wire the engine to its
 * backing store, stream, search, and subscription transports.
 */

import { formatISODate, identifyProvider } from '../../modules/utils';
import { StreamConsumerRegistry } from '../stream/registry';
import { ExporterService } from '../exporter';
import { Router } from '../router';
import { SearchServiceFactory } from '../search/factory';
import { StoreServiceFactory } from '../store/factory';
import { StreamServiceFactory } from '../stream/factory';
import { SubServiceFactory } from '../sub/factory';
import { TaskService } from '../task';
import { ILogger } from '../logger';
import { SearchService } from '../search';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { SubService } from '../sub';
import {
  HotMeshConfig,
  HotMeshSettings,
} from '../../types/hotmesh';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../types/provider';
import { StreamDataResponse, StreamRole } from '../../types/stream';

interface InitContext {
  namespace: string;
  appId: string;
  guid: string;
  logger: ILogger;
  search: SearchService<ProviderClient> | null;
  store: StoreService<ProviderClient, ProviderTransaction> | null;
  stream: StreamService<ProviderClient, ProviderTransaction> | null;
  subscribe: SubService<ProviderClient> | null;
  router: Router<typeof this.stream> | null;
  taskService: TaskService | null;
  exporter: ExporterService | null;
  inited: string;
  processStreamMessage(streamData: StreamDataResponse): Promise<void>;
}

export function verifyEngineFields(config: HotMeshConfig) {
  if (
    !identifyProvider(config.engine.store) ||
    !identifyProvider(config.engine.stream) ||
    !identifyProvider(config.engine.sub)
  ) {
    throw new Error(
      'engine must include `store`, `stream`, and `sub` fields.',
    );
  }
}

export async function initSearchChannel(
  instance: InitContext,
  search: ProviderClient,
  store?: ProviderClient,
) {
  instance.search = await SearchServiceFactory.init(
    search,
    store,
    instance.namespace,
    instance.appId,
    instance.logger,
  );
}

export async function initStoreChannel(
  instance: InitContext,
  store: ProviderClient,
) {
  instance.store = await StoreServiceFactory.init(
    store,
    instance.namespace,
    instance.appId,
    instance.logger,
  );
}

export async function initSubChannel(
  instance: InitContext,
  sub: ProviderClient,
  store: ProviderClient,
) {
  instance.subscribe = await SubServiceFactory.init(
    sub,
    store,
    instance.namespace,
    instance.appId,
    instance.guid,
    instance.logger,
  );
}

export async function initStreamChannel(
  instance: InitContext,
  stream: ProviderClient,
  store: ProviderClient,
) {
  instance.stream = await StreamServiceFactory.init(
    stream,
    store,
    instance.namespace,
    instance.appId,
    instance.logger,
  );
}

export async function initRouter(
  instance: InitContext,
  config: HotMeshConfig,
): Promise<Router<StreamService<ProviderClient, ProviderTransaction>>> {
  const throttle = await instance.store.getThrottleRate(':');
  return new Router(
    {
      namespace: instance.namespace,
      appId: instance.appId,
      guid: instance.guid,
      role: StreamRole.ENGINE,
      reclaimDelay: config.engine.reclaimDelay,
      reclaimCount: config.engine.reclaimCount,
      throttle,
      readonly: config.engine.readonly,
    },
    instance.stream,
    instance.logger,
  );
}

export async function registerStreamConsumer(
  instance: InitContext,
  config: HotMeshConfig,
) {
  await StreamConsumerRegistry.registerEngine(
    instance.namespace,
    instance.appId,
    instance.guid,
    instance.processStreamMessage.bind(instance),
    instance.stream,
    instance.store,
    instance.logger,
    {
      reclaimDelay: config.engine.reclaimDelay,
      reclaimCount: config.engine.reclaimCount,
    },
  );
}

export function initServices(instance: InitContext) {
  instance.taskService = new TaskService(instance.store, instance.logger);
  instance.exporter = new ExporterService(
    instance.appId,
    instance.store,
    instance.logger,
  );
  instance.inited = formatISODate(new Date());
}

export async function getSettings(
  instance: InitContext,
): Promise<HotMeshSettings> {
  return await instance.store.getSettings();
}
