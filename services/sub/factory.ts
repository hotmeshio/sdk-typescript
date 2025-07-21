import { identifyProvider } from '../../modules/utils';
import { ILogger } from '../logger';
import { NatsClientType } from '../../types/nats';
import { PostgresClientType } from '../../types/postgres';
import { ProviderClient } from '../../types/provider';
import { PostgresSubService } from './providers/postgres/postgres';
import { NatsSubService } from './providers/nats/nats';
import { SubService } from './index';

class SubServiceFactory {
  static async init(
    providerSubClient: ProviderClient,
    providerPubClient: ProviderClient,
    namespace: string,
    appId: string,
    engineId: string,
    logger: ILogger,
  ): Promise<SubService<ProviderClient>> {
    let service: SubService<ProviderClient>;
    const providerType = identifyProvider(providerSubClient);

    if (providerType === 'nats') {
      service = new NatsSubService(
        providerSubClient as NatsClientType & ProviderClient,
        providerPubClient as NatsClientType & ProviderClient,
      );
    } else if (providerType === 'postgres') {
      service = new PostgresSubService(
        providerSubClient as PostgresClientType & ProviderClient,
        providerPubClient as PostgresClientType & ProviderClient,
      );
    }
    await service.init(namespace, appId, engineId, logger);
    return service;
  }
}

export { SubServiceFactory };
