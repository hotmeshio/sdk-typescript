import { ILogger } from '../logger';
import { ProviderClient } from '../../types/provider';

abstract class SearchService<ClientProvider extends ProviderClient> {
  protected searchClient: ClientProvider;
  protected storeClient: ClientProvider;
  protected namespace: string;
  protected logger: ILogger;
  protected appId: string;

  constructor(searchClient: ClientProvider, storeClient?: ClientProvider) {
    this.searchClient = searchClient;
    this.storeClient = storeClient;
  }

  abstract init(
    namespace: string,
    appId: string,
    logger: ILogger,
  ): Promise<void>;
  abstract createSearchIndex(
    indexName: string,
    prefixes: string[],
    schema: string[],
  ): Promise<void>;
  abstract listSearchIndexes(): Promise<string[]>;
  abstract setFields(
    key: string,
    fields: Record<string, string>,
  ): Promise<number>;
  abstract getField(key: string, field: string): Promise<string>;
  abstract getFields(key: string, fields: string[]): Promise<string[]>;
  abstract getAllFields(key: string): Promise<Record<string, string>>;
  abstract deleteFields(key: string, fields: string[]): Promise<number>;
  abstract incrementFieldByFloat(
    key: string,
    field: string,
    increment: number,
  ): Promise<number>;
  abstract sendQuery(query: any): Promise<any>;
  abstract sendIndexedQuery(index: string, query: any[]): Promise<any>;
}

export { SearchService };
