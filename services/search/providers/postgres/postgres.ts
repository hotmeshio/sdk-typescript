import { SearchService } from '../../index';
import { ILogger } from '../../../logger';
import { PostgresClientType } from '../../../../types/postgres';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';
import { KVSQL } from '../../../store/providers/postgres/kvsql';

class PostgresSearchService extends SearchService<
  PostgresClientType & ProviderClient
> {
  pgClient: PostgresClientType;

  transact(): ProviderTransaction {
    return this.storeClient.transact();
  }

  constructor(
    searchClient: PostgresClientType & ProviderClient,
    storeClient?: PostgresClientType & ProviderClient,
  ) {
    super(searchClient, storeClient);
    this.pgClient = searchClient; //raw pg client (to send raw sql)
    this.searchClient = new KVSQL( //wrapped pg client (to send as redis commands)
      searchClient as unknown as PostgresClientType,
      this.namespace,
      this.appId,
    ) as unknown as PostgresClientType & ProviderClient;
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    //bind appId and namespace to searchClient once initialized
    // (it uses these values to construct keys for the store)
    this.searchClient.namespace = this.namespace = namespace;
    this.searchClient.appId = this.appId = appId;
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
  }

  async createSearchIndex(
    indexName: string,
    prefixes: string[],
    schema: string[],
  ): Promise<void> {
    //no-op
  }

  async listSearchIndexes(): Promise<string[]> {
    return [];
  }

  async setFields(
    key: string,
    fields: Record<string, string>,
  ): Promise<number> {
    try {
      const result = await this.searchClient.hset(key, fields);
      return Number(result);
    } catch (error) {
      this.logger.error(`postgres-search-set-fields-error`, { key, ...error });
      throw error;
    }
  }

  async getField(key: string, field: string): Promise<string> {
    try {
      return await this.searchClient.hget(key, field);
    } catch (error) {
      this.logger.error(`postgres-search-get-field-error`, {
        key,
        field,
        ...error,
      });
      throw error;
    }
  }

  async getFields(key: string, fields: string[]): Promise<string[]> {
    try {
      return await this.searchClient.hmget(key, [...fields]);
    } catch (error) {
      this.logger.error(`postgres-search-get-fields-error`, {
        key,
        fields,
        ...error,
      });
      throw error;
    }
  }

  async getAllFields(key: string): Promise<Record<string, string>> {
    try {
      return await this.searchClient.hgetall(key);
    } catch (error) {
      this.logger.error(`postgres-search-get-all-fields-error`, {
        key,
        ...error,
      });
      throw error;
    }
  }

  async deleteFields(key: string, fields: string[]): Promise<number> {
    try {
      const result = await this.searchClient.hdel(key, fields);
      return Number(result);
    } catch (error) {
      this.logger.error(`postgres-search-delete-fields-error`, {
        key,
        fields,
        ...error,
      });
      throw error;
    }
  }

  async incrementFieldByFloat(
    key: string,
    field: string,
    increment: number,
  ): Promise<number> {
    try {
      const result = await this.searchClient.hincrbyfloat(
        key,
        field,
        increment,
      );
      return Number(result);
    } catch (error) {
      this.logger.error(`postgres-increment-field-error`, {
        key,
        field,
        ...error,
      });
      throw error;
    }
  }

  async sendQuery(query: string): Promise<any> {
    try {
      //exec raw sql (local call, not meant for external use); return raw result
      return await this.pgClient.query(query);
    } catch (error) {
      this.logger.error(`postgres-send-query-error`, { query, ...error });
      throw error;
    }
  }

  /**
   * assume aggregation type query
   */
  async sendIndexedQuery(
    type: string,
    queryParams: any[] = [],
  ): Promise<any[]> {
    const [sql, ...params] = queryParams;
    try {
      const res = await this.pgClient.query(
        sql,
        params.length ? params : undefined,
      );
      return res.rows;
    } catch (error) {
      this.logger.error(`postgres-send-indexed-query-error`, {
        query: sql,
        ...error,
      });
      throw error;
    }
  }
}

export { PostgresSearchService };
