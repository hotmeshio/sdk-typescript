import { SearchService } from '../../index';
import { ILogger } from '../../../logger';
import { IORedisClientType } from '../../../../types/redis';

class IORedisSearchService extends SearchService<IORedisClientType> {
  constructor(
    searchClient: IORedisClientType,
    storeClient?: IORedisClientType,
  ) {
    super(searchClient, storeClient);
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.appId = appId;
    this.logger = logger;
  }

  async createSearchIndex(
    indexName: string,
    prefixes: string[],
    schema: string[],
  ): Promise<void> {
    try {
      await this.searchClient.call(
        'FT.CREATE',
        indexName,
        'ON',
        'HASH',
        'PREFIX',
        prefixes.length.toString(),
        ...prefixes,
        'SCHEMA',
        ...schema,
      );
    } catch (error) {
      this.logger.info('Error creating search index', { error });
      throw error;
    }
  }

  async listSearchIndexes(): Promise<string[]> {
    try {
      const indexes = await this.searchClient.call('FT._LIST');
      return indexes as string[];
    } catch (error) {
      this.logger.info('Error listing search indexes', { error });
      throw error;
    }
  }

  async updateContext(
    key: string,
    fields: Record<string, string>,
  ): Promise<any> {
    //no-op;
    throw new Error('Not implemented');
  }

  async setFields(
    key: string,
    fields: Record<string, string>,
  ): Promise<number> {
    try {
      const result = await this.searchClient.hset(key, fields);
      return Number(result);
    } catch (error) {
      this.logger.error(`Error setting fields for key: ${key}`, { error });
      throw error;
    }
  }

  async getField(key: string, field: string): Promise<string> {
    try {
      return await this.searchClient.hget(key, field);
    } catch (error) {
      this.logger.error(`Error getting field ${field} for key: ${key}`, {
        error,
      });
      throw error;
    }
  }

  async getFields(key: string, fields: string[]): Promise<string[]> {
    try {
      return await this.searchClient.hmget(key, [...fields]);
    } catch (error) {
      this.logger.error(`Error getting fields for key: ${key}`, { error });
      throw error;
    }
  }

  async getAllFields(key: string): Promise<Record<string, string>> {
    try {
      return await this.searchClient.hgetall(key);
    } catch (error) {
      this.logger.error(`Error getting fields for key: ${key}`, { error });
      throw error;
    }
  }

  async deleteFields(key: string, fields: string[]): Promise<number> {
    try {
      const result = await this.searchClient.hdel(key, ...fields);
      return Number(result);
    } catch (error) {
      this.logger.error(`Error deleting fields for key: ${key}`, { error });
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
      this.logger.error(`Error incrementing field ${field} for key: ${key}`, {
        error,
      });
      throw error;
    }
  }

  async sendQuery(...query: [string, ...string[]]): Promise<any> {
    try {
      return await this.searchClient.call(...query);
    } catch (error) {
      this.logger.error('Error executing query', { error });
      throw error;
    }
  }

  async sendIndexedQuery(index: string, query: string[]): Promise<string[]> {
    try {
      if (query[0]?.startsWith('FT.')) {
        const [cmd, ...rest] = query;
        return (await this.searchClient.call(cmd, ...rest)) as string[];
      }
      return (await this.searchClient.call(
        'FT.SEARCH',
        index,
        ...query,
      )) as string[];
    } catch (error) {
      this.logger.error('Error executing query', { error });
      throw error;
    }
  }
}

export { IORedisSearchService };
