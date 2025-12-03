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
    // Find replay ID if present (field with hyphen, not the @udata field)
    const replayId = Object.keys(fields).find(
      (k) => k.includes('-') && !k.startsWith('@'),
    );

    // Route based on @udata operation
    if ('@udata:set' in fields) {
      const udata = JSON.parse(fields['@udata:set']);
      const fieldsToSet: Record<string, string> = Array.isArray(udata)
        ? Object.fromEntries(
            Array.from({ length: udata.length / 2 }, (_, i) => [
              udata[i * 2],
              udata[i * 2 + 1],
            ]),
          )
        : udata;
      const result = await this.setFields(key, fieldsToSet);
      if (replayId) await this.searchClient.hset(key, { [replayId]: String(result) });
      return result;
    }

    if ('@udata:get' in fields) {
      const result = await this.getField(key, fields['@udata:get']);
      if (replayId) await this.searchClient.hset(key, { [replayId]: result });
      return result;
    }

    if ('@udata:mget' in fields) {
      const result = await this.getFields(key, JSON.parse(fields['@udata:mget']));
      if (replayId) await this.searchClient.hset(key, { [replayId]: result.join('|||') });
      return result;
    }

    if ('@udata:delete' in fields) {
      const result = await this.deleteFields(key, JSON.parse(fields['@udata:delete']));
      if (replayId) await this.searchClient.hset(key, { [replayId]: String(result) });
      return result;
    }

    if ('@udata:increment' in fields) {
      const { field, value } = JSON.parse(fields['@udata:increment']);
      const result = await this.incrementFieldByFloat(key, field, value);
      if (replayId) await this.searchClient.hset(key, { [replayId]: String(result) });
      return result;
    }

    if ('@udata:multiply' in fields) {
      const { field, value } = JSON.parse(fields['@udata:multiply']);
      const result = await this.incrementFieldByFloat(key, field, Math.log(value));
      if (replayId) await this.searchClient.hset(key, { [replayId]: String(result) });
      return result;
    }

    if ('@udata:all' in fields) {
      const all = await this.getAllFields(key);
      const result = Object.fromEntries(
        Object.entries(all).filter(([k]) => k.startsWith('_')),
      );
      if (replayId) await this.searchClient.hset(key, { [replayId]: JSON.stringify(result) });
      return result;
    }

    // Default: call setFields
    return await this.setFields(key, fields);
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

  // Entity methods - not implemented for Redis (postgres-specific JSONB operations)
  async findEntities(): Promise<any[]> {
    throw new Error(
      'Entity findEntities not supported in Redis - use PostgreSQL',
    );
  }

  async findEntityById(): Promise<any> {
    throw new Error(
      'Entity findEntityById not supported in Redis - use PostgreSQL',
    );
  }

  async findEntitiesByCondition(): Promise<any[]> {
    throw new Error(
      'Entity findEntitiesByCondition not supported in Redis - use PostgreSQL',
    );
  }

  async createEntityIndex(): Promise<void> {
    throw new Error(
      'Entity createEntityIndex not supported in Redis - use PostgreSQL',
    );
  }
}

export { IORedisSearchService };
