import { SearchService } from '../../index';
import { ILogger } from '../../../logger';
import { PostgresClientType } from '../../../../types/postgres';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';
import { KVSQL } from '../../../store/providers/postgres/kvsql';
import { KeyService, KeyType, HMNS } from '../../../../modules/key';

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

  async updateContext(
    key: string,
    fields: Record<string, string>,
  ): Promise<any> {
    try {
      const result = await this.searchClient.hset(key, fields);
      return isNaN(result) ? result : Number(result);
    } catch (error) {
      this.logger.error(`postgres-search-set-fields-error`, { key, error });
      throw error;
    }
  }

  async setFields(
    key: string,
    fields: Record<string, string>,
  ): Promise<any> {
    try {
      const result = await this.searchClient.hset(key, fields);
      const isGetOperation = '@context:get' in fields;
      if (isGetOperation) {
        return result;
      }
      return isNaN(result) ? result : Number(result);
    } catch (error) {
      this.logger.error(`postgres-search-set-fields-error`, { key, error });
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
        error,
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
        error,
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
        error,
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
        error,
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
        error,
      });
      throw error;
    }
  }

  async sendQuery(query: string): Promise<any> {
    try {
      //exec raw sql (local call, not meant for external use); return raw result
      return await this.pgClient.query(query);
    } catch (error) {
      this.logger.error(`postgres-send-query-error`, { query, error });
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
        error,
      });
      throw error;
    }
  }

  // Entity querying methods for JSONB/SQL operations

  async findEntities(
    entity: string,
    conditions: Record<string, any>,
    options?: { limit?: number; offset?: number },
  ): Promise<any[]> {
    try {
      const schemaName = this.searchClient.safeName(this.appId);
      const tableName = `${schemaName}.${this.searchClient.safeName('jobs')}`;
      
      // Build WHERE conditions from the conditions object
      const whereConditions: string[] = [`entity = $1`];
      const params: any[] = [entity];
      let paramIndex = 2;

      for (const [key, value] of Object.entries(conditions)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          // Handle MongoDB-style operators like { $gte: 18 }
          for (const [op, opValue] of Object.entries(value)) {
            const sqlOp = this.mongoToSqlOperator(op);
            whereConditions.push(`(context->>'${key}')::${this.inferType(opValue)} ${sqlOp} $${paramIndex}`);
            params.push(opValue);
            paramIndex++;
          }
        } else {
          // Simple equality
          whereConditions.push(`context->>'${key}' = $${paramIndex}`);
          params.push(String(value));
          paramIndex++;
        }
      }

      let sql = `
        SELECT key, context, status
        FROM ${tableName}
        WHERE ${whereConditions.join(' AND ')}
        ORDER BY created_at DESC
      `;

      if (options?.limit) {
        sql += ` LIMIT $${paramIndex}`;
        params.push(options.limit);
        paramIndex++;
      }

      if (options?.offset) {
        sql += ` OFFSET $${paramIndex}`;
        params.push(options.offset);
      }

      const result = await this.pgClient.query(sql, params);
      return result.rows.map(row => ({
        key: row.key,
        context: typeof row.context === 'string' ? JSON.parse(row.context || '{}') : (row.context || {}),
        status: row.status,
      }));
    } catch (error) {
      this.logger.error(`postgres-find-entities-error`, { entity, conditions, error });
      throw error;
    }
  }

  async findEntityById(entity: string, id: string): Promise<any> {
    try {
      const schemaName = this.searchClient.safeName(this.appId);
      const tableName = `${schemaName}.${this.searchClient.safeName('jobs')}`;
      
      // Use KeyService to mint the job state key
      const fullKey = KeyService.mintKey(HMNS, KeyType.JOB_STATE, {
        appId: this.appId,
        jobId: id
      });
      
      const sql = `
        SELECT key, context, status, entity
        FROM ${tableName}
        WHERE entity = $1 AND key = $2
        LIMIT 1
      `;

      const result = await this.pgClient.query(sql, [entity, fullKey]);
      
      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return {
        key: row.key,
        context: typeof row.context === 'string' ? JSON.parse(row.context || '{}') : (row.context || {}),
        status: row.status,
      };
    } catch (error) {
      this.logger.error(`postgres-find-entity-by-id-error`, { entity, id, error });
      throw error;
    }
  }

  async findEntitiesByCondition(
    entity: string,
    field: string,
    value: any,
    operator: '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'IN' = '=',
    options?: { limit?: number; offset?: number },
  ): Promise<any[]> {
    try {
      const schemaName = this.searchClient.safeName(this.appId);
      const tableName = `${schemaName}.${this.searchClient.safeName('jobs')}`;
      
      const params: any[] = [entity];
      let whereCondition: string;
      let paramIndex = 2;

      if (operator === 'IN') {
        // Handle IN operator with arrays
        const placeholders = Array.isArray(value) 
          ? value.map(() => `$${paramIndex++}`).join(',')
          : `$${paramIndex++}`;
        whereCondition = `context->>'${field}' IN (${placeholders})`;
        if (Array.isArray(value)) {
          params.push(...value);
        } else {
          params.push(value);
        }
      } else if (operator === 'LIKE') {
        whereCondition = `context->>'${field}' LIKE $${paramIndex}`;
        params.push(value);
        paramIndex++;
      } else {
        // Handle numeric/comparison operators
        const valueType = this.inferType(value);
        whereCondition = `(context->>'${field}')::${valueType} ${operator} $${paramIndex}`;
        params.push(value);
        paramIndex++;
      }

      let sql = `
        SELECT key, context, status
        FROM ${tableName}
        WHERE entity = $1 AND ${whereCondition}
        ORDER BY created_at DESC
      `;

      if (options?.limit) {
        sql += ` LIMIT $${paramIndex}`;
        params.push(options.limit);
        paramIndex++;
      }

      if (options?.offset) {
        sql += ` OFFSET $${paramIndex}`;
        params.push(options.offset);
      }

      const result = await this.pgClient.query(sql, params);
      return result.rows.map(row => ({
        key: row.key,
        context: typeof row.context === 'string' ? JSON.parse(row.context || '{}') : (row.context || {}),
        status: row.status,
      }));
    } catch (error) {
      this.logger.error(`postgres-find-entities-by-condition-error`, { 
        entity, field, value, operator, error 
      });
      throw error;
    }
  }

  async createEntityIndex(
    entity: string,
    field: string,
    indexType: 'btree' | 'gin' | 'gist' = 'btree',
  ): Promise<void> {
    try {
      const schemaName = this.searchClient.safeName(this.appId);
      const tableName = `${schemaName}.${this.searchClient.safeName('jobs')}`;
      const indexName = `idx_${this.appId}_${entity}_${field}`.replace(/[^a-zA-Z0-9_]/g, '_');

      let sql: string;
      if (indexType === 'gin') {
        // GIN index for JSONB operations
        sql = `
          CREATE INDEX IF NOT EXISTS ${indexName}
          ON ${tableName} USING gin (context jsonb_path_ops)
          WHERE entity = '${entity}'
        `;
      } else if (indexType === 'gist') {
        // GiST index for specific field
        sql = `
          CREATE EXTENSION IF NOT EXISTS pg_trgm;
          CREATE INDEX IF NOT EXISTS ${indexName}
          ON ${tableName} USING gist ((context->>'${field}') gist_trgm_ops)
          WHERE entity = '${entity}'
        `;
      } else {
        // B-tree index for specific field
        sql = `
          CREATE INDEX IF NOT EXISTS ${indexName}
          ON ${tableName} USING btree ((context->>'${field}'))
          WHERE entity = '${entity}'
        `;
      }

      await this.pgClient.query(sql);
      this.logger.info(`postgres-entity-index-created`, { entity, field, indexType, indexName });
    } catch (error) {
      this.logger.error(`postgres-create-entity-index-error`, { 
        entity, field, indexType, error 
      });
      throw error;
    }
  }

  // Helper methods for entity operations

  private mongoToSqlOperator(mongoOp: string): string {
    const mapping: Record<string, string> = {
      '$eq': '=',
      '$ne': '!=',
      '$gt': '>',
      '$gte': '>=',
      '$lt': '<',
      '$lte': '<=',
      '$in': 'IN',
    };
    return mapping[mongoOp] || '=';
  }

  private inferType(value: any): string {
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'integer' : 'numeric';
    }
    if (typeof value === 'boolean') {
      return 'boolean';
    }
    return 'text';
  }
}

export { PostgresSearchService };
