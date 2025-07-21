import { KVSQL } from '../../kvsql';

import { HSetOptions } from './types';
import {
  createBasicOperations,
  _hset,
  _hget,
  _hdel,
  _hmget,
  _hincrbyfloat,
} from './basic';
import { createJsonbOperations } from './jsonb';
import { createUdataOperations } from './udata';
import { createScanOperations, _hscan, _scan } from './scan';
import { createExpireOperations, _expire } from './expire';
import { isJobsTable } from './utils';

export const hashModule = (context: KVSQL) => {
  const basicOps = createBasicOperations(context);
  const jsonbOps = createJsonbOperations(context);
  const udataOps = createUdataOperations(context);
  const scanOps = createScanOperations(context);
  const expireOps = createExpireOperations(context);

  return {
    // Basic operations
    ...basicOps,

    // Scan operations
    ...scanOps,

    // Expire operations
    ...expireOps,

    // Enhanced hset that handles JSONB operations
    async hset(
      key: string,
      fields: Record<string, string>,
      options?: HSetOptions,
      multi?: any,
    ): Promise<number | any> {
      const tableName = context.tableForKey(key, 'hash');
      const isJobsTableResult = isJobsTable(tableName);

      // Handle JSONB operations for jobs tables
      if (isJobsTableResult) {
        // Check for various JSONB operations
        if ('@context' in fields) {
          const { sql, params } = jsonbOps.handleContextSet(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:merge' in fields) {
          const { sql, params } = jsonbOps.handleContextMerge(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:delete' in fields) {
          const { sql, params } = jsonbOps.handleContextDelete(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:append' in fields) {
          const { sql, params } = jsonbOps.handleContextAppend(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:prepend' in fields) {
          const { sql, params } = jsonbOps.handleContextPrepend(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:remove' in fields) {
          const { sql, params } = jsonbOps.handleContextRemove(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:increment' in fields) {
          const { sql, params } = jsonbOps.handleContextIncrement(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:toggle' in fields) {
          const { sql, params } = jsonbOps.handleContextToggle(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:setIfNotExists' in fields) {
          const { sql, params } = jsonbOps.handleContextSetIfNotExists(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if (
          Object.keys(fields).some((k) => k.startsWith('@context:get:'))
        ) {
          const { sql, params } = jsonbOps.handleContextGetPath(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        } else if ('@context:get' in fields) {
          const { sql, params } = jsonbOps.handleContextGet(
            key,
            fields,
            options,
          );
          return executeJsonbOperation(sql, params, multi);
        }
      }

      // Handle udata operations for search fields
      if (isJobsTableResult) {
        if ('@udata:set' in fields) {
          const { sql, params } = udataOps.handleUdataSet(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:get' in fields) {
          const { sql, params } = udataOps.handleUdataGet(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:mget' in fields) {
          const { sql, params } = udataOps.handleUdataMget(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:delete' in fields) {
          const { sql, params } = udataOps.handleUdataDelete(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:increment' in fields) {
          const { sql, params } = udataOps.handleUdataIncrement(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:multiply' in fields) {
          const { sql, params } = udataOps.handleUdataMultiply(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        } else if ('@udata:all' in fields) {
          const { sql, params } = udataOps.handleUdataAll(key, fields, options);
          return executeJsonbOperation(sql, params, multi);
        }
      }

      // Fall back to basic hset for all other cases
      return basicOps.hset(key, fields, options, multi);
    },

    // Private methods for internal use by kvsql
    _hset: (
      key: string,
      fields: Record<string, string>,
      options?: HSetOptions,
    ) => {
      const tableName = context.tableForKey(key, 'hash');
      const isJobsTableResult = isJobsTable(tableName);

      // Handle JSONB operations for jobs tables
      if (isJobsTableResult) {
        if ('@context' in fields) {
          return jsonbOps.handleContextSet(key, fields, options);
        } else if ('@context:merge' in fields) {
          return jsonbOps.handleContextMerge(key, fields, options);
        } else if ('@context:delete' in fields) {
          return jsonbOps.handleContextDelete(key, fields, options);
        } else if ('@context:append' in fields) {
          return jsonbOps.handleContextAppend(key, fields, options);
        } else if ('@context:prepend' in fields) {
          return jsonbOps.handleContextPrepend(key, fields, options);
        } else if ('@context:remove' in fields) {
          return jsonbOps.handleContextRemove(key, fields, options);
        } else if ('@context:increment' in fields) {
          return jsonbOps.handleContextIncrement(key, fields, options);
        } else if ('@context:toggle' in fields) {
          return jsonbOps.handleContextToggle(key, fields, options);
        } else if ('@context:setIfNotExists' in fields) {
          return jsonbOps.handleContextSetIfNotExists(key, fields, options);
        } else if (
          Object.keys(fields).some((k) => k.startsWith('@context:get:'))
        ) {
          return jsonbOps.handleContextGetPath(key, fields, options);
        } else if ('@context:get' in fields) {
          return jsonbOps.handleContextGet(key, fields, options);
        }
      }

      // Handle udata operations for search fields
      if (isJobsTableResult) {
        if ('@udata:set' in fields) {
          return udataOps.handleUdataSet(key, fields, options);
        } else if ('@udata:get' in fields) {
          return udataOps.handleUdataGet(key, fields, options);
        } else if ('@udata:mget' in fields) {
          return udataOps.handleUdataMget(key, fields, options);
        } else if ('@udata:delete' in fields) {
          return udataOps.handleUdataDelete(key, fields, options);
        } else if ('@udata:increment' in fields) {
          return udataOps.handleUdataIncrement(key, fields, options);
        } else if ('@udata:multiply' in fields) {
          return udataOps.handleUdataMultiply(key, fields, options);
        } else if ('@udata:all' in fields) {
          return udataOps.handleUdataAll(key, fields, options);
        }
      }

      // Use the imported _hset function
      return _hset(context, key, fields, options);
    },

    _hget: (key: string, field: string) => {
      return _hget(context, key, field);
    },

    _hdel: (key: string, fields: string[]) => {
      return _hdel(context, key, fields);
    },

    _hmget: (key: string, fields: string[]) => {
      return _hmget(context, key, fields);
    },

    _hincrbyfloat: (key: string, field: string, increment: number) => {
      return _hincrbyfloat(context, key, field, increment);
    },

    _hscan: (key: string, cursor: string, count: number, pattern?: string) => {
      return _hscan(context, key, cursor, count, pattern);
    },

    _expire: (key: string, seconds: number) => {
      return _expire(context, key, seconds);
    },

    _scan: (cursor: number, count: number, pattern?: string) => {
      return _scan(context, cursor, count, pattern);
    },

    // Utility functions
    isJobsTable,
  };

  async function executeJsonbOperation(
    sql: string,
    params: any[],
    multi?: any,
  ): Promise<any> {
    if (multi) {
      (multi as any).addCommand(sql, params, 'any');
      return Promise.resolve(0);
    } else {
      try {
        const res = await context.pgClient.query(sql, params);

        if (res.rows[0]?.new_value !== undefined) {
          let returnValue;
          try {
            // Try to parse as JSON, fallback to string if it fails
            returnValue = JSON.parse(res.rows[0].new_value);
          } catch {
            returnValue = res.rows[0].new_value;
          }
          return returnValue;
        }

        return res.rowCount || res.rows[0]?.count || 0;
      } catch (err) {
        console.error('JSONB operation error', err, sql, params);
        return 0;
      }
    }
  }
};

// Export utility functions for other modules to use
export { isJobsTable, deriveType } from './utils';
export * from './types';
