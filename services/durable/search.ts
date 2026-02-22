import { HotMesh } from '../hotmesh';
import { SearchService } from '../search';
import { KeyService, KeyType } from '../../modules/key';
import { WorkflowSearchOptions } from '../../types/durable';
import { asyncLocalStorage } from '../../modules/storage';

/**
 * The Search module provides methods for reading and
 * writing record data to a workflow. The instance
 * methods exposed by this class are available
 * for use from within a running workflow. The following example
 * uses search to set a `name` field and increment a
 * `counter` field. The workflow returns the incremented value.
 * 
 * @example
 * ```typescript
 * //searchWorkflow.ts
 * import { workflow } from '@hotmeshio/hotmesh';

 * export async function searchExample(name: string): Promise<{counter: number}> {
 *   const search = await workflow.search();
 *   await search.set({ name });
 *   const newCounterValue = await search.incr('counter', 1);
 *   return { counter: newCounterValue };
 * }
 * ```
 */
export class Search {
  /**
   * @private
   */
  jobId: string;
  /**
   * @private
   */
  searchSessionId: string;
  /**
   * @private
   */
  searchSessionIndex = 0;
  /**
   * @private
   */
  hotMeshClient: HotMesh;
  /**
   * @private
   */
  search: SearchService<any> | null;

  /**
   * @private
   */
  constructor(
    workflowId: string,
    hotMeshClient: HotMesh,
    searchSessionId: string,
  ) {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: workflowId,
    };
    this.jobId = KeyService.mintKey(
      hotMeshClient.namespace,
      KeyType.JOB_STATE,
      keyParams,
    );
    this.searchSessionId = searchSessionId;
    this.hotMeshClient = hotMeshClient;
    this.search = hotMeshClient.engine.search;
  }

  /**
   * Prefixes the key with an underscore to keep separate from the
   * activity and job history (and searchable via HKEYS)
   *
   * @private
   */
  safeKey(key: string): string {
    if (key.startsWith('"')) {
      return key.slice(1, -1);
    }
    return `_${key}`;
  }

  /**
   * For those deployments with search configured, this method
   * will configure the search index with the provided schema.
   *
   * @private
   * @example
   * const search = {
   *  index: 'my_search_index',
   *  prefix: ['my_workflow_prefix'],
   *  schema: {
   *   field1: { type: 'TEXT', sortable: true },
   *   field2: { type: 'NUMERIC', sortable: true }
   *  }
   * }
   * await Search.configureSearchIndex(hotMeshClient, search);
   */
  static async configureSearchIndex(
    hotMeshClient: HotMesh,
    search?: WorkflowSearchOptions,
  ): Promise<void> {
    if (search?.schema) {
      const searchService = hotMeshClient.engine.search;
      const schema: string[] = [];
      for (const [key, value] of Object.entries(search.schema)) {
        if (value.indexed !== false) {
          schema.push(
            value.fieldName ? `${value.fieldName.toString()}` : `_${key}`,
          );
          schema.push(value.type ? value.type : 'TEXT');
          if (value.noindex) {
            schema.push('NOINDEX');
          } else {
            if (value.nostem && value.type === 'TEXT') {
              schema.push('NOSTEM');
            }
            if (value.sortable) {
              schema.push('SORTABLE');
            }
          }
        }
      }
      try {
        const keyParams = {
          appId: hotMeshClient.appId,
          jobId: '',
        };
        const hotMeshPrefix = KeyService.mintKey(
          hotMeshClient.namespace,
          KeyType.JOB_STATE,
          keyParams,
        );
        const prefixes = search.prefix.map(
          (prefix) => `${hotMeshPrefix}${prefix}`,
        );
        await searchService.createSearchIndex(
          `${search.index}`,
          prefixes,
          schema,
        );
      } catch (error) {
        hotMeshClient.engine.logger.info('durable-client-search-err', {
          error,
        });
      }
    }
  }

  /**
   * Returns all user-defined attributes (udata) for a workflow.
   * These are fields that start with underscore (_) and have type='udata'.
   *
   * @example
   * ```typescript
   * const allUserData = await Search.findAllUserData('job123', hotMeshClient);
   * // Returns: { _status: "active", _counter: "42", _name: "test" }
   * ```
   */
  static async findAllUserData(
    jobId: string, 
    hotMeshClient: HotMesh
  ): Promise<Record<string, string>> {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: jobId,
    };
    const key = KeyService.mintKey(
      hotMeshClient.namespace,
      KeyType.JOB_STATE,
      keyParams,
    );
    const search = hotMeshClient.engine.search;
    const rawResult = await search.updateContext(key, {
      '@udata:all': ''
    }) as Record<string, string>;

    // Transform the result:
    // 1. Remove underscore prefix from keys
    // 2. Handle special fields (like exponential values)
    const result: Record<string, string> = {};
    for (const [key, value] of Object.entries(rawResult)) {
      // Remove underscore prefix
      const cleanKey = key.startsWith('_') ? key.slice(1) : key;
      
      // Special handling for fields that use logarithmic storage
      if (cleanKey === 'multer') {
        // Convert from log value back to actual value
        const expValue = Math.exp(Number(value));
        // Round to nearest integer since log multiplication doesn't need decimal precision
        result[cleanKey] = Math.round(expValue).toString();
      } else {
        result[cleanKey] = value;
      }
    }

    return result;
  }

  /**
   * Returns an array of search indexes ids
   *
   * @example
   * const searchIndexes = await Search.listSearchIndexes(hotMeshClient);
   */
  static async listSearchIndexes(hotMeshClient: HotMesh): Promise<string[]> {
    try {
      const searchService = hotMeshClient.engine.search;
      return await searchService.listSearchIndexes();
    } catch (error) {
      hotMeshClient.engine.logger.info('durable-client-search-list-err', {
        error,
      });
      return [];
    }
  }

  /**
   * increments the index to return a unique search session guid when
   * calling any method that produces side effects (changes the value)
   * @private
   */
  getSearchSessionGuid(): string {
    return `${this.searchSessionId}-${this.searchSessionIndex++}-`;
  }

  /**
   * Sets the fields listed in args. Returns the
   * count of new fields that were set (does not
   * count fields that were updated)
   *
   * @example
   * const search = await workflow.search();
   * const count = await search.set({ field1: 'value1', field2: 'value2' });
   */
  async set(...args: any[]): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }

    // Prepare fields to set with udata format
    let udataFields: Record<string, string> | string[];
    if (typeof args[0] === 'object') {
      // Object format: { field1: 'value1', field2: 'value2' }
      udataFields = {};
      for (const [key, value] of Object.entries(args[0])) {
        udataFields[this.safeKey(key)] = value.toString();
      }
    } else {
      // Array format: ['field1', 'value1', 'field2', 'value2']
      udataFields = [];
      for (let i = 0; i < args.length; i += 2) {
        const keyName = args[i];
        const key = this.safeKey(keyName);
        const value = args[i + 1].toString();
        udataFields.push(key, value);
      }
    }

    // Use single transactional call to update fields and store replay value
    const result = await this.search.updateContext(this.jobId, {
      '@udata:set': JSON.stringify(udataFields),
      [ssGuid]: '', // Pass replay ID to hash module for transactional replay storage
    });

    return result as number;
  }

  /**
   * Returns the value of the record data field, given a field id
   *
   * @example
   * const search = await workflow.search();
   * const value = await search.get('field1');
   */
  async get(id: string): Promise<string> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      // Replay cache stores the field value
      return replay[ssGuid];
    }

    try {
      // Use server-side udata get operation with replay storage
      const result = await this.search.updateContext(this.jobId, {
        '@udata:get': this.safeKey(id),
        [ssGuid]: '', // Pass replay ID to hash module
      });

      return result || '';
    } catch (error) {
      this.hotMeshClient.logger.error('durable-search-get-error', {
        error,
      });
      return '';
    }
  }

  /**
   * Returns the values of all specified fields in the HASH stored at key.
   */
  async mget(...args: string[]): Promise<string[]> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      // Replay cache stores the field values array
      const replayValue = replay[ssGuid];
      return typeof replayValue === 'string' ? replayValue.split('|||') : replayValue;
    }

    try {
      const safeArgs = args.map(arg => this.safeKey(arg));
      
      // Use server-side udata mget operation with replay storage
      const result = await this.search.updateContext(this.jobId, {
        '@udata:mget': JSON.stringify(safeArgs),
        [ssGuid]: '', // Pass replay ID to hash module
      });

      return result || [];
    } catch (error) {
      this.hotMeshClient.logger.error('durable-search-mget-error', {
        error,
      });
      return [];
    }
  }

  /**
   * Deletes the fields provided as args. Returns the
   * count of fields that were deleted.
   *
   * @example
   * const search = await workflow.search();
   * const count = await search.del('field1', 'field2', 'field3');
   */
  async del(...args: string[]): Promise<number | void> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }

    const safeArgs = args.map(arg => this.safeKey(arg));

    // Use server-side udata delete operation with replay storage
    const result = await this.search.updateContext(this.jobId, {
      '@udata:delete': JSON.stringify(safeArgs),
      [ssGuid]: '', // Pass replay ID to hash module for transactional replay storage
    });

    return Number(result || 0);
  }

  /**
   * Increments the value of a float field by the given amount. Returns the
   * new value of the field after the increment. Pass a negative
   * number to decrement the value.
   *
   * @example
   * const search = await workflow.search();
   * const count = await search.incr('field1', 1.5);
   */
  async incr(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }

    // Use server-side udata increment operation with replay storage
    const result = await this.search.updateContext(this.jobId, {
      '@udata:increment': JSON.stringify({ field: this.safeKey(key), value: val }),
      [ssGuid]: '', // Pass replay ID to hash module for transactional replay storage
    });

    return Number(result);
  }

  /**
   * Multiplies the value of a field by the given amount. Returns the
   * new value of the field after the multiplication. NOTE:
   * this is exponential multiplication.
   *
   * @example
   * const search = await workflow.search();
   * const product = await search.mult('field1', 1.5);
   */
  async mult(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    
    if (ssGuid in replay) {
      return Math.exp(Number(replay[ssGuid]));
    }

    // Use server-side udata multiply operation with replay storage
    const result = await this.search.updateContext(this.jobId, {
      '@udata:multiply': JSON.stringify({ field: this.safeKey(key), value: val }),
      [ssGuid]: '', // Pass replay ID to hash module for transactional replay storage
    });

    // The result is the log value, so we need to exponentiate it
    return Math.exp(Number(result));
  }
}
