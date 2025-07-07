import { HotMesh } from '../hotmesh';
import { SearchService } from '../search';
import { KeyService, KeyType } from '../../modules/key';
import { WorkflowSearchOptions } from '../../types/memflow';
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
  cachedFields: Record<string, string> = {};

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
        hotMeshClient.engine.logger.info('memflow-client-search-err', {
          error,
        });
      }
    }
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
      hotMeshClient.engine.logger.info('memflow-client-search-list-err', {
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
    const fields: Record<string, string> = {};
    if (typeof args[0] === 'object') {
      for (const [key, value] of Object.entries(args[0])) {
        delete this.cachedFields[key];
        fields[this.safeKey(key)] = value.toString();
      }
    } else {
      for (let i = 0; i < args.length; i += 2) {
        const keyName = args[i];
        delete this.cachedFields[keyName];
        const key = this.safeKey(keyName);
        const value = args[i + 1].toString();
        fields[key] = value;
      }
    }
    const fieldCount = await this.search.setFields(this.jobId, fields);
    await this.search.setFields(this.jobId, {
      [ssGuid]: fieldCount.toString(),
    });
    return fieldCount;
  }

  /**
   * Returns the value of the record data field, given a field id
   *
   * @example
   * const search = await workflow.search();
   * const value = await search.get('field1');
   */
  async get(id: string): Promise<string> {
    try {
      if (id in this.cachedFields) {
        return this.cachedFields[id];
      }
      const value = await this.search.getField(this.jobId, this.safeKey(id));
      this.cachedFields[id] = value;
      return value;
    } catch (error) {
      this.hotMeshClient.logger.error('memflow-search-get-error', {
        error,
      });
      return '';
    }
  }

  /**
   * Returns the values of all specified fields in the HASH stored at key.
   */
  async mget(...args: string[]): Promise<string[]> {
    let isCached = true;
    const values: string[] = [];
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i++) {
      if (isCached && args[i] in this.cachedFields) {
        values.push(this.cachedFields[args[i]]);
      } else {
        isCached = false;
      }
      safeArgs.push(this.safeKey(args[i]));
    }
    try {
      if (isCached) {
        return values;
      }
      const returnValues = await this.search.getFields(this.jobId, safeArgs);
      returnValues.forEach((value, index) => {
        if (value !== null) {
          this.cachedFields[args[index]] = value;
        }
      });
      return returnValues;
    } catch (error) {
      this.hotMeshClient.logger.error('memflow-search-mget-error', {
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
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i++) {
      const keyName = args[i];
      delete this.cachedFields[keyName];
      safeArgs.push(this.safeKey(keyName));
    }
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const response = await this.search.deleteFields(this.jobId, safeArgs);
    const formattedResponse = isNaN(response as unknown as number)
      ? 0
      : Number(response);
    await this.search.setFields(this.jobId, {
      [ssGuid]: formattedResponse.toString(),
    });
    return formattedResponse;
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
    delete this.cachedFields[key];
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const num = await this.search.incrementFieldByFloat(
      this.jobId,
      this.safeKey(key),
      val,
    );
    await this.search.setFields(this.jobId, { [ssGuid]: num.toString() });
    return num;
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
    delete this.cachedFields[key];
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Math.exp(Number(replay[ssGuid]));
    }
    const ssGuidValue = await this.search.incrementFieldByFloat(
      this.jobId,
      ssGuid,
      1,
    );
    if (ssGuidValue === 1) {
      const log = Math.log(val);
      const logTotal = await this.search.incrementFieldByFloat(
        this.jobId,
        this.safeKey(key),
        log,
      );
      await this.search.setFields(this.jobId, {
        [ssGuid]: logTotal.toString(),
      });
      return Math.exp(logTotal);
    } else {
      const logTotalStr = await this.search.getField(this.jobId, ssGuid);
      const logTotal = Number(logTotalStr);
      return Math.exp(logTotal);
    }
  }
}
