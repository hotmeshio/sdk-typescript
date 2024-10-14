import { HotMesh } from '../hotmesh';
import { RedisClient, RedisMulti } from '../../types/redis';
import { StoreService } from '../store';
import { KeyService, KeyType } from '../../modules/key';
import { WorkflowSearchOptions } from '../../types/meshflow';
import { asyncLocalStorage } from '../../modules/storage';

/**
 * The Search module provides methods for reading and
 * writing data to a workflow record. The instance
 * methods exposed by this class are available
 * for use from within a running workflow. The following example
 * uses search to set a `name` field and increment a
 * `counter` field. The workflow returns the incremented value.
 * 
 * @example
 * ```typescript
 * //searchWorkflow.ts
 * import { MeshFlow } from '@hotmeshio/hotmesh';

 * export async function searchExample(name: string): Promise<{counter: number}> {
 *   const search = await MeshFlow.workflow.search();
 *   await search.set('name', name);
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
  store: StoreService<RedisClient, RedisMulti> | null;
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
    this.store = hotMeshClient.engine.store as StoreService<
      RedisClient,
      RedisMulti
    >;
  }

  /**
   * Prefixes the key with an underscore to keep separate from the
   * activity and job history (and searchable via HKEYS)
   * @param {string} key - the key to be sanitized. Wrap in quotes to avoid sanitization.
   * @returns {string} - the sanitized key
   * @private
   */
  safeKey(key: string): string {
    if (key.startsWith('"')) {
      return key.slice(1, -1);
    }
    return `_${key}`;
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will configure the search index for the workflow. For all
   * others, this method will exit/fail gracefully and not index
   * the fields in the HASH. All values are searchable via HKEYS/HSC/HGET
   * @param {HotMesh} hotMeshClient - the hotmesh client
   * @param {WorkflowSearchOptions} search - the search options
   * @returns {Promise<void>}
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
      const store = hotMeshClient.engine.store;
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
        await store.exec(
          'FT.CREATE',
          `${search.index}`,
          'ON',
          'HASH',
          'PREFIX',
          prefixes.length.toString(),
          ...prefixes,
          'SCHEMA',
          ...schema,
        );
      } catch (error) {
        hotMeshClient.engine.logger.info('meshflow-client-search-err', {
          ...error,
        });
      }
    }
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will list all search indexes.
   *
   * @param {HotMesh} hotMeshClient - the hotmesh client
   * @returns {Promise<string[]>} - the list of search indexes
   * @example
   * const searchIndexes = await Search.listSearchIndexes(hotMeshClient);
   */
  static async listSearchIndexes(hotMeshClient: HotMesh): Promise<string[]> {
    try {
      const store = hotMeshClient.engine.store;
      const searchIndexes = await store.exec('FT._LIST');
      return searchIndexes as string[];
    } catch (error) {
      hotMeshClient.engine.logger.info('meshflow-client-search-list-err', {
        ...error,
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
    //return the search session as it would exist in the search session index
    return `${this.searchSessionId}-${this.searchSessionIndex++}-`;
  }

  /**
   * Sets the fields listed in args. Returns the
   * count of new fields that were set (does not
   * count fields that were updated)
   * @param args
   * @returns {number}
   * @example
   * const search = new Search();
   * const count = await search.set('field1', 'value1', 'field2', 'value2');
   */
  async set(...args: string[]): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i += 2) {
      const keyName = args[i];
      delete this.cachedFields[keyName];
      const key = this.safeKey(keyName);
      const value = args[i + 1].toString();
      safeArgs.push(key, value);
    }
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const fieldCount = await this.store.exec('HSET', this.jobId, ...safeArgs);
    //no need to wait; set this interim value in the replay
    this.store.exec('HSET', this.jobId, ssGuid, fieldCount.toString());
    return Number(fieldCount);
  }

  /**
   * Returns the value of the field in the HASH stored at key.
   * @param key
   * @returns {string}
   * @example
   * const search = new Search();
   * const value = await search.get('field1');
   */
  async get(key: string): Promise<string> {
    try {
      if (key in this.cachedFields) {
        return this.cachedFields[key];
      }
      const value = (await this.store.exec(
        'HGET',
        this.jobId,
        this.safeKey(key),
      )) as string;
      this.cachedFields[key] = value;
      return value;
    } catch (error) {
      this.hotMeshClient.logger.error('meshflow-search-get-error', {
        ...error,
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
      const returnValues = (await this.store.exec(
        'HMGET',
        this.jobId,
        ...safeArgs,
      )) as string[];
      returnValues.forEach((value, index) => {
        if (value !== null) {
          this.cachedFields[args[index]] = value;
        }
      });
      return returnValues;
    } catch (error) {
      this.hotMeshClient.logger.error('meshflow-search-mget-error', {
        ...error,
      });
      return [];
    }
  }

  /**
   * Deletes the fields provided as args. Returns the
   * count of fields that were deleted.
   *
   * @param args
   * @returns {number}
   * @example
   * const search = new Search();
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
    const response = await this.store.exec('HDEL', this.jobId, ...safeArgs);
    const formattedResponse = isNaN(response as unknown as number)
      ? 0
      : Number(response);
    this.store.exec('HSET', this.jobId, ssGuid, formattedResponse.toString());
    return formattedResponse;
  }

  /**
   * Increments the value of a float field by the given amount. Returns the
   * new value of the field after the increment. Pass a negative
   * number to decrement the value.
   *
   * @param key - the key to increment
   * @param val - the value to increment by
   * @returns {number} - the new value
   * @example
   * const search = new Search();
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
    const num = (await this.store.exec(
      'HINCRBYFLOAT',
      this.jobId,
      this.safeKey(key),
      val.toString(),
    )) as string;
    this.store.exec('HSET', this.jobId, ssGuid, num.toString());
    return Number(num);
  }

  /**
   * Multiplies the value of a field by the given amount. Returns the
   * new value of the field after the multiplication. NOTE:
   * this is exponential multiplication.
   *
   * @param key - the key to multiply
   * @param val - the value to multiply by
   * @returns {number} - the new product of the multiplication
   * @example
   * const search = new Search();
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
    const ssGuidValue = Number(
      (await this.store.exec(
        'HINCRBYFLOAT',
        this.jobId,
        ssGuid,
        '1',
      )) as string,
    );
    if (ssGuidValue === 1) {
      const log = Math.log(val);
      const logTotal = (await this.store.exec(
        'HINCRBYFLOAT',
        this.jobId,
        this.safeKey(key),
        log.toString(),
      )) as string;
      this.store.exec('HSET', this.jobId, ssGuid, logTotal.toString());
      return Math.exp(Number(logTotal));
    }
  }
}
