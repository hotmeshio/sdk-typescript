import { HotMeshService as HotMesh } from '../hotmesh'
import { RedisClient, RedisMulti } from '../../types/redis';
import { StoreService } from '../store';
import { KeyService, KeyType } from '../../modules/key';
import { WorkflowSearchOptions } from '../../types/durable';
import { asyncLocalStorage } from '../../modules/storage';

export class Search {
  jobId: string;
  searchSessionId: string;
  searchSessionIndex: number = 0;
  hotMeshClient: HotMesh;
  store: StoreService<RedisClient, RedisMulti> | null;

  constructor(workflowId: string, hotMeshClient: HotMesh, searchSessionId: string) {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: workflowId
    }
    this.jobId = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
    this.searchSessionId = searchSessionId;
    this.hotMeshClient = hotMeshClient;
    this.store = hotMeshClient.engine.store as StoreService<RedisClient, RedisMulti>;
  }

  safeKey(key:string): string {
    return `_${key}`;
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will configure the search index for the workflow. For all
   * others, this method will exit/fail gracefully and not index
   * the fields in the HASH. However, all values are still available
   * in the HASH.
   */
  static async configureSearchIndex(hotMeshClient: HotMesh, search?: WorkflowSearchOptions): Promise<void> {
    if (search?.schema) {
      const store = hotMeshClient.engine.store;
      const schema: string[] = [];
      for (const [key, value] of Object.entries(search.schema)) {
        //prefix with a comma (avoids collisions with hotmesh reserved words)
        schema.push(`_${key}`);
        schema.push(value.type);
        if (value.sortable) {
          schema.push('SORTABLE');
        }
      }
      try {
        const keyParams = {
          appId: hotMeshClient.appId,
          jobId: ''
        }
        const hotMeshPrefix = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
        const prefixes = search.prefix.map((prefix) => `${hotMeshPrefix}${prefix}`);
        await store.exec('FT.CREATE', `${search.index}`, 'ON', 'HASH', 'PREFIX', prefixes.length.toString(), ...prefixes, 'SCHEMA', ...schema);
      } catch (error) {
        hotMeshClient.engine.logger.info('durable-client-search-err', { ...error });
      }
    }
  }

  /**
   * For those deployments with a redis stack backend (with the FT module),
   * this method will list all search indexes.
   * @param {HotMesh} hotMeshClient - the hotmesh client
   * @returns {Promise<string[]>} - the list of search indexes
   */
  static async listSearchIndexes(hotMeshClient: HotMesh): Promise<string[]> {
    try {
      const store = hotMeshClient.engine.store;
      const searchIndexes = await store.exec('FT._LIST');
      return searchIndexes as string[];
    } catch (error) {
      hotMeshClient.engine.logger.info('durable-client-search-list-err', { ...error });
      return [];
    }
  }

  /**
   * increments the index to return a unique search session guid when
   * calling any method that produces side effects (changes the value)
   */
  getSearchSessionGuid(): string {
    //return the search session as it would exist in the search session index
    return `${this.searchSessionId}-${this.searchSessionIndex++}-`;
  }

  /**
   * Sets the fields listed in args. Returns the
   * count of new fields that were set (does not
   * count fields that were updated)
   */
  async set(...args: string[]): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i += 2) {
      const key = this.safeKey(args[i]);
      const value = args[i+1].toString();
      safeArgs.push(key, value);
    }
    const fieldCount = await this.store.exec('HSET', this.jobId, ...safeArgs);
    //no need to wait; set this interim value in the replay
    this.store.exec('HSET', this.jobId, ssGuid, fieldCount.toString());
    return Number(fieldCount);
  }

  async get(key: string): Promise<string> {
    try {
      return await this.store.exec('HGET',this.jobId, this.safeKey(key)) as string;
    } catch (error) {
      this.hotMeshClient.logger.error('durable-search-get-error', { ...error });
      return '';
    }
  }

  async mget(...args: string[]): Promise<string[]> {
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i++) {
      safeArgs.push(this.safeKey(args[i]));
    }
    try {
      return await this.store.exec('HMGET', this.jobId, ...safeArgs) as string[];
    } catch (error) {
      this.hotMeshClient.logger.error('durable-search-mget-error', { ...error });
      return [];
    }
  }

  /**
   * Deletes the fields listed in args. Returns the
   * count of fields that were deleted.
   */
  async del(...args: string[]): Promise<number | void> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const safeArgs: string[] = [];
    for (let i = 0; i < args.length; i++) {
      safeArgs.push(this.safeKey(args[i]));
    }
    const response = await this.store.exec('HDEL', this.jobId, ...safeArgs);
    const formattedResponse = isNaN(response as unknown as number) ? 0 : Number(response);
    //no need to wait; set this interim value in the replay
    this.store.exec('HSET', this.jobId, ssGuid, formattedResponse.toString());
    return formattedResponse;
  }

  /**
   * Increments the value of a field by the given amount. Returns the
   * new value of the field after the increment. Can be
   * used to decrement the value of a field by specifying a negative.
   */
  async incr(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Number(replay[ssGuid]);
    }
    const num = await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), val.toString()) as string;
    //no need to wait; set this interim value in the replay
    this.store.exec('HSET', this.jobId, ssGuid, num.toString());
    return Number(num);
  }

  /**
   * Multiplies the value of a field by the given amount. Returns the
   * new value of the field after the multiplication. NOTE:
   * this is exponential multiplication.
   */
  async mult(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};
    if (ssGuid in replay) {
      return Math.exp(Number(replay[ssGuid]));
    }
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      const log = Math.log(val);
      const logTotal = await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), log.toString()) as string;
      //no need to wait; set this interim value in the replay
      this.store.exec('HSET', this.jobId, ssGuid, logTotal.toString());
      return Math.exp(Number(logTotal));
    }
  }
}
