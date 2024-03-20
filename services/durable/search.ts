import { HotMeshService as HotMesh } from '../hotmesh'
import { RedisClient, RedisMulti } from '../../types/redis';
import { StoreService } from '../store';
import { KeyService, KeyType } from '../../modules/key';
import { WorkflowSearchOptions } from '../../types/durable';

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
      } catch (err) {
        hotMeshClient.engine.logger.info('durable-client-search-err', { err });
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
    } catch (err) {
      hotMeshClient.engine.logger.info('durable-client-search-list-err', { err });
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

  async set(...args: string[]): Promise<void> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      const safeArgs: string[] = [];
      for (let i = 0; i < args.length; i += 2) {
        const key = this.safeKey(args[i]);
        const value = args[i+1].toString();
        safeArgs.push(key, value);
      }
      await this.store.exec('HSET', this.jobId, ...safeArgs);
    }
  }

  async get(key: string): Promise<string> {
    try {
      return await this.store.exec('HGET',this.jobId, this.safeKey(key)) as string;
    } catch (err) {
      this.hotMeshClient.logger.error('durable-search-get-error', { err });
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
    } catch (err) {
      this.hotMeshClient.logger.error('durable-search-mget-error', { err });
      return [];
    }
  }

  async del(...args: string[]): Promise<number | void> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      const safeArgs: string[] = [];
      for (let i = 0; i < args.length; i++) {
        safeArgs.push(this.safeKey(args[i]));
      }
      const response = await this.store.exec('HDEL', this.jobId, ...safeArgs);
      return isNaN(response as unknown as number) ? undefined : Number(response);
    }
  }

  async incr(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      return Number(await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), val.toString()) as string);
    }
  }

  async mult(key: string, val: number): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      const log = Math.log(val);
      const logTotal = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), log.toString()) as string);
      return Math.exp(logTotal);
    }
  }
}
