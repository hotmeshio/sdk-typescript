import { HotMeshService as HotMesh } from '../hotmesh'
import { RedisClient, RedisMulti } from '../../types/redis';
import { StoreService } from '../store';
import { KeyService, KeyType } from '../../modules/key';

export class Search {
  jobId: string;
  searchSessionId: string;
  searchSessionIndex: number = 0;
  hotMeshClient: HotMesh;
  store: StoreService<RedisClient, RedisMulti> | null;

  safeKey(key:string): string {
    //note: protect the execution namespace with a prefix,
    //so its design never conflicts with the hotmesh keyspace
    return `_${key}`;
  }

  constructor(workflowId: string, hotMeshClient: HotMesh, searchSessionId: string) {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: ''
    }
    const hotMeshPrefix = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
    this.jobId = `${hotMeshPrefix}${workflowId}`;
    this.searchSessionId = searchSessionId;
    this.hotMeshClient = hotMeshClient;
    this.store = hotMeshClient.engine.store as StoreService<RedisClient, RedisMulti>;
  }

  /**
   * increments the index to return a unique search session guid when
   * calling any method that produces side effects (changes the value)
   */
  getSearchSessionGuid(): string {
    //return the search session as it would exist in the search session index
    return `${this.searchSessionId}-${this.searchSessionIndex++}-`;
  }

  async set(key: string, value: string): Promise<void> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      //only allowed to set a value the first time
      await this.store.exec('HSET', this.jobId, this.safeKey(key), value.toString());
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

  async del(key: string): Promise<void> {
    const ssGuid = this.getSearchSessionGuid();
    const ssGuidValue = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, ssGuid, '1') as string);
    if (ssGuidValue === 1) {
      await this.store.exec('HDEL', this.jobId, this.safeKey(key));
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
