import { HotMeshService as HotMesh } from '../hotmesh'
import { RedisClient, RedisMulti } from '../../types/redis';
import { StoreService } from '../store';
import { KeyService, KeyType } from '../../modules/key';

export class Search {
  jobId: string;
  hotMeshClient: HotMesh;
  store: StoreService<RedisClient, RedisMulti> | null;

  safeKey(key:string): string {
    //note: protect the execution namespace with a prefix,
    //so its design never conflicts with the hotmesh keyspace
    return `_${key}`;
  }

  constructor(workflowId: string, hotMeshClient: HotMesh) {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: ''
    }
    const hotMeshPrefix = KeyService.mintKey(hotMeshClient.namespace, KeyType.JOB_STATE, keyParams);
    this.jobId = `${hotMeshPrefix}${workflowId}`;
    this.hotMeshClient = hotMeshClient;
    this.store = hotMeshClient.engine.store as StoreService<RedisClient, RedisMulti>;
  }

  async set(key: string, value: string): Promise<void> {
    await this.store.exec('HSET', this.jobId, this.safeKey(key), value.toString());
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
    await this.store.exec('HDEL', this.jobId, this.safeKey(key));
  }

  async incr(key: string, val: number): Promise<number> {
    return Number(await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), val.toString()) as string);
  }

  async mult(key: string, val: number): Promise<number> {
    const log = Math.log(val);
    const logTotal = Number(await this.store.exec('HINCRBYFLOAT', this.jobId, this.safeKey(key), log.toString()) as string);
    return Math.exp(logTotal);
  }
}
