import { KeyType } from '../../../modules/key';
import { ILogger } from '../../logger';
import { SerializerService as Serializer } from '../../serializer';
import { Cache } from '../cache';
import { StoreService } from '../index';
import { RedisClientType, RedisMultiType } from '../../../types/ioredisclient';
import { ReclaimedMessageType } from '../../../types/stream';

class IORedisStoreService extends StoreService<RedisClientType, RedisMultiType> {
  redisClient: RedisClientType;
  cache: Cache;
  namespace: string;
  appId: string;
  logger: ILogger;
  serializer: Serializer;

  constructor(redisClient: RedisClientType) {
    super(redisClient);
  }

  getMulti(): RedisMultiType {
    return this.redisClient.multi();
  }

  async exec(...args: any[]): Promise<string|string[]|string[][]> {
    const response = await this.redisClient.call.apply(this.redisClient, args as any);
    if (typeof response === 'string') {
      return response as string;
    } else if (Array.isArray(response)) {
      if (Array.isArray(response[0])) {
        return response as string[][];
      }
      return response as string[];
    }
    return response;
  }

  hGetAllResult(result: any) {
    //ioredis response signature is [null, {}] or [null, null]
    return result[1];
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const multi = this.redisClient.multi();
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      multi.zadd(zsetKey, 'NX', Date.now(), key);
    }
    await multi.exec();
  }

  async publish(keyType: KeyType.QUORUM, message: Record<string, any>, appId: string, engineId?: string): Promise<boolean> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const status: number = await this.redisClient.publish(topic, JSON.stringify(message));
    return status === 1;
  }

  async xgroup(command: 'CREATE', key: string, groupName: string, id: string, mkStream?: 'MKSTREAM'): Promise<boolean> {
    if (mkStream === 'MKSTREAM') {
      try {
        return (await this.redisClient.xgroup(command, key, groupName, id, mkStream)) === 'OK';
      } catch (err) {
        this.logger.debug(`Consumer group not created with MKSTREAM for key: ${key} and group: ${groupName}`);
        throw err;
      }
    } else {
      try {
        return (await this.redisClient.xgroup(command, key, groupName, id)) === 'OK';
      } catch (err) {
        this.logger.debug(`Consumer group not created for key: ${key} and group: ${groupName}`);
        throw err;
      }
    }
  }

  async xadd(key: string, id: string, messageId: string, messageValue: string, multi?: RedisMultiType): Promise<string | RedisMultiType> {
    try {
      return await (multi || this.redisClient).xadd(key, id, messageId, messageValue);
    } catch (error) {
      this.logger.error(`Error publishing 'xadd'; key: ${key}`, { error });
      throw error;
    }
  }

  async xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string
  ): Promise<[string, string, number, [string, number][]][] | [string, string, number, number] | unknown[]> {
    try {
      return await this.redisClient.xpending(key, group, start, end, count, consumer);
    } catch (error) {
      this.logger.error(`Error in retrieving pending messages for [stream ${key}], [group ${group}]`, { error });
      throw error;
    }
  }

  async xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<ReclaimedMessageType> {
    try {
      return await this.redisClient.xclaim(key, group, consumer, minIdleTime, id, ...args) as unknown as ReclaimedMessageType;
    } catch (error) {
      this.logger.error(`Error in claiming message with id: ${id} in group: ${group} for key: ${key}`, { error });
      throw error;
    }
  }

  async xack(key: string, group: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      return await (multi || this.redisClient).xack(key, group, id);
    } catch (error) {
      this.logger.error(`Error in acknowledging messages in group: ${group} for key: ${key}`, { error });
      throw error;
    }
  }

  async xdel(key: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      return await (multi || this.redisClient).xdel(key, id);
    } catch (error) {
      this.logger.error(`Error in deleting messages with id: ${id} for key: ${key}`, { error });
      throw error;
    }
  }
}

export { IORedisStoreService };
