import { KeyType } from '../../../modules/key';
import { ILogger } from '../../logger';
import { SerializerService as Serializer } from '../../serializer';
import { Cache } from '../cache';
import { StoreService } from '../index';
import { RedisClientType, RedisMultiType } from '../../../types/redisclient';
import { ReclaimedMessageType } from '../../../types/stream';

class RedisStoreService extends StoreService<RedisClientType, RedisMultiType> {
  redisClient: RedisClientType;
  cache: Cache;
  namespace: string;
  appId: string;
  logger: ILogger;
  serializer: Serializer;
  commands: Record<string, string>;

  constructor(redisClient: RedisClientType) {
    super(redisClient);
    this.commands = {
      set: 'SET',
      setnx: 'SETNX',
      del: 'DEL',
      expire: 'EXPIRE',
      hscan: 'HSCAN',
      hset: 'HSET',
      hsetnx: 'HSETNX',
      hincrby: 'HINCRBY',
      hdel: 'HDEL',
      hget: 'HGET',
      hmget: 'HMGET',
      hgetall: 'HGETALL',
      hincrbyfloat: 'HINCRBYFLOAT',
      zrange: 'ZRANGE',
      zrangebyscore_withscores: 'ZRANGEBYSCORE_WITHSCORES',
      zrangebyscore: 'ZRANGEBYSCORE',
      zrem: 'ZREM',
      zadd: 'ZADD',
      lmove: 'LMOVE',
      lrange: 'LRANGE',
      llen: 'LLEN',
      lpop: 'LPOP',
      rename: 'RENAME',
      rpush: 'RPUSH',
      scan: 'SCAN',
      xack: 'XACK',
      xdel: 'XDEL',
      xlen: 'XLEN',
    };
  }

  getMulti(): RedisMultiType {
    return this.redisClient.multi() as unknown as RedisMultiType;
  }

  async exec(...args: any[]): Promise<string|string[]|string[][]> {
    return await this.redisClient.sendCommand(args);
  }

  async publish(keyType: KeyType.QUORUM, message: Record<string, any>, appId: string, engineId?: string): Promise<boolean> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const status: number = await this.redisClient.publish(topic, JSON.stringify(message));
    return this.isSuccessful(status);
  }

  async zAdd(key: string, score: number | string, value: string | number, redisMulti?: RedisMultiType): Promise<any> {
    return await (redisMulti || this.redisClient)[this.commands.zadd](key, { score: score, value: value.toString() } as any);
  }

  async zRangeByScoreWithScores(key: string, score: number | string, value: string | number): Promise<string | null> {
    const result = await this.redisClient[this.commands.zrangebyscore_withscores](key, score, value);
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async zRangeByScore(key: string, score: number | string, value: string | number): Promise<string | null> {
    const result = await this.redisClient[this.commands.zrangebyscore](key, score, value);
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async xgroup(command: 'CREATE', key: string, groupName: string, id: string, mkStream?: 'MKSTREAM'): Promise<boolean> {
    const args = mkStream === 'MKSTREAM' ? ['MKSTREAM'] : [];
    try {
      return (await this.redisClient.sendCommand(['XGROUP', 'CREATE', key, groupName, id, ...args])) === 1;
    } catch (error) {
      const streamType = mkStream === 'MKSTREAM' ? 'with MKSTREAM' : 'without MKSTREAM';
      this.logger.debug(`x-group-error ${streamType} for key: ${key} and group: ${groupName}`, { error });
      throw error;
    }
  }

  async xadd(key: string, id: string, ...args: any[]): Promise<string | RedisMultiType> {
    let multi: RedisMultiType;
    if (typeof args[args.length - 1] !== 'string') {
      multi = args.pop() as RedisMultiType;
    }
    try {
      return await (multi || this.redisClient).XADD(key, id, { [args[0]]: args[1] });
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
  ): Promise<[string, string, number, [string, number][]][] | [string, string, number, number]> {
    try {
      const args = [key, group];
      if (start) args.push(start);
      if (end) args.push(end);
      if (count !== undefined) args.push(count.toString());
      if (consumer) args.push(consumer);
      return await this.redisClient.sendCommand(['XPENDING', ...args]);
    } catch (error) {
      this.logger.error(`Error in retrieving pending messages for group: ${group} in key: ${key}`, { error });
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
      return await this.redisClient.sendCommand(['XCLAIM', key, group, consumer, minIdleTime.toString(), id, ...args]) as unknown as ReclaimedMessageType;
    } catch (error) {
      this.logger.error(`Error in claiming message with id: ${id} in group: ${group} for key: ${key}`, { error });
      throw error;
    }
  }

  async xack(key: string, group: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      if (multi) {
        multi[this.commands.xack](key, group, id);
        return multi;
      } else {
        return await this.redisClient[this.commands.xack](key, group, id);
      }
    } catch (error) {
      this.logger.error(`Error in acknowledging messages in group: ${group} for key: ${key}`, { error });
      throw error;
    }
  }

  async xdel(key: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      if (multi) {
        multi[this.commands.xdel](key, id);
        return multi;
      } else {
        return await this.redisClient[this.commands.xdel](key, id);
      }
    } catch (error) {
      this.logger.error(`Error in deleting messages with ids: ${id} for key: ${key}`, { error });
      throw error;
    }
  }

  async xlen(key: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      if (multi) {
        multi.XLEN(key);
        return multi;
      } else {
        return await this.redisClient.XLEN(key);
      }
    } catch (error) {
      this.logger.error(`Error getting stream depth: ${key}`, { error });
      throw error;
    }
  }

}

export { RedisStoreService };
