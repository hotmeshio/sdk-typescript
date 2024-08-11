import { KeyType } from '../../../modules/key';
import { ILogger } from '../../logger';
import { SerializerService as Serializer } from '../../serializer';
import { Cache } from '../cache';
import { StoreService } from '../index';
import {
  RedisRedisClientType as RedisClientType,
  RedisRedisMultiType as RedisMultiType,
} from '../../../types/redis';
import { ReclaimedMessageType } from '../../../types/stream';
import { HMSH_IS_CLUSTER } from '../../../modules/enums';

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
      get: 'GET',
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

  /**
   * When in cluster mode, the getMulti wrapper only
   * sends commands to the same node/shard if they share a key.
   * All other commands are sent simultaneouslyusing Promise.all
   * and are then collated
   */
  getMulti(): RedisMultiType {
    const my = this;
    if (HMSH_IS_CLUSTER) {
      const commands: { command: string; args: any[] }[] = [];

      const addCommand = (command: string, args: any[]) => {
        commands.push({ command: command.toUpperCase(), args });
        return multiInstance;
      };

      const multiInstance: RedisMultiType = {
        sendCommand(command: string, ...args) {
          return my.redisClient.sendCommand([command, ...args]);
        },
        async exec() {
          if (commands.length === 0) return [];

          const sameKey = commands.every((cmd) => {
            return cmd.args[0] === commands[0].args[0];
          });

          if (sameKey) {
            const multi = my.redisClient.multi();
            commands.forEach((cmd) => {
              if (cmd.command === 'ZADD') {
                return multi.ZADD(cmd.args[0], cmd.args[1], cmd.args[2]);
              }
              return multi[cmd.command](...(cmd.args as unknown as any[]));
            });
            const results = await multi.exec();
            return results.map((item) => item);
          } else {
            return Promise.all(
              commands.map((cmd) => {
                if (cmd.command === 'ZADD') {
                  return my.redisClient.ZADD(
                    cmd.args[0],
                    cmd.args[1],
                    cmd.args[2],
                  );
                }
                return my.redisClient[cmd.command](
                  ...(cmd.args as unknown as any[]),
                );
              }),
            );
          }
        },
        XADD(key: string, id: string, fields: any, message?: string) {
          return addCommand('XADD', [key, id, fields, message]);
        },
        XACK(key: string, group: string, id: string) {
          return addCommand('XACK', [key, group, id]);
        },
        XDEL(key: string, id: string) {
          return addCommand('XDEL', [key, id]);
        },
        XLEN(key: string) {
          return addCommand('XLEN', [key]);
        },
        XCLAIM(
          key: string,
          group: string,
          consumer: string,
          minIdleTime: number,
          id: string,
          ...args: string[]
        ) {
          return addCommand('XCLAIM', [
            key,
            group,
            consumer,
            minIdleTime,
            id,
            ...args,
          ]);
        },
        XPENDING(
          key: string,
          group: string,
          start?: string,
          end?: string,
          count?: number,
          consumer?: string,
        ) {
          return addCommand('XPENDING', [
            key,
            group,
            start,
            end,
            count,
            consumer,
          ]);
        },
        DEL: function (key: string): RedisMultiType {
          return addCommand('DEL', [key]);
        },
        EXPIRE: function (key: string, seconds: number): RedisMultiType {
          return addCommand('EXPIRE', [key, seconds]);
        },
        HDEL(key: string, itemId: string) {
          return addCommand('HDEL', [key, itemId]);
        },
        HGET(key: string, itemId: string) {
          return addCommand('HGET', [key, itemId]);
        },
        HGETALL(key: string) {
          return addCommand('HGETALL', [key]);
        },
        HINCRBYFLOAT(key: string, itemId: string, value: number) {
          return addCommand('HINCRBYFLOAT', [key, itemId, value]);
        },
        HMGET(key: string, itemIds: string[]) {
          return addCommand('HMGET', [key, itemIds]);
        },
        HSET(key: string, values: Record<string, string>) {
          return addCommand('HSET', [key, values]);
        },
        LRANGE(key: string, start: number, end: number) {
          return addCommand('LRANGE', [key, start, end]);
        },
        RPUSH(key: string, items: string[]) {
          return addCommand('RPUSH', [key, items]);
        },
        ZADD(
          key: string,
          args: { score: number | string; value: string },
          opts?: { NX: boolean },
        ) {
          return addCommand('ZADD', [key, args, opts]);
        },
        XGROUP(
          command: 'CREATE',
          key: string,
          groupName: string,
          id: string,
          mkStream?: 'MKSTREAM',
        ) {
          return addCommand('XGROUP', [command, key, groupName, id, mkStream]);
        },
        EXISTS: function (key: string): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        HMPUSH: function (
          key: string,
          values: Record<string, string>,
        ): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        LPUSH: function (key: string, items: string[]): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        SET: function (key: string, value: string): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        ZRANGE_WITHSCORES: function (
          key: string,
          start: number,
          end: number,
        ): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        ZRANK: function (key: string, member: string): RedisMultiType {
          throw new Error('Function not implemented.');
        },
        ZSCORE: function (key: string, value: string): RedisMultiType {
          throw new Error('Function not implemented.');
        },
      };

      return multiInstance;
    }

    return this.redisClient.multi() as unknown as RedisMultiType;
  }

  async exec(...args: any[]): Promise<string | string[] | string[][]> {
    return await this.redisClient.sendCommand(args);
  }

  async setnxex(
    key: string,
    value: string,
    expireSeconds: number,
  ): Promise<boolean> {
    const status: number = await this.redisClient[this.commands.set](
      key,
      value,
      { NX: true, EX: expireSeconds },
    );
    return this.isSuccessful(status);
  }

  async publish(
    keyType: KeyType.QUORUM,
    message: Record<string, any>,
    appId: string,
    engineId?: string,
  ): Promise<boolean> {
    const topic = this.mintKey(keyType, { appId, engineId });
    const status: number = await this.redisClient.publish(
      topic,
      JSON.stringify(message),
    );
    return this.isSuccessful(status);
  }

  async zAdd(
    key: string,
    score: number | string,
    value: string | number,
    redisMulti?: RedisMultiType,
  ): Promise<any> {
    return await (redisMulti || this.redisClient)[this.commands.zadd](key, {
      score: score,
      value: value.toString(),
    } as any);
  }

  async zRangeByScoreWithScores(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.redisClient[
      this.commands.zrangebyscore_withscores
    ](key, score, value);
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async zRangeByScore(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.redisClient[this.commands.zrangebyscore](
      key,
      score,
      value,
    );
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }

  async xgroup(
    command: 'CREATE',
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM',
  ): Promise<boolean> {
    const args = mkStream === 'MKSTREAM' ? ['MKSTREAM'] : [];
    try {
      return (
        (await this.redisClient.sendCommand([
          'XGROUP',
          'CREATE',
          key,
          groupName,
          id,
          ...args,
        ])) === 1
      );
    } catch (error) {
      const streamType =
        mkStream === 'MKSTREAM' ? 'with MKSTREAM' : 'without MKSTREAM';
      this.logger.debug(
        `x-group-error ${streamType} for key: ${key} and group: ${groupName}`,
        { ...error },
      );
      throw error;
    }
  }

  async xadd(
    key: string,
    id: string,
    ...args: any[]
  ): Promise<string | RedisMultiType> {
    let multi: RedisMultiType;
    if (typeof args[args.length - 1] !== 'string') {
      multi = args.pop() as RedisMultiType;
    }
    try {
      return await (multi || this.redisClient).XADD(key, id, {
        [args[0]]: args[1],
      });
    } catch (error) {
      this.logger.error(`Error publishing 'xadd'; key: ${key}`, { ...error });
      throw error;
    }
  }

  async xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string,
  ): Promise<
    | [string, string, number, [string, number][]][]
    | [string, string, number, number]
  > {
    try {
      const args = [key, group];
      if (start) args.push(start);
      if (end) args.push(end);
      if (count !== undefined) args.push(count.toString());
      if (consumer) args.push(consumer);
      return await this.redisClient.sendCommand(['XPENDING', ...args]);
    } catch (error) {
      this.logger.error(
        `Error in retrieving pending messages for group: ${group} in key: ${key}`,
        { ...error },
      );
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
      return (await this.redisClient.sendCommand([
        'XCLAIM',
        key,
        group,
        consumer,
        minIdleTime.toString(),
        id,
        ...args,
      ])) as unknown as ReclaimedMessageType;
    } catch (error) {
      this.logger.error(
        `Error in claiming message with id: ${id} in group: ${group} for key: ${key}`,
        { ...error },
      );
      throw error;
    }
  }

  async xack(
    key: string,
    group: string,
    id: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      if (multi) {
        multi[this.commands.xack](key, group, id);
        return multi;
      } else {
        return await this.redisClient[this.commands.xack](key, group, id);
      }
    } catch (error) {
      this.logger.error(
        `Error in acknowledging messages in group: ${group} for key: ${key}`,
        { ...error },
      );
      throw error;
    }
  }

  async xdel(
    key: string,
    id: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      if (multi) {
        multi[this.commands.xdel](key, id);
        return multi;
      } else {
        return await this.redisClient[this.commands.xdel](key, id);
      }
    } catch (error) {
      this.logger.error(
        `Error in deleting messages with ids: ${id} for key: ${key}`,
        { ...error },
      );
      throw error;
    }
  }

  async xlen(
    key: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      if (multi) {
        multi.XLEN(key);
        return multi;
      } else {
        return await this.redisClient.XLEN(key);
      }
    } catch (error) {
      this.logger.error(`Error getting stream depth: ${key}`, { ...error });
      throw error;
    }
  }
}

export { RedisStoreService };
