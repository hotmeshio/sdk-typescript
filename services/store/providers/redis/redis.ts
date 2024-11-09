import { HMSH_IS_CLUSTER } from '../../../../modules/enums';
import { StoreInitializable } from '../store-initializable';
import {
  RedisRedisClientType as RedisClientType,
  RedisRedisMultiType as RedisMultiType,
} from '../../../../types/redis';

import { RedisStoreBase } from './_base';

class RedisStoreService
  extends RedisStoreBase<RedisClientType, RedisMultiType>
  implements StoreInitializable
{
  constructor(storeClient: RedisClientType) {
    super(storeClient);
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
   * When in cluster mode, the transact wrapper only
   * sends commands to the same node/shard if they share a key.
   * All other commands are sent simultaneouslyusing Promise.all
   * and are then collated
   */
  transact(): RedisMultiType {
    const my = this;
    if (HMSH_IS_CLUSTER) {
      const commands: { command: string; args: any[] }[] = [];

      const addCommand = (command: string, args: any[]) => {
        commands.push({ command: command.toUpperCase(), args });
        return multiInstance;
      };

      const multiInstance: RedisMultiType = {
        sendCommand(command: string, ...args) {
          return my.storeClient.sendCommand([command, ...args]);
        },
        async exec() {
          if (commands.length === 0) return [];

          const sameKey = commands.every((cmd) => {
            return cmd.args[0] === commands[0].args[0];
          });

          if (sameKey) {
            const multi = my.storeClient.multi();
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
                  return my.storeClient.ZADD(
                    cmd.args[0],
                    cmd.args[1],
                    cmd.args[2],
                  );
                }
                return my.storeClient[cmd.command](
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

    return this.storeClient.multi() as unknown as RedisMultiType;
  }

  async exec(...args: any[]): Promise<string | string[] | string[][]> {
    return await this.storeClient.sendCommand(args);
  }

  async setnxex(
    key: string,
    value: string,
    expireSeconds: number,
  ): Promise<boolean> {
    const status: number = await this.storeClient[this.commands.set](
      key,
      value,
      { NX: true, EX: expireSeconds },
    );
    return this.isSuccessful(status);
  }

  async zAdd(
    key: string,
    score: number | string,
    value: string | number,
    redisMulti?: RedisMultiType,
  ): Promise<any> {
    return await (redisMulti || this.storeClient)[this.commands.zadd](key, {
      score: score,
      value: value.toString(),
    } as any);
  }

  async zRangeByScoreWithScores(
    key: string,
    score: number | string,
    value: string | number,
  ): Promise<string | null> {
    const result = await this.storeClient[
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
    const result = await this.storeClient[this.commands.zrangebyscore](
      key,
      score,
      value,
    );
    if (result?.length > 0) {
      return result[0];
    }
    return null;
  }
}

export { RedisStoreService };
