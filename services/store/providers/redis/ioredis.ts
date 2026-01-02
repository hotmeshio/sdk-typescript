import { KeyType } from '../../../../modules/key';
import {
  IORedisMultiType,
  IORedisClientType as RedisClientType,
  IORedisMultiType as RedisMultiType,
} from '../../../../types/redis';
import { HMSH_IS_CLUSTER } from '../../../../modules/enums';
import { StoreInitializable } from '../store-initializable';

import { RedisStoreBase } from './_base';

class IORedisStoreService
  extends RedisStoreBase<RedisClientType, RedisMultiType>
  implements StoreInitializable
{
  constructor(storeClient: RedisClientType) {
    super(storeClient);
    this.commands = {
      get: 'get',
      set: 'set', //nx, nx+ex
      setnx: 'setnx',
      del: 'del',
      expire: 'expire',
      hset: 'hset', //nx, nx+ex
      hscan: 'hscan',
      hsetnx: 'hsetnx',
      hincrby: 'hincrby',
      hdel: 'hdel',
      hget: 'hget',
      hmget: 'hmget',
      hgetall: 'hgetall',
      hincrbyfloat: 'hincrbyfloat',
      zrank: 'zrank',
      zrange: 'zrange',
      zrangebyscore_withscores: 'zrangebyscore',
      zrangebyscore: 'zrangebyscore',
      zrem: 'zrem',
      zadd: 'zadd', //nx
      lmove: 'lmove',
      lpop: 'lpop',
      lrange: 'lrange',
      rename: 'rename',
      rpush: 'rpush',
      scan: 'scan',
      xack: 'xack',
      xdel: 'xdel',
    };
  }

  /**
   * When in cluster mode, the transact wrapper only
   * sends commands to the same node/shard if they share a key.
   * All other commands are sent simultaneouslyusing Promise.all
   * and are then collated. this is effectiely a wrapper for
   * `multi` but is closer to `pipeline` in terms of usage when
   * promises are used.
   */
  transact(): RedisMultiType {
    const my = this;
    if (HMSH_IS_CLUSTER) {
      const commands: { command: string; args: any[] }[] = [];

      const addCommand = (command: string, args: any[]) => {
        commands.push({ command, args });
        return multiInstance;
      };

      const multiInstance: RedisMultiType = {
        sendCommand(command: string[]): Promise<any> {
          return my.storeClient.sendCommand(command);
        },
        async exec() {
          if (commands.length === 0) return [];

          const sameKey = commands.every((cmd) => {
            return cmd.args[0] === commands[0].args[0];
          });

          if (sameKey) {
            const multi = my.storeClient.multi();
            commands.forEach((cmd) => multi[cmd.command](...cmd.args));
            const results = await multi.exec();
            return results.map((item) => item);
          } else {
            return Promise.all(
              commands.map((cmd) => my.storeClient[cmd.command](...cmd.args)),
            );
          }
        },
        xadd(key: string, id: string, fields: any, message?: string) {
          return addCommand('xadd', [key, id, fields, message]);
        },
        xack(key: string, group: string, id: string) {
          return addCommand('xack', [key, group, id]);
        },
        xdel(key: string, id: string) {
          return addCommand('xdel', [key, id]);
        },
        xlen(key: string) {
          return addCommand('xlen', [key]);
        },
        xpending(
          key: string,
          group: string,
          start?: string,
          end?: string,
          count?: number,
          consumer?: string,
        ) {
          return addCommand('xpending', [
            key,
            group,
            start,
            end,
            count,
            consumer,
          ]);
        },
        xclaim(
          key: string,
          group: string,
          consumer: string,
          minIdleTime: number,
          id: string,
          ...args: string[]
        ) {
          return addCommand('xclaim', [
            key,
            group,
            consumer,
            minIdleTime,
            id,
            ...args,
          ]);
        },
        del(key: string) {
          return addCommand('del', [key]);
        },
        expire: function (key: string, seconds: number): RedisMultiType {
          return addCommand('expire', [key, seconds]);
        },
        hdel(key: string, itemId: string) {
          return addCommand('hdel', [key, itemId]);
        },
        hget(key: string, itemId: string) {
          return addCommand('hget', [key, itemId]);
        },
        hgetall(key: string) {
          return addCommand('hgetall', [key]);
        },
        hincrbyfloat(key: string, itemId: string, value: number) {
          return addCommand('hincrbyfloat', [key, itemId, value]);
        },
        hmget(key: string, itemIds: string[]) {
          return addCommand('hmget', [key, itemIds]);
        },
        hset(key: string, values: Record<string, string>) {
          return addCommand('hset', [key, values]);
        },
        lrange(key: string, start: number, end: number) {
          return addCommand('lrange', [key, start, end]);
        },
        rpush(key: string, value: string) {
          return addCommand('rpush', [key, value]);
        },
        zadd(...args: Array<string | number>) {
          return addCommand('zadd', args);
        },
        xgroup(
          command: 'CREATE',
          key: string,
          groupName: string,
          id: string,
          mkStream?: 'MKSTREAM',
        ) {
          return addCommand('xgroup', [command, key, groupName, id, mkStream]);
        },
      };

      return multiInstance;
    }

    return this.storeClient.multi() as unknown as IORedisMultiType;
  }

  async exec(...args: any[]): Promise<string | string[] | string[][]> {
    try {
      const response = await this.storeClient.call.apply(
        this.storeClient,
        args as any,
      );
      if (typeof response === 'string') {
        return response as string;
      } else if (Array.isArray(response)) {
        if (Array.isArray(response[0])) {
          return response as string[][];
        }
        return response as string[];
      }
      return response;
    } catch (error) {
      // Connection closed during test cleanup - log and return empty response
      if (error?.message?.includes('Connection is closed')) {
        return [];
      }
      // Re-throw unexpected errors
      throw error;
    }
  }

  async setnxex(
    key: string,
    value: string,
    expireSeconds: number,
  ): Promise<boolean> {
    const status: number = await this.storeClient[this.commands.set](
      key,
      value,
      'NX',
      'EX',
      expireSeconds.toString(),
    );
    return this.isSuccessful(status);
  }

  hGetAllResult(result: any) {
    //ioredis response signature is [null, {}] or [null, null]
    return result[1];
  }

  async addTaskQueues(keys: string[]): Promise<void> {
    const multi = this.storeClient.multi();
    const zsetKey = this.mintKey(KeyType.WORK_ITEMS, { appId: this.appId });
    for (const key of keys) {
      multi.zadd(zsetKey, 'NX', Date.now(), key);
    }
    await multi.exec();
  }
}

export { IORedisStoreService };
