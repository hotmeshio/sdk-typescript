import { KeyType } from '../../../modules/key';
import { ILogger } from '../../logger';
import { SerializerService as Serializer } from '../../serializer';
import { Cache } from '../cache';
import { StoreService } from '../index';
import {
  IORedisClientType as RedisClientType,
  IORedisMultiType as RedisMultiType,
} from '../../../types/redis';
import { ReclaimedMessageType } from '../../../types/stream';
import { HMSH_IS_CLUSTER } from '../../../modules/enums';

class IORedisStoreService extends StoreService<
  RedisClientType,
  RedisMultiType
> {
  redisClient: RedisClientType;
  cache: Cache;
  namespace: string;
  appId: string;
  logger: ILogger;
  serializer: Serializer;

  constructor(redisClient: RedisClientType) {
    super(redisClient);
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
        commands.push({ command, args });
        return multiInstance;
      };

      const multiInstance: RedisMultiType = {
        sendCommand(command: string[]): Promise<any> {
          return my.redisClient.sendCommand(command);
        },
        async exec() {
          if (commands.length === 0) return [];

          const sameKey = commands.every(cmd => {
            return cmd.args[0] === commands[0].args[0];
          });

          if (sameKey) {
            const multi = my.redisClient.multi();
            commands.forEach(cmd => multi[cmd.command](...cmd.args));
            const results = await multi.exec();
            return results.map(item => item);
          } else {
            return Promise.all(commands.map(cmd => my.redisClient[cmd.command](...cmd.args)));
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
          return addCommand('xpending', [key, group, start, end, count, consumer]);
        },
        xclaim(
          key: string,
          group: string,
          consumer: string,
          minIdleTime: number,
          id: string,
          ...args: string[]
        ) {
          return addCommand('xclaim', [key, group, consumer, minIdleTime, id, ...args]);
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
        }
      };

      return multiInstance;
    }

    return this.redisClient.multi() as unknown as RedisMultiType;
  }

  async exec(...args: any[]): Promise<string | string[] | string[][]> {
    const response = await this.redisClient.call.apply(
      this.redisClient,
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
    return status === 1;
  }

  async xgroup(
    command: 'CREATE',
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM',
  ): Promise<boolean> {
    if (mkStream === 'MKSTREAM') {
      try {
        return (
          (await this.redisClient.xgroup(
            command,
            key,
            groupName,
            id,
            mkStream,
          )) === 'OK'
        );
      } catch (err) {
        this.logger.debug(
          `Consumer group not created with MKSTREAM for key: ${key} and group: ${groupName}`,
        );
        throw err;
      }
    } else {
      try {
        return (
          (await this.redisClient.xgroup(command, key, groupName, id)) === 'OK'
        );
      } catch (err) {
        this.logger.debug(
          `Consumer group not created for key: ${key} and group: ${groupName}`,
        );
        throw err;
      }
    }
  }

  async xadd(
    key: string,
    id: string,
    messageId: string,
    messageValue: string,
    multi?: RedisMultiType,
  ): Promise<string | RedisMultiType> {
    try {
      return await (multi || this.redisClient).xadd(
        key,
        id,
        messageId,
        messageValue,
      );
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
    | unknown[]
  > {
    try {
      return await this.redisClient.xpending(
        key,
        group,
        start,
        end,
        count,
        consumer,
      );
    } catch (error) {
      this.logger.error(
        `Error in retrieving pending messages for [stream ${key}], [group ${group}]`,
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
      return (await this.redisClient.xclaim(
        key,
        group,
        consumer,
        minIdleTime,
        id,
        ...args,
      )) as unknown as ReclaimedMessageType;
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
      return await (multi || this.redisClient).xack(key, group, id);
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
      return await (multi || this.redisClient).xdel(key, id);
    } catch (error) {
      this.logger.error(
        `Error in deleting messages with id: ${id} for key: ${key}`,
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
      return await (multi || this.redisClient).xlen(key);
    } catch (error) {
      this.logger.error(`Error getting stream depth: ${key}`, { ...error });
      throw error;
    }
  }
}

export { IORedisStoreService };
