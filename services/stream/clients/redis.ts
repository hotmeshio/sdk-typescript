import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../modules/key';
import { ILogger } from '../../logger';
import { StreamService } from '../index';
import {
  RedisRedisClientType as RedisClientType,
  RedisRedisMultiType as RedisMultiType,
} from '../../../types/redis';
import { ReclaimedMessageType } from '../../../types/stream';

class RedisStreamService extends StreamService<
  RedisClientType,
  RedisMultiType
> {
  redisClient: RedisClientType;
  namespace: string;
  logger: ILogger;
  appId: string;

  constructor(redisClient: RedisClientType) {
    super(redisClient);
  }

  async init(namespace = HMNS, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.logger = logger;
    this.appId = appId;
  }

  getMulti(): RedisMultiType {
    return this.redisClient.multi() as unknown as RedisMultiType;
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
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
      this.logger.error(
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

  async xreadgroup(
    command: 'GROUP',
    groupName: string,
    consumerName: string,
    blockOption: 'BLOCK' | 'COUNT',
    blockTime: number | string,
    streamsOption: 'STREAMS',
    streamName: string,
    id: string,
  ): Promise<string[][][] | null> {
    try {
      return await this.redisClient.sendCommand([
        'XREADGROUP',
        command,
        groupName,
        consumerName,
        blockOption,
        blockTime.toString(),
        streamsOption,
        streamName,
        id,
      ]);
    } catch (error) {
      this.logger.error(
        `Error reading stream data [Stream ${streamName}] [Group ${groupName}]`,
        { ...error },
      );
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
      try {
        return await this.redisClient.sendCommand(['XPENDING', ...args]);
      } catch (error) {
        this.logger.error('error, args', { ...error }, args);
      }
    } catch (error) {
      this.logger.error(
        `Error retrieving pending messages for group: ${group} in key: ${key}`,
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
        `Error claiming message with id: ${id} in group: ${group} for key: ${key}`,
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
        multi.XACK(key, group, id);
        return multi;
      } else {
        return await this.redisClient.XACK(key, group, id);
      }
    } catch (error) {
      this.logger.error(
        `Error acknowledging messages in group: ${group} for key: ${key}`,
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
        multi.XDEL(key, id);
        return multi;
      } else {
        return await this.redisClient.XDEL(key, id);
      }
    } catch (error) {
      this.logger.error(
        `Error deleting messages with ids: ${id} for key: ${key}`,
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

export { RedisStreamService };
