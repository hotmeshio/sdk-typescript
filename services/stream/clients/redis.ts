import { KeyService, KeyStoreParams, KeyType, HMNS } from '../../../modules/key';
import { ILogger } from '../../logger';
import { StreamService } from '../index';
import { RedisClientType, RedisMultiType } from '../../../types/redisclient';
import { ReclaimedMessageType } from '../../../types/stream';

class RedisStreamService extends StreamService<RedisClientType, RedisMultiType> {
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

  async xgroup(command: 'CREATE', key: string, groupName: string, id: string, mkStream?: 'MKSTREAM'): Promise<boolean> {
    const args = mkStream === 'MKSTREAM' ? ['MKSTREAM'] : [];
    try {
      return (await this.redisClient.sendCommand(['XGROUP', 'CREATE', key, groupName, id, ...args])) === 1;
    } catch (err) {
      const streamType = mkStream === 'MKSTREAM' ? 'with MKSTREAM' : 'without MKSTREAM';
      this.logger.error(`x-group-error ${streamType} for key: ${key} and group: ${groupName}`, err);
      throw err;
    }
  }

  async xadd(key: string, id: string, ...args: any[]): Promise<string | RedisMultiType> {
    let multi: RedisMultiType;
    if (typeof args[args.length - 1] !== 'string') {
      multi = args.pop() as RedisMultiType;
    }
    try {
      return await (multi || this.redisClient).XADD(key, id, { [args[0]]: args[1] });
    } catch (err) {
      this.logger.error(`Error publishing 'xadd'; key: ${key}`, err);
      throw err;
    }
  }

  async xreadgroup(
    command: 'GROUP',
    groupName: string,
    consumerName: string,
    blockOption: 'BLOCK'|'COUNT',
    blockTime: number|string,
    streamsOption: 'STREAMS',
    streamName: string,
    id: string
  ): Promise<string[][][] | null> {
    try {
      return await this.redisClient.sendCommand(['XREADGROUP', command, groupName, consumerName, blockOption, blockTime.toString(), streamsOption, streamName, id]);
    } catch (err) {
      this.logger.error(`Error in reading data from group: ${groupName} in stream: ${streamName}`, err);
      throw err;
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
      try {
        return await this.redisClient.sendCommand(['XPENDING', ...args]);
      } catch (err) {
        this.logger.error('err, args', err, args);
      }
    } catch (err) {
      this.logger.error(`Error in retrieving pending messages for group: ${group} in key: ${key}`, err);
      throw err;
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
    } catch (err) {
      this.logger.error(`Error in claiming message with id: ${id} in group: ${group} for key: ${key}`, err);
      throw err;
    }
  }

  async xack(key: string, group: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      if (multi) {
        multi.XACK(key, group, id);
        return multi;
      } else {
        return await this.redisClient.XACK(key, group, id);
      }
    } catch (err) {
      this.logger.error(`Error in acknowledging messages in group: ${group} for key: ${key}`, err);
      throw err;
    }
  }

  async xdel(key: string, id: string, multi? : RedisMultiType): Promise<number|RedisMultiType> {
    try {
      if (multi) {
        multi.XDEL(key, id);
        return multi;
      } else {
        return await this.redisClient.XDEL(key, id);
      }
    } catch (err) {
      this.logger.error(`Error in deleting messages with ids: ${id} for key: ${key}`, err);
      throw err;
    }
  }
}

export { RedisStreamService };
