import { KeyStoreParams, KeyType } from '../../modules/key';
import { ReclaimedMessageType } from '../../types/stream';
import { ILogger } from '../logger';

abstract class StreamService<T, U> {
  redisClient: T;
  namespace: string;
  logger: ILogger;
  appId: string;

  constructor(redisClient: T) {
    this.redisClient = redisClient;
  }

  abstract init(namespace: string, appId: string, logger: ILogger): Promise<void>;
  abstract getMulti(): U;
  abstract mintKey(type: KeyType, params: KeyStoreParams): string;
  abstract xgroup(
    command: 'CREATE',
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM'): Promise<boolean>;
  abstract xadd(
    key: string,
    id: string,
    messageId: string,
    messageValue: string,
    multi?: U): Promise<string | U>;
  abstract xreadgroup(
    command: 'GROUP',
    groupName: string,
    consumerName: string,
    blockOption: 'BLOCK'|'COUNT',
    blockTime: number|string,
    streamsOption: 'STREAMS',
    streamName: string,
    id: string): Promise<string[][][] | null | unknown[]>;
  abstract xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string): Promise<[string, string, number, [string, number][]][] | [string, string, number, number] | unknown[]>;
  abstract xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]): Promise<ReclaimedMessageType>;
  abstract xack(key: string, group: string, id: string, multi?: U): Promise<number|U>;
  abstract xdel(key: string, id: string, multi?: U): Promise<number|U>;
  abstract xlen(key: string, multi?: U): Promise<number|U>;
}

export { StreamService };
