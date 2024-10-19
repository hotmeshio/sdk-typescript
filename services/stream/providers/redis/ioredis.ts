import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import { StreamService } from '../../index';
import {
  IORedisClientType as RedisClientType,
  IORedisMultiType as RedisMultiType,
} from '../../../../types/redis';
import { ReclaimedMessageType } from '../../../../types/stream';

class IORedisStreamService extends StreamService<
  RedisClientType,
  RedisMultiType
  > {

  constructor(streamClient: RedisClientType, storeClient: RedisClientType) {
    super(streamClient, storeClient);
  }

  async init(namespace = HMNS, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.logger = logger;
    this.appId = appId;
  }

  getMulti(): RedisMultiType {
    return this.streamClient.multi();
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, params);
  }

  async consumeMessages(
    groupName: string,
    consumerName: string,
    blockTime: number | string, //should be externalized
    streamName: string,
  ): Promise<string[][][] | null> {
    const command = 'GROUP';
    const blockOption = 'BLOCK';
    const streamsOption = 'STREAMS';
    const id = '>';
    try {
      //@ts-ignore
      return await this.streamClient.xreadgroup(
        command,
        groupName,
        consumerName,
        // @ts-ignore
        blockOption,
        blockTime,
        streamsOption,
        streamName,
        id,
      );
    } catch (error) {
      this.logger.error(
        `Error reading stream data [Stream ${streamName}] [Group ${groupName}]`,
        { ...error },
      );
      throw error;
    }
  }

  async getPendingMessages(
    key: string,
    group: string,
    count?: number,
    consumer?: string,
  ): Promise<
    | [string, string, number, [string, number][]][]
    | [string, string, number, number]
    | unknown[]
  > {
    const start = '-';
    const end = '+';
    try {
      const args = [key, group];
      if (start) args.push(start);
      if (end) args.push(end);
      if (count !== undefined) args.push(count.toString());
      if (consumer) args.push(consumer);
      try {
        return (await this.streamClient.call('XPENDING', ...args)) as [
          string,
          string,
          number,
          number,
        ][];
      } catch (error) {
        this.logger.error('err, args', { ...error }, args);
      }
    } catch (error) {
      this.logger.error(
        `Error in retrieving pending messages for [stream ${key}], [group ${group}]`,
        { ...error },
      );
      throw error;
    }
  }

  async claimMessage(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<ReclaimedMessageType> {
    try {
      return (await this.streamClient.xclaim(
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

  async ackAndDelete(key: string, group: string, id: string): Promise<number | RedisMultiType> {
    const multi = this.getMulti();
    this.acknowledgeMessage(key, group, id, multi);
    this.deleteMessage(key, id, multi);
    return await multi.exec() as unknown as number;
  }

  async acknowledgeMessage(
    key: string,
    group: string,
    id: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      return await (multi || this.streamClient).xack(key, group, id);
    } catch (error) {
      this.logger.error(
        `Error in acknowledging messages in group: ${group} for key: ${key}`,
        { ...error },
      );
      throw error;
    }
  }

  async deleteMessage(
    key: string,
    id: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      return await (multi || this.streamClient).xdel(key, id);
    } catch (error) {
      this.logger.error(
        `Error in deleting messages with id: ${id} for key: ${key}`,
        { ...error },
      );
      throw error;
    }
  }

  async createConsumerGroup(
    key: string,
    groupName: string,
  ): Promise<boolean> {
    try {
      return (
        (await this.storeClient.xgroup(
          'CREATE',
          key,
          groupName,
          '$',
          'MKSTREAM',
        )) === 'OK'
      );
    } catch (err) {
      this.logger.debug('stream-mkstream-caught', { key, group: groupName });
      throw err;
    }
  }

  async publishMessage(
    key: string,
    id: string,
    messageId: string,
    messageValue: string,
    multi?: RedisMultiType,
  ): Promise<string | RedisMultiType> {
    try {
      return await (multi || this.storeClient).xadd(
        key,
        id,
        messageId,
        messageValue,
      );
    } catch (error) {
      this.logger.error(`Error publishingMessage [xadd]; key: ${key}`, { ...error });
      throw error;
    }
  }

  async getMessageDepth(
    key: string,
    multi?: RedisMultiType,
  ): Promise<number | RedisMultiType> {
    try {
      return await (multi || this.storeClient).xlen(key);
    } catch (error) {
      this.logger.error(`Error getting stream depth: ${key}`, { ...error });
      throw error;
    }
  }
}

export { IORedisStreamService };
