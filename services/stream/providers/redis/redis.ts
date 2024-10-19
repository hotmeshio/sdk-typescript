import {
  KeyService,
  KeyStoreParams,
  KeyType,
  HMNS,
} from '../../../../modules/key';
import { ILogger } from '../../../logger';
import { StreamService } from '../../index';
import {
  RedisRedisClientType as RedisClientType,
  RedisRedisMultiType as RedisMultiType,
} from '../../../../types/redis';
import { ReclaimedMessageType } from '../../../../types/stream';

class RedisStreamService extends StreamService<
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
    return this.streamClient.multi() as unknown as RedisMultiType;
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
      return await this.streamClient.sendCommand([
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

  async getPendingMessages(
    key: string,
    group: string,
    count?: number,
    consumer?: string,
  ): Promise<
    | [string, string, number, [string, number][]][]
    | [string, string, number, number]
  > {
    try {
      const start = '-';
      const end = '+';
      const args = [key, group];
      if (start) args.push(start);
      if (end) args.push(end);
      if (count !== undefined) args.push(count.toString());
      if (consumer) args.push(consumer);
      try {
        return await this.streamClient.sendCommand(['XPENDING', ...args]);
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

  async claimMessage(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<ReclaimedMessageType> {
    try {
      return (await this.streamClient.sendCommand([
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
      if (multi) {
        multi.XACK(key, group, id);
        return multi;
      } else {
        return await this.streamClient.XACK(key, group, id);
      }
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
      if (multi) {
        multi.XDEL(key, id);
        return multi;
      } else {
        return await this.streamClient.XDEL(key, id);
      }
    } catch (error) {
      this.logger.error(
        `Error in deleting messages with ids: ${id} for key: ${key}`,
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
        (await this.storeClient.sendCommand([
          'XGROUP',
          'CREATE',
          key,
          groupName,
          '$',
          'MKSTREAM',
        ])) === 1
      );
    } catch (error) {
      const streamType = 'with MKSTREAM';
      this.logger.debug(
        `x-group-error ${streamType} for key: ${key} and group: ${groupName}`,
        { ...error },
      );
      throw error;
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
      return await (multi || this.storeClient).XADD(key, id, {
        [messageId]: messageValue,
      });
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
      if (multi) {
        multi.XLEN(key);
        return multi;
      } else {
        return await this.storeClient.XLEN(key);
      }
    } catch (error) {
      this.logger.error(`Error getting stream depth: ${key}`, { ...error });
      throw error;
    }
  }
}

export { RedisStreamService };
