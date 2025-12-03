import { ILogger } from '../../../logger';
import { StreamService } from '../../index';
import {
  RedisRedisClientType,
  RedisRedisMultiType,
} from '../../../../types/redis';
import {
  PublishMessageConfig,
  ReclaimedMessageType,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../../../types/stream';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import { isStreamMessage, parseStreamMessage } from '../../../../modules/utils';
import { KeyService, KeyType } from '../../../../modules/key';
import { HMSH_BLOCK_TIME_MS } from '../../../../modules/enums';
import { ProviderTransaction } from '../../../../types/provider';

class RedisStreamService extends StreamService<
  RedisRedisClientType,
  RedisRedisMultiType
> {
  constructor(
    streamClient: RedisRedisClientType,
    storeClient: RedisRedisClientType,
    config: StreamConfig = {},
  ) {
    super(streamClient, storeClient, config);
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.logger = logger;
    this.appId = appId;
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, {
      ...params,
      appId: this.appId,
    });
  }

  transact(): ProviderTransaction {
    return this.streamClient.multi() as unknown as RedisRedisMultiType;
  }

  // Core streaming operations
  async createStream(streamName: string): Promise<boolean> {
    try {
      // streams are created when you add messages.
      // To create an empty stream, we can add and delete a dummy message.
      const dummyId = await this.streamClient.XADD(streamName, '*', {
        field: 'value',
      });
      await this.streamClient.XDEL(streamName, dummyId);
      return true;
    } catch (error) {
      this.logger.error(`Error creating stream ${streamName}`, { error });
      throw error;
    }
  }

  async deleteStream(streamName: string): Promise<boolean> {
    try {
      const result = await this.streamClient.DEL(streamName);
      return result > 0;
    } catch (error) {
      this.logger.error(`Error deleting stream ${streamName}`, { error });
      throw error;
    }
  }

  // Consumer group operations
  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    try {
      const result = (await this.storeClient.sendCommand([
        'XGROUP',
        'CREATE',
        streamName,
        groupName,
        '$',
        'MKSTREAM',
      ])) as unknown as string;
      return result === 'OK';
    } catch (error) {
      const streamType = 'with MKSTREAM';
      this.logger.debug(
        `x-group-error ${streamType} for key: ${streamName} and group: ${groupName}`,
        { error },
      );
      throw error;
    }
  }

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    try {
      const result = await this.streamClient.xGroupDestroy(
        streamName,
        groupName,
      );
      return result;
    } catch (error) {
      this.logger.error(
        `Error deleting consumer group ${groupName} for stream ${streamName}`,
        { error },
      );
      throw error;
    }
  }

  async publishMessages(
    streamName: string,
    messages: string[],
    options?: PublishMessageConfig,
  ): Promise<string[]> {
    try {
      const multi =
        options?.transaction ||
        (messages.length > 1 && this.storeClient.multi());
      let response: string | string[];
      for (const message of messages) {
        response = await (multi || this.storeClient).XADD(streamName, '*', {
          message: message,
        });
      }

      if (multi && !options?.transaction) {
        //only exec if we created the multi;
        //otherwise caller is responsible
        return (await multi.exec()).map(
          (result: [string, string]) => result[1],
        );
      } else {
        return [response as string];
      }
    } catch (error) {
      this.logger.error(`ioredis-xadd-error key: ${streamName}`, { error });
      throw error;
    }
  }

  async consumeMessages(
    streamName: string,
    groupName: string,
    consumerName: string,
    options?: {
      batchSize?: number;
      blockTimeout?: number;
    },
  ): Promise<StreamMessage[]> {
    const command = 'GROUP';
    const blockOption = 'BLOCK';
    const streamsOption = 'STREAMS';
    const id = '>';
    try {
      const result = await this.streamClient.sendCommand([
        'XREADGROUP',
        command,
        groupName,
        consumerName,
        blockOption,
        options?.blockTimeout?.toString() ?? HMSH_BLOCK_TIME_MS.toString(),
        streamsOption,
        streamName,
        id,
      ]);

      const response = [];
      if (isStreamMessage(result)) {
        const [[, messages]] = result;
        for (const [id, message] of messages) {
          response.push({
            id,
            data: parseStreamMessage(message[1]),
          });
        }
      } else {
        return [];
      }
      return response;
    } catch (error) {
      this.logger.error(`Error consuming messages from ${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async ackAndDelete(
    stream: string,
    group: string,
    ids: string[],
  ): Promise<number> {
    const multi = this.storeClient.multi();
    await this.acknowledgeMessages(stream, group, ids, { multi });
    await this.deleteMessages(stream, group, ids, { multi });
    await multi.exec();
    return ids.length;
  }

  async acknowledgeMessages(
    stream: string,
    group: string,
    ids: string[],
    options?: StringAnyType,
  ): Promise<number | RedisRedisMultiType> {
    try {
      if (options?.multi) {
        options.multi.XACK(stream, group, ids);
        return options.multi;
      } else {
        return await this.streamClient.XACK(stream, group, ...ids);
      }
    } catch (error) {
      this.logger.error(
        `Error in acknowledging messages in group: ${group} for key: ${stream}`,
        { error },
      );
      throw error;
    }
  }

  async deleteMessages(
    stream: string,
    group: string,
    ids: string[],
    options?: StringAnyType,
  ): Promise<number | RedisRedisMultiType> {
    try {
      if (options?.multi) {
        options.multi.xDel(stream, ids);
        return options.multi;
      } else {
        return await this.streamClient.XDEL(stream, ...ids);
      }
    } catch (error) {
      this.logger.error(
        `Error in deleting messages with ids: ${ids.join(
          ',',
        )} for key: ${stream}`,
        { error },
      );
      throw error;
    }
  }

  async getPendingMessages(
    stream: string,
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
      const args = [stream, group];
      if (start) args.push(start);
      if (end) args.push(end);
      if (count !== undefined) args.push(count.toString());
      if (consumer) args.push(consumer);
      try {
        return await this.streamClient.sendCommand(['XPENDING', ...args]);
      } catch (error) {
        this.logger.error('error, args', { error }, args);
      }
    } catch (error) {
      this.logger.error(
        `Error retrieving pending messages for group: ${group} in key: ${stream}`,
        { error },
      );
      throw error;
    }
  }

  // Retry Method
  async retryMessages(
    streamName: string,
    groupName: string,
    options?: {
      consumerName?: string;
      minIdleTime?: number;
      messageIds?: string[];
      delay?: number;
      maxRetries?: number;
      limit?: number;
    },
  ): Promise<StreamMessage[]> {
    let pendingMessages = [];
    const pendingMessagesInfo = await this.getPendingMessages(
      streamName,
      groupName,
      options?.limit,
    ); //[[ '1688768134881-0', 'testConsumer1', 1017, 1 ]]
    for (const pendingMessageInfo of pendingMessagesInfo) {
      if (Array.isArray(pendingMessageInfo)) {
        const [id, , elapsedTimeMs, deliveryCount] = pendingMessageInfo;
        if (elapsedTimeMs > options?.minIdleTime) {
          const reclaimedMessage = await this.claimMessage(
            streamName,
            groupName,
            options?.consumerName,
            options?.minIdleTime,
            id,
          );
          pendingMessages = pendingMessages.concat(reclaimedMessage);
        }
      }
    }
    return pendingMessages;
  }

  async claimMessage(
    stream: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<StreamMessage> {
    try {
      const message = (await this.streamClient.sendCommand([
        'XCLAIM',
        stream,
        group,
        consumer,
        minIdleTime.toString(),
        id,
        ...args,
      ])) as unknown as ReclaimedMessageType;
      return {
        id: message[0][0],
        data: parseStreamMessage(message[0][1][1]),
      };
    } catch (error) {
      this.logger.error(
        `Error in claiming message with id: ${id} in group: ${group} for key: ${stream}`,
        { error },
      );
      throw error;
    }
  }

  async getStreamStats(streamName: string): Promise<StreamStats> {
    return {
      messageCount: await this.getStreamDepth(streamName),
    };
  }

  async getStreamDepth(
    streamName: string,
    options?: { multi: RedisRedisMultiType },
  ): Promise<number> {
    try {
      if (options?.multi) {
        options.multi.XLEN(streamName);
        return 0;
      }
      const length = await this.streamClient.XLEN(streamName);
      return length;
    } catch (error) {
      this.logger.error(`Error getting depth for ${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    const multi = this.storeClient.multi() as RedisRedisMultiType;
    const uniqueStreams = new Map<string, number>(); // Use a Map to track unique streams

    // Add unique streams to the multi command
    streamNames.forEach((profile) => {
      if (!uniqueStreams.has(profile.stream)) {
        uniqueStreams.set(profile.stream, -1); // Initialize depth to -1 as a placeholder
        this.getStreamDepth(profile.stream, { multi: multi });
      }
    });

    // Execute all commands in multi (ioredis strips null fields)
    const streamDepthResults = (await multi.exec()) as number[];
    // Update the uniqueStreams map with actual depths from multi.exec() results
    Array.from(uniqueStreams.keys()).forEach((stream, idx) => {
      uniqueStreams.set(stream, streamDepthResults[idx]);
    });

    // Map back to the original `streamNames` array with correct depths
    return streamNames.map((profile) => {
      return {
        stream: profile.stream,
        depth: uniqueStreams.get(profile.stream) || 0,
      };
    });
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    //no-op for now
    return 0;
  }

  // Provider-specific helpers
  getProviderSpecificFeatures() {
    return {
      supportsBatching: true,
      supportsDeadLetterQueue: false,
      supportsOrdering: true,
      supportsTrimming: true,
      supportsRetry: true,
      supportsNotifications: false, // Redis doesn't support LISTEN/NOTIFY like PostgreSQL
      maxMessageSize: 512 * 1024 * 1024, // 512 MB
      maxBatchSize: 1000,
    };
  }
}

export { RedisStreamService };
