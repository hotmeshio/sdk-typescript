import { ILogger } from '../../../logger';
import { StreamService } from '../../index';
import { IORedisClientType, IORedisMultiType } from '../../../../types/redis';
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

class IORedisStreamService extends StreamService<
  IORedisClientType,
  IORedisMultiType
> {
  constructor(
    streamClient: IORedisClientType,
    storeClient: IORedisClientType,
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
    return this.streamClient.multi() as unknown as ProviderTransaction;
  }

  // Core streaming operations
  async createStream(streamName: string): Promise<boolean> {
    try {
      // streams are created when you add messages.
      // To create an empty stream, we can add and delete a dummy message.
      const dummyId = await this.streamClient.xadd(
        streamName,
        '*',
        'field',
        'value',
      );
      await this.streamClient.xdel(streamName, dummyId);
      return true;
    } catch (error) {
      this.logger.error(`Error creating stream ${streamName}`, { error });
      throw error;
    }
  }

  async deleteStream(streamName: string): Promise<boolean> {
    try {
      const result = await this.streamClient.del(streamName);
      return result > 0;
    } catch (error) {
      this.logger.error(`Error deleting stream ${streamName}`, { error });
      throw error;
    }
  }

  // Consumer group operations
  async createConsumerGroup(key: string, groupName: string): Promise<boolean> {
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

  async deleteConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    try {
      const result = await this.streamClient.xgroup(
        'DESTROY',
        streamName,
        groupName,
      );
      return result === 1;
    } catch (error) {
      this.logger.error(
        `Error deleting consumer group ${groupName} for stream ${streamName}`,
        { error },
      );
      throw error;
    }
  }

  // Message operations
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
        response = await (multi || this.storeClient).xadd(
          streamName,
          '*',
          'message',
          message,
        );
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
    try {
      const result = await this.streamClient.xreadgroup(
        'GROUP',
        groupName,
        consumerName,
        'BLOCK',
        options?.blockTimeout ?? HMSH_BLOCK_TIME_MS,
        'STREAMS',
        streamName,
        '>',
      );

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
    this.acknowledgeMessages(stream, group, ids, { multi });
    this.deleteMessages(stream, group, ids, { multi });
    await multi.exec();
    return ids.length;
  }

  async acknowledgeMessages(
    stream: string,
    group: string,
    ids: string[],
    options?: StringAnyType,
  ): Promise<number | IORedisMultiType> {
    try {
      if (options?.multi) {
        options.multi.xack(stream, group, ...ids);
        return options.multi;
      } else {
        return await this.streamClient.xack(stream, group, ...ids);
      }
    } catch (error) {
      this.logger.error(
        `Error in acknowledging messages: [${ids}] in group: ${group} for key: ${stream}`,
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
  ): Promise<number | IORedisMultiType> {
    try {
      if (options?.multi) {
        options.multi.xdel(stream, ...ids);
        return options.multi;
      } else {
        return await this.streamClient.xdel(stream, ...ids);
      }
    } catch (error) {
      this.logger.error(
        `Error in deleting messages: ${ids} for key: ${stream}`,
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
    | unknown[]
  > {
    const start = '-';
    const end = '+';
    try {
      const args = [stream, group];
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
        this.logger.error('err, args', { error }, args);
      }
    } catch (error) {
      this.logger.error(
        `Error in retrieving pending messages for [stream ${stream}], [group ${group}]`,
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
    streamName: string,
    groupName: string,
    consumerName: string,
    minIdleTime: number,
    messageId: string,
    ...args: string[]
  ): Promise<StreamMessage> {
    try {
      const message = (await this.streamClient.xclaim(
        streamName,
        groupName,
        consumerName,
        minIdleTime,
        messageId,
        ...args,
      )) as unknown as ReclaimedMessageType;
      return {
        id: message[0][0],
        data: parseStreamMessage(message[0][1][1]),
      };
    } catch (error) {
      this.logger.error(
        `Error in claiming message with id: ${messageId} in group: ${groupName} for key: ${streamName}`,
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
    options?: { multi: IORedisMultiType },
  ): Promise<number> {
    try {
      if (options?.multi) {
        options.multi.xlen(streamName);
        return 0;
      }
      const length = await this.streamClient.xlen(streamName);
      return length;
    } catch (error) {
      this.logger.error(`Error getting depth for ${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    const multi = this.storeClient.multi();
    const uniqueStreams = new Map<string, number>(); // to store unique streams

    // Add unique streams to the multi command
    streamNames.forEach((profile) => {
      if (!uniqueStreams.has(profile.stream)) {
        uniqueStreams.set(profile.stream, -1); // initialize depth to -1 as a placeholder
        this.getStreamDepth(profile.stream, { multi });
      }
    });

    // Execute all commands
    const streamDepthResults = (await multi.exec()) as [
      [err: any, depth: number],
    ];

    // Update the uniqueStreams map with the actual depths from multi.exec() results
    Array.from(uniqueStreams.keys()).forEach((stream, idx) => {
      uniqueStreams.set(stream, streamDepthResults[idx][1]);
    });

    // Map back to the original `streamNames` array with correct depths
    const updatedNames = streamNames.map((profile) => {
      return {
        stream: profile.stream,
        depth: uniqueStreams.get(profile.stream) || 0,
      };
    });

    return updatedNames;
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

export { IORedisStreamService };
