// /app/services/stream/providers/nats/nats.ts
import { ILogger } from '../../../logger';
import { StreamService } from '../../index';
import {
  PublishMessageConfig,
  StreamConfig,
  StreamMessage,
  StreamStats,
} from '../../../../types/stream';
import { KeyStoreParams, StringAnyType } from '../../../../types';
import {
  NatsJetStreamType,
  NatsJetStreamManager as NatsJetStreamManagerType,
  NatsStreamConfigType,
  NatsConsumerConfigType,
  NatsPubAckType,
  NatsRetentionPolicyWorkqueueType,
  NatsClientType,
  NatsStorageMemoryType,
  NatsAckPolicyExplicitType,
} from '../../../../types/nats';
import { KeyService, KeyType } from '../../../../modules/key';
import {
  ProviderClient,
  ProviderTransaction,
} from '../../../../types/provider';
import { HMSH_BLOCK_TIME_MS } from '../../../../modules/enums';
import { parseStreamMessage } from '../../../../modules/utils';

class NatsStreamService extends StreamService<NatsClientType, NatsPubAckType> {
  jetstream: NatsJetStreamType;
  jsm: NatsJetStreamManagerType;

  constructor(
    streamClient: NatsClientType,
    storeClient: ProviderClient,
    config: StreamConfig = {},
  ) {
    super(streamClient, storeClient, config);
    this.jetstream = streamClient.jetstream();
  }

  async init(namespace: string, appId: string, logger: ILogger): Promise<void> {
    this.namespace = namespace;
    this.logger = logger;
    this.appId = appId;
    this.jsm = await this.jetstream.jetstreamManager();
  }

  mintKey(type: KeyType, params: KeyStoreParams): string {
    if (!this.namespace) throw new Error('namespace not set');
    return KeyService.mintKey(this.namespace, type, {
      ...params,
      appId: this.appId,
    });
  }

  transact(): ProviderTransaction {
    return {} as ProviderTransaction;
  }

  async createStream(streamName: string): Promise<boolean> {
    try {
      const config: NatsStreamConfigType = {
        name: streamName,
        subjects: [`${streamName}.*`],
        retention: 'workqueue' as NatsRetentionPolicyWorkqueueType,
        storage: 'memory' as NatsStorageMemoryType,
        num_replicas: 1,
      };
      await this.jsm.streams.add(config);
      return true;
    } catch (error) {
      this.logger.error(`Error creating stream ${streamName}`, { error });
      throw error;
    }
  }

  async deleteStream(streamName: string): Promise<boolean> {
    try {
      await this.jsm.streams.delete(streamName);
      return true;
    } catch (error) {
      this.logger.error(`Error deleting stream ${streamName}`, { error });
      throw error;
    }
  }

  async createConsumerGroup(
    streamName: string,
    groupName: string,
  ): Promise<boolean> {
    try {
      const consumerConfig: NatsConsumerConfigType = {
        durable_name: groupName,
        deliver_group: groupName,
        ack_policy: 'explicit' as NatsAckPolicyExplicitType,
        ack_wait: 30 * 1000,
        max_deliver: 10,
      };
      await this.jsm.consumers.add(streamName, consumerConfig);
      return true;
    } catch (error) {
      this.logger.error(
        `Error creating consumer group ${groupName} for stream ${streamName}`,
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
      await this.jsm.consumers.delete(streamName, groupName);
      return true;
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
  ): Promise<string[] | ProviderTransaction> {
    try {
      const publishPromises = messages.map(async (message) => {
        const subject = `${streamName}.message`;
        const ack = await this.jetstream.publish(subject, Buffer.from(message));
        return ack;
      });
      const acks = await Promise.all(publishPromises);
      return acks.map((ack) => ack.seq.toString());
    } catch (error) {
      this.logger.error(`Error publishing messages to ${streamName}`, {
        error,
      });
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
      autoAck?: boolean;
    },
  ): Promise<StreamMessage[]> {
    try {
      const consumer = await this.jetstream.consumers.get(
        streamName,
        groupName,
      );
      const messages: StreamMessage[] = [];

      // Construct FetchOptions with batch size and timeout
      const fetchOptions = {
        batch: options?.batchSize || 1,
        expires: options?.blockTimeout || HMSH_BLOCK_TIME_MS,
      };

      // Fetch a specified batch of messages using the FetchOptions object
      const fetchedMessages = await consumer.fetch(fetchOptions);

      for await (const msg of fetchedMessages) {
        messages.push({
          id: msg.seq.toString(),
          data: parseStreamMessage(msg.string()),
        });

        if (options?.autoAck) {
          await msg.ack();
        }
      }

      return messages;
    } catch (error) {
      this.logger.error(`Error consuming messages from ${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async ackAndDelete(
    streamName: string,
    groupName: string,
    messageIds: string[],
  ): Promise<number> {
    try {
      await this.acknowledgeMessages(streamName, groupName, messageIds);
      return messageIds.length;
    } catch (error) {
      this.logger.error(`Error in ack and delete for stream ${streamName}`, {
        error,
      });
      throw error;
    }
  }

  async acknowledgeMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    //no-op
    return messageIds.length;
  }

  async deleteMessages(
    streamName: string,
    groupName: string,
    messageIds: string[],
    options?: StringAnyType,
  ): Promise<number> {
    try {
      await Promise.all(
        messageIds.map((id) =>
          this.jsm.streams.deleteMessage(streamName, parseInt(id)),
        ),
      );
      return messageIds.length;
    } catch (error) {
      this.logger.error(`Error deleting messages from ${streamName}`, {
        error,
      });
      throw error;
    }
  }

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
    return [];
  }

  async getStreamStats(streamName: string): Promise<StreamStats> {
    try {
      const info = await this.jsm.streams.info(streamName);
      return {
        messageCount: info.state.messages,
      };
    } catch (error) {
      this.logger.error(`Error getting stats for ${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepth(streamName: string): Promise<number> {
    try {
      const info = await this.jsm.streams.info(streamName);
      return info.state.messages;
    } catch (error) {
      this.logger.error(`Error getting depth for ${streamName}`, { error });
      throw error;
    }
  }

  async getStreamDepths(
    streamNames: { stream: string }[],
  ): Promise<{ stream: string; depth: number }[]> {
    try {
      const results = await Promise.all(
        streamNames.map(async ({ stream }) => ({
          stream,
          depth: await this.getStreamDepth(stream),
        })),
      );
      return results;
    } catch (error) {
      this.logger.error('Error getting multiple stream depths', { error });
      throw error;
    }
  }

  async trimStream(
    streamName: string,
    options: {
      maxLen?: number;
      maxAge?: number;
      exactLimit?: boolean;
    },
  ): Promise<number> {
    try {
      // Retrieve the current stream info
      const streamInfo = await this.jsm.streams.info(streamName);
      const config = { ...streamInfo.config }; // Clone to avoid direct mutation

      // Apply new limits based on options
      if (options.maxLen !== undefined) {
        config.max_msgs = options.maxLen;
      }
      if (options.maxAge !== undefined) {
        config.max_age = options.maxAge * 1e9; // Convert maxAge to nanoseconds
      }

      // Update the stream with the modified config
      await this.jsm.streams.update(streamName, config);
      return 0; // Trimming is applied automatically based on updated config
    } catch (error) {
      this.logger.error(`Error trimming stream ${streamName}`, { error });
      throw error;
    }
  }

  getProviderSpecificFeatures() {
    return {
      supportsBatching: true,
      supportsDeadLetterQueue: true,
      supportsOrdering: true,
      supportsTrimming: true,
      supportsRetry: false,
      maxMessageSize: 1024 * 1024,
      maxBatchSize: 256,
    };
  }
}

export { NatsStreamService };
