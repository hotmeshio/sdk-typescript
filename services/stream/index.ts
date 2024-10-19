import { ReclaimedMessageType } from '../../types/stream';
import { ILogger } from '../logger';

abstract class StreamService<Client, MultiClient> {
  streamClient: Client;
  storeClient: Client;
  namespace: string;
  logger: ILogger;
  appId: string;

  constructor(streamClient: Client, storeClient: Client) {
    this.streamClient = streamClient;
    this.storeClient = storeClient;
  }

  abstract init(namespace: string, appId: string, logger: ILogger): Promise<void>;
  abstract consumeMessages(//xreadgroup
    groupName: string,
    consumerName: string,
    blockTime: number | string,
    streamName: string,
    //onMessage: (message: any) => Promise<void>
  ): Promise<string[][][] | null>;
  abstract ackAndDelete(
    key: string,
    group: string,
    id: string,
  ): Promise<number | MultiClient>;
  abstract acknowledgeMessage(
    key: string,
    group: string,
    id: string,
    multi?: MultiClient,
  ): Promise<number | MultiClient>;
  abstract deleteMessage(
    key: string,
    id: string,
    multi?: MultiClient,
  ): Promise<number | MultiClient>;
  abstract getPendingMessages(
    stream: string,
    groupName: string,
    count?: number,
    consumer?: string,
  ): Promise<any[]>;
  abstract claimMessage(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<ReclaimedMessageType>;
  abstract createConsumerGroup(
    key: string,
    groupName: string
  ): Promise<boolean>;
  abstract publishMessage(
    key: string,
    id: string,
    messageId: string,
    messageValue: string,
    multi?: MultiClient
  ): Promise<string | MultiClient>;
  abstract getMessageDepth(
    key: string,
    multi?: MultiClient,
  ): Promise<number | MultiClient>;
}

export { StreamService };
