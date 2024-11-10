import { Pool, PoolConfig, PoolClient, QueryResult, QueryConfig } from 'pg';

export interface PostgresStreamOptions extends PoolConfig {
  schema?: string;
  maxRetries?: number;
  retryDelay?: number;
  streamTablePrefix?: string;
  consumerTablePrefix?: string;
}

export type PostgresClientOptions = PoolConfig;
export type PostgresClientType = Pool;
export type PostgresClassType = typeof Pool;
export type PostgresPoolClientType = PoolClient;
export type PostgresQueryResultType = QueryResult;
export type PostgresQueryConfigType = QueryConfig;

export interface PostgresStreamMessage {
  id: string;
  stream: string;
  message: any;
  created_at: Date;
  sequence?: number;
}

export interface PostgresConsumerGroup {
  stream: string;
  group_name: string;
  last_message_id: string;
  created_at: Date;
  updated_at: Date;
}

export interface PostgresPendingMessage {
  stream: string;
  group_name: string;
  consumer_name: string;
  message_id: string;
  delivered_at: Date;
  delivery_count: number;
}

export interface PostgresTransaction {
  client: PoolClient;
  queryBuffer: {
    text: string;
    values: any[];
  }[];
  begin(): Promise<void>;
  query(text: string, values?: any[]): Promise<PostgresQueryResultType>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  release(): void;
}
