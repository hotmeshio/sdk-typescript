// /app/types/postgres.ts
export interface PostgresClientOptions {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  max?: number;
  idleTimeoutMillis?: number;
  // Add any other options you might need
}

export type PostgresJobEnumType =
  | 'status'
  | 'jdata'
  | 'adata'
  | 'udata'
  | 'jmark'
  | 'hmark'
  | 'other';

export type PostgresClassType = {
  new (options: PostgresClientOptions): PostgresClientType;
  connect: (options: PostgresClientOptions) => Promise<PostgresClientType>;
};

export interface PostgresClientType {
  query: (text: string, values?: any[]) => Promise<PostgresQueryResultType>;
  end: () => Promise<void>;
  // Include other methods if necessary
}

export interface PostgresPoolClientType {
  query: (text: string, values?: any[]) => Promise<PostgresQueryResultType>;
  release: () => void;
  // Include other methods if necessary
}

export interface PostgresQueryResultType {
  rows: any[];
  rowCount: number;
  // Include other properties if necessary
}

export interface PostgresQueryConfigType {
  text: string;
  values?: any[];
}

export interface PostgresStreamOptions extends PostgresClientOptions {
  schema?: string;
  maxRetries?: number;
  retryDelay?: number;
  streamTablePrefix?: string;
  consumerTablePrefix?: string;
}

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
  client: PostgresPoolClientType;
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
