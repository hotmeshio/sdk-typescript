import { KeyStoreParams } from '../modules/key';

import { StringAnyType } from './serializer';

/**
 * Generic type for provider class
 */
export interface ProviderClass {
  [key: string]: any;
}

/**
 * Generic type for provider options
 */
export interface ProviderOptions {
  [key: string]: any;
}

export type Providers = 'redis' | 'nats' | 'postgres' | 'ioredis';

/**
 * A provider transaction is a set of operations that are executed
 * atomically by the provider. The transaction is created by calling
 * the `transact` method on the provider. The transaction object
 * contains methods specific to the provider allowing it to optionally
 * choose to execute a single command or collect all commands and
 * execute as a single transaction.
 */
export interface ProviderTransaction {
  exec(): Promise<any>;

  // Allows callable properties while avoiding conflicts with ProviderTransaction instances
  [key: string]: ((...args: any[]) => Promise<any>) | undefined | any;
}

/**
 * A provider native client is the raw client object provided by the
 * connecter service. This object is passed to the ProviderClient
 * (which wraps it), providing a standardized interface for all
 * providers.
 */
export interface ProviderNativeClient {
  [key: string]: any;
}

/**
 * Wrapped provider native client object that standardizes the
 * interface for all providers.
 */
export interface ProviderClient {
  /** The provider-specific transaction object */
  transact(): ProviderTransaction;

  /** Mint a provider-specific key */
  mintKey(type: KeyType, params: KeyStoreParams): string;

  /** The provider-specific client object */
  [key: string]: any;
}

/**
 * an array of outputs generic to all providers
 * e.g., [3, 2, '0']
 */
export type TransactionResultList = (string | number)[]; // e.g., [3, 2, '0']

export type ProviderConfig = {
  class: any;
  options: StringAnyType;
  /* 'redis' (Class) | 'ioredis' (Class) | 'postgres' (Client module) | 'postgres.pool' | 'postgres.poolclient', 'nats' */
  provider?: string;
  /**
   * If provided and if true, the engine router instance will
   * be initialized as a readonly instance and will not consume
   * messages from the stream channel. An engine in readonly mode
   * can still read/write to the store and can still pub/sub events.
   */
  readonly?: boolean;
};

export type ProvidersConfig = {
  sub: ProviderConfig;
  store: ProviderConfig;
  stream: ProviderConfig;
  pub?: ProviderConfig;
  search?: ProviderConfig;
  /**
   * If provided and if true, the engine router instance will
   * be initialized as a readonly instance and will not consume
   * messages from the stream channel. An engine in readonly mode
   * can still read/write to the store and can still pub/sub events.
   */
  readonly?: boolean;
};

export interface SetOptions {
  nx?: boolean;
  ex?: number; // Expiry time in seconds
}

export interface HSetOptions {
  nx?: boolean;
  ex?: number; // Expiry time in seconds
  entity?: string;
}

export interface ZAddOptions {
  nx?: boolean;
}

export interface HScanResult {
  cursor: string;
  items: Record<string, string>;
}

export interface KVSQLProviderTransaction extends ProviderTransaction {
  [key: string]: any;
  exec(): Promise<any[]>;
  set(key: string, value: string, options?: SetOptions): ProviderTransaction;
  get(key: string): ProviderTransaction;
  del(key: string): ProviderTransaction;
  expire(key: string, seconds: number): ProviderTransaction;
  hset(
    key: string,
    fields: Record<string, string>,
    options?: HSetOptions,
  ): ProviderTransaction;
  hget(key: string, field: string): ProviderTransaction;
  hdel(key: string, fields: string[]): ProviderTransaction;
  hmget(key: string, fields: string[]): ProviderTransaction;
  hgetall(key: string): ProviderTransaction;
  hincrbyfloat(key: string, field: string, value: number): ProviderTransaction;
  hscan(key: string, cursor: string, count?: number): ProviderTransaction;
  lrange(key: string, start: number, end: number): ProviderTransaction;
  rpush(key: string, value: string): ProviderTransaction;
  lpush(key: string, value: string): ProviderTransaction;
  lpop(key: string): ProviderTransaction;
  lmove(
    source: string,
    destination: string,
    srcPosition: 'LEFT' | 'RIGHT',
    destPosition: 'LEFT' | 'RIGHT',
  ): ProviderTransaction;
  zadd(
    key: string,
    score: number,
    member: string,
    options?: ZAddOptions,
  ): ProviderTransaction;
  zrange(key: string, start: number, stop: number): ProviderTransaction;
  zrangebyscore(key: string, min: number, max: number): ProviderTransaction;
  zrangebyscore_withscores(
    key: string,
    min: number,
    max: number,
  ): ProviderTransaction;
  zrem(key: string, member: string): ProviderTransaction;
  zrank(key: string, member: string): ProviderTransaction;
  scan(cursor: number, count?: number): ProviderTransaction;
  rename(oldKey: string, newKey: string): ProviderTransaction;
  // Add other methods as needed
}
