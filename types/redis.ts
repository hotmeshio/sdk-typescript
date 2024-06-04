import { StringStringType } from './serializer';
import { ReclaimedMessageType } from './stream';

type MultiResponseFlags = (string | number)[]; // e.g., [3, 2, '0']

/**
 * Redis types
 */
interface ConnectionOptions {
  //[key: string]: any;
  host?: string;
  port?: number;
  path?: string; // Creates unix socket connection to path. If this option is specified, host and port are ignored.
  socket?: any; // Establish secure connection on a given socket rather than creating a new socket
  NPNProtocols?: string[] | Buffer[]; // Defaults to []
  ALPNProtocols?: string[] | Buffer[]; // Defaults to []
  servername?: string; // SNI TLS Extension
  checkServerIdentity?: (servername: string, cert: Buffer) => Error | undefined;
  session?: Buffer;
  minDHSize?: number;
  secureContext?: any; // If not provided, the entire ConnectionOptions object will be passed to tls.createSecureContext()
  secureProtocol?: string; // The SSL method to use, e.g., SSLv3_method
  ciphers?: string;
  honorCipherOrder?: boolean;
  requestCert?: boolean;
  rejectUnauthorized?: boolean;
  maxVersion?: 'TLSv1.3' | 'TLSv1.2' | 'TLSv1.1' | 'TLSv1';
  minVersion?: 'TLSv1.3' | 'TLSv1.2' | 'TLSv1.1' | 'TLSv1';
}

interface RedisRedisClientOptions {
  url?: string;
  socket?: {
    host?: string;
    port?: number;
    tls?: boolean;
    servername?: string;
    //[key: string]: any;
  };
  username?: string;
  password?: string;
  database?: number;
  name?: string;
  readonly?: boolean;
  legacyMode?: boolean;
  commandsQueueMaxLength?: number;
  disableOfflineQueue?: boolean;
  connectTimeout?: number;
  autoResubscribe?: boolean;
  autoResendUnfulfilledCommands?: boolean;
  lazyConnect?: boolean;
  tls?: boolean | ConnectionOptions;
  enableReadyCheck?: boolean;
  keepAlive?: number;
  family?: 'IPv4' | 'IPv6';
  keyPrefix?: string;
  retry?: {
    maxReconnectionAttempts?: number;
    initialReconnectionDelay?: number;
    reconnectionBackoffFactor?: number;
    totalReconnectionTimeout?: number;
    maxRetryCount?: number;
  };
  stringNumbers?: boolean;
}

interface RedisRedisMultiType {
  sendCommand(command: string, ...args: string[]): Promise<any>;
  exec: () => Promise<unknown[]>;

  XADD(key: string, id: string, fields: any): this;
  XACK(key: string, group: string, id: string): this;
  XDEL(key: string, id: string): this;
  XLEN(key: string): this;
  DEL(key: string): this;
  EXISTS(key: string): this;
  HDEL(key: string, itemId: string): this;
  HGET(key: string, itemId: string): this;
  HGETALL(key: string): this;
  HINCRBYFLOAT(key: string, itemId: string, value: number): this;
  HMGET(key: string, itemIds: string[]): Promise<string[]>;
  HSET(key: string, values: Record<string, string>): this;
  LPUSH(key: string, items: string[]): this;
  LRANGE(key: string, start: number, end: number): this;
  RPUSH(key: string, items: string[]): this;
  SET(key: string, value: string): this;
  ZADD(
    key: string,
    values: { score: string; value: string },
    opts?: { NX: boolean },
  ): this;
  ZRANGE_WITHSCORES(key: string, start: number, end: number): this;
  ZRANK(key: string, member: string): this;
  ZSCORE(key: string, value: string): this;
}

interface RedisRedisClientType {
  multi(): Partial<RedisRedisMultiType>;
  connect(): Promise<void>;
  sendCommand(args: any[]): Promise<any>;
  exec(): Promise<unknown[]>;
  flushDb(): Promise<string>;
  quit(): Promise<string>;
  disconnect(): void;
  duplicate(): RedisRedisClientType;
  on(event: string, callback: (...args: any[]) => void): void;
  publish(channel: string, message: string): Promise<number>;
  pSubscribe(
    pattern: string,
    callback: (channel: string, message: string) => void,
  ): void;
  pUnsubscribe(pattern: string): void;
  subscribe(
    channel: string,
    callback: (channel: string, message: string) => void,
  ): void;
  unsubscribe(channel: string): void;
  punsubscribe(channel: string): void;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<string>;

  XADD(key: string, id: string, fields: any): Promise<string>;
  XACK(key: string, group: string, id: string): Promise<number>;
  XDEL(key: string, id: string): Promise<number>;
  XLEN(key: string): Promise<number>;
  DEL(key: string): Promise<number>;
  EXISTS(key: string): Promise<number>;
  HDEL(key: string, itemId: string): Promise<number>;
  HGET(key: string, itemId: string): Promise<string | null>;
  HGETALL(key: string): Promise<StringStringType>;
  HINCRBYFLOAT(key: string, itemId: string, value: number): Promise<number>;
  HMGET(key: string, itemIds: string[]): Promise<string[]>;
  HSET(key: string, values: Record<string, string>): this;
  LPUSH(key: string, items: string[]): Promise<number>;
  LRANGE(key: string, start: number, end: number): Promise<string[]>;
  RPUSH(key: string, items: string[]): Promise<number>;
  SET(key: string, value: string): Promise<string>;
  ZADD(
    key: string,
    values: { score: string; value: string },
    opts?: { NX: boolean },
  ): Promise<number>;
  ZRANGE_WITHSCORES(
    key: string,
    start: number,
    end: number,
  ): Promise<{ score: number; value: string }>;
  ZRANK(key: string, member: string): Promise<number>;
  ZSCORE(key: string, value: string): Promise<number>;
}

interface RedisRedisClassType {
  createClient(options: RedisRedisClientOptions): Partial<RedisRedisClientType>;
}

/**
 * IORedis types
 */
interface IORedisClientOptions {
  port?: number;
  host?: string;
  family?: 'IPv4' | 'IPv6';
  path?: string;
  keepAlive?: number;
  noDelay?: boolean;
  connectionName?: string;
  db?: number;
  password?: string;
  username?: string;
  sentinels?: Array<{ host: string; port: number }>;
  name?: string;
  readOnly?: boolean;
  keyPrefix?: string;
  reconnectOnError?: (err: Error) => boolean;
}

interface IORedisClient {
  multi(): IORedisMultiType;
  exec(): Promise<unknown[]>;
  sendCommand(args: any[]): Promise<any>;
  call(command: string, ...args: any[]): Promise<any>;
  quit(): Promise<string>;
  flushdb(): Promise<string>;

  publish(channel: string, message: string): Promise<number>;
  psubscribe(
    pattern: string,
    callback: (channel: string, message: string) => void,
  ): Promise<void>;
  punsubscribe(pattern: string): void;
  subscribe(
    channel: string,
    callback: (channel: string, message: string) => void,
  ): void;
  unsubscribe(channel: string): void;
  punsubscribe(channel: string): void;

  xadd(key: string, id: string, fields: any, message?: string): Promise<string>;
  xack(key: string, group: string, id: string): Promise<number>;
  xdel(key: string, id: string): Promise<number>;
  xlen(key: string): Promise<number>;
  xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string,
  ): Promise<
    | [string, string, number, [string, number][]][]
    | [string, string, number, number]
    | unknown[]
  >;
  xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): Promise<ReclaimedMessageType>;
  xinfo(command: 'GROUPS' | string, key: string): Promise<unknown>;
  xrange(key: string, start: string, end: string): Promise<string[][]>;
  del(key: string): Promise<number>;
  exists(key: string): Promise<number>;
  get(key: string): Promise<string | null>;
  hdel(key: string, itemId: string): Promise<number>;
  hget(key: string, itemId: string): Promise<string | null>;
  hgetall(key: string): Promise<StringStringType>;
  hincrbyfloat(key: string, itemId: string, value: number): Promise<number>;
  hmget(key: string, itemIds: string[]): Promise<string[]>;
  hset(key: string, values: Record<string, string>): this;
  lpush(key: string, ...args: string[]): Promise<number>;
  lrange(key: string, start: number, end: number): Promise<string[]>;
  on(event: string, callback: (...args: any[]) => void): void;
  rpush(key: string, ...args: string[]): Promise<number>;
  set(key: string, value: string): Promise<string>;
  zadd(...args: Array<string | number>): Promise<number>;
  zrange(
    key: string,
    start: number,
    end: number,
    withScores?: 'WITHSCORES',
  ): Promise<string[]>;
  zrank(key: string, member: string): Promise<number>;
  zscore(key: string, value: string): Promise<number>;
  xgroup(
    command: 'CREATE' | string,
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM',
  ): Promise<string>;
}

type IORedisClassType = new (
  options: IORedisClientOptions,
  ...args: any[]
) => IORedisClient;
interface IORedisMultiType {
  xadd(key: string, id: string, fields: any, message?: string): this;
  xack(key: string, group: string, id: string): this;
  xdel(key: string, id: string): this;
  xlen(key: string): this;
  xpending(
    key: string,
    group: string,
    start?: string,
    end?: string,
    count?: number,
    consumer?: string,
  ): this;
  xclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleTime: number,
    id: string,
    ...args: string[]
  ): this;
  hdel(key: string, itemId: string): this;
  hget(key: string, itemId: string): this;
  hgetall(key: string): this;
  hincrbyfloat(key: string, itemId: string, value: number): this;
  hmget(key: string, itemIds: string[]): this;
  hset(key: string, values: Record<string, string>): this;
  lrange(key: string, start: number, end: number): this;
  rpush(key: string, value: string): this;
  zadd(...args: Array<string | number>): this;
  xgroup(
    command: 'CREATE',
    key: string,
    groupName: string,
    id: string,
    mkStream?: 'MKSTREAM',
  ): this;

  sendCommand(command: string[]): Promise<any>;
  exec: () => Promise<unknown[]>;
}

type RedisClass = RedisRedisClassType | IORedisClassType;
type RedisClient = RedisRedisClientType | IORedisClient;
type RedisOptions = RedisRedisClientOptions | IORedisClientOptions;
type RedisMulti = RedisRedisMultiType | IORedisMultiType;

function isRedisClient(client: RedisClient): client is RedisRedisClientType {
  return 'sendCommand' in client;
}

function isIORedisClient(client: RedisClient): client is IORedisClient {
  return 'pipeline' in client;
}

export {
  RedisClass,
  RedisRedisClientType,
  RedisRedisClientOptions,
  RedisRedisClassType,
  IORedisClient as IORedisClientType,
  RedisClient,
  RedisMulti,
  RedisRedisMultiType,
  IORedisClientOptions,
  IORedisClassType,
  IORedisMultiType,
  RedisOptions,
  MultiResponseFlags,
  isRedisClient,
  isIORedisClient,
};
