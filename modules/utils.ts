import os from 'os';
import { createHash } from 'crypto';

import { nanoid } from 'nanoid';
import ms from 'ms';

import { LoggerService } from '../services/logger';
import { StoreService } from '../services/store';
import { AppSubscriptions, AppTransitions, AppVID } from '../types/app';
import {
  ProviderClient,
  ProviderConfig,
  ProviderTransaction,
  Providers,
  ProvidersConfig,
} from '../types/provider';
import { StringAnyType } from '../types/serializer';
import { StreamCode, StreamData, StreamStatus } from '../types/stream';
import { SystemHealth } from '../types/quorum';

import { HMSH_GUID_SIZE } from './enums';

const logger = new LoggerService('hotmesh', 'utils');

/**
 * @private
 */
export const hashOptions = (options: any): string => {
  const str = JSON.stringify(options);
  return createHash('sha256').update(str).digest('hex');
};

export async function getSystemHealth(): Promise<SystemHealth> {
  const totalMemory = os.totalmem();
  const freeMemory = os.freemem();
  const usedMemory = totalMemory - freeMemory;

  const systemHealth = {
    TotalMemoryGB: `${(totalMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    FreeMemoryGB: `${(freeMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    UsedMemoryGB: `${(usedMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    CPULoad: [],
    NetworkStats: [],
  };

  return systemHealth as SystemHealth;
}

export function deepCopy<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj));
}

export function deterministicRandom(seed: number): number {
  const x = Math.sin(seed) * 10000;
  return x - Math.floor(x);
}

export function guid(size: number = HMSH_GUID_SIZE): string {
  return `H` + nanoid(size);
}

export async function sleepFor(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function sleepImmediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export function XSleepFor(ms: number): {
  promise: Promise<unknown>;
  timerId: NodeJS.Timeout;
} {
  //can be interrupted with `clearTimeout`
  let timerId: NodeJS.Timeout;
  const promise = new Promise((resolve) => {
    timerId = setTimeout(resolve, ms);
  });
  return { promise, timerId };
}

/**
 * @private
 */
export function identifyProvider(provider: any): Providers | null {
  const prototype = Object.getPrototypeOf(provider);

  if (provider.Query?.prototype || Object.keys(provider).includes('database') || (prototype.name === 'Pool')) {
    return 'postgres';
  } else if (provider.constructor && provider.constructor.name === 'Client') {
    return 'nats';
  } else if (
    'defineCommand' in prototype ||
    Object.keys(prototype).includes('multi')
  ) {
    return 'ioredis';
  } else if (Object.keys(prototype).includes('Multi')) {
    return 'redis';
  }

  if (provider.constructor) {
    if (
      provider.constructor.name === 'Redis' ||
      provider.constructor.name === 'EventEmitter'
    ) {
      if ('hset' in provider) {
        return 'ioredis';
      }
    } else if (
      provider.constructor.name === 'ProviderClient' ||
      provider.constructor.name === 'Commander'
    ) {
      if ('HSET' in provider) {
        return 'redis';
      }
    }
  }

  let type: Providers | null = null;
  if (Object.keys(provider).includes('connection') || (!isNaN(provider.totalCount) && !isNaN(provider.idleCount))) {
    type = 'postgres';
  } else if (Object.keys(provider).includes('Pipeline')) {
    type = 'ioredis';
  } else if (Object.keys(provider).includes('createClient')) {
    type = 'redis';
  } else if (Object.keys(provider).includes('jetstream')) {
    type = 'nats';
  }
  return type;
}

/**
 * @private
 */
export const polyfill = {
  /**
   * `activity` is deprecated; `hook` is the replacement
   */
  resolveActivityType(activityType: string): string {
    return activityType === 'activity' ? 'hook' : activityType;
  },

  /**
   * `redis` is deprecated; `connection` is the generic replacement
   */
  providerConfig(obj: any): any {
    return obj?.connection ?? obj?.redis ?? obj?.connections;
  },

  /**
   * `redisClass and redisOptions` are deprecated; use `connection`
   */
  meshDataConfig(obj: {
    connection?: Partial<ProviderConfig>;
    connections?: Partial<ProvidersConfig>;
    redisClass?: any;
    redisOptions?: StringAnyType;
  }): Partial<ProviderConfig> | Partial<ProvidersConfig> {
    if (obj?.connections) {
      return obj.connections;
    }
    return (
      obj?.connection ?? {
        class: obj?.redisClass,
        options: obj?.redisOptions,
      }
    );
  },
};

/**
 * @private
 */
export function identifyRedisTypeFromClass(
  redisClass: any,
): 'redis' | 'ioredis' | null {
  if (
    redisClass && redisClass.name === 'Redis' ||
    redisClass.name === 'EventEmitter'
  ) {
    return 'ioredis';
  } else if (redisClass && 'createClient' in redisClass) {
    return 'redis';
  }
  return null;
}

/**
 * @private
 */
export function matchesStatusCode(
  code: StreamCode,
  pattern: string | RegExp,
): boolean {
  if (typeof pattern === 'string') {
    // Convert '*' wildcard to its regex equivalent (\d)
    const regexPattern = `^${pattern.replace(/\*/g, '\\d')}$`;
    return new RegExp(regexPattern).test(code.toString());
  }
  return pattern.test(code.toString());
}

/**
 * @private
 */
export function matchesStatus(
  status: StreamStatus,
  targetStatus: StreamStatus,
): boolean {
  return status === targetStatus;
}

/**
 * @private
 */
export function findTopKey(obj: AppTransitions, input: string): string | null {
  for (const [key, value] of Object.entries(obj)) {
    if (value.hasOwnProperty(input)) {
      const parentKey = findTopKey(obj, key.replace(/^\./, ''));
      return (parentKey || key).replace(/^\./, '');
    }
  }
  return null;
}

/**
 * @private
 */
export function findSubscriptionForTrigger(
  obj: AppSubscriptions,
  value: string,
): string | null {
  for (const [key, itemValue] of Object.entries(obj)) {
    if (itemValue === value) {
      return key;
    }
  }
  return null;
}

/**
 * Get the subscription topic for the flow to which activityId belongs.
 * @private
 */
export async function getSubscriptionTopic(
  activityId: string,
  store: StoreService<ProviderClient, ProviderTransaction>,
  appVID: AppVID,
): Promise<string | undefined> {
  const appTransitions = await store.getTransitions(appVID);
  const appSubscriptions = await store.getSubscriptions(appVID);
  const triggerId = findTopKey(appTransitions, activityId);
  const topic = findSubscriptionForTrigger(appSubscriptions, triggerId);
  return topic;
}

/**
 * returns the 12-digit format of the iso timestamp (e.g, 202101010000); returns
 * an empty string if overridden by the user to not segment by time (infinity).
 * @private
 */
export function getTimeSeries(granularity: string): string {
  if (granularity.toString() === 'infinity') {
    return '0';
  }
  const now = new Date();
  const granularityUnit = granularity.slice(-1);
  const granularityValue = parseInt(granularity.slice(0, -1), 10);
  if (granularityUnit === 'm') {
    const minute =
      Math.floor(now.getMinutes() / granularityValue) * granularityValue;
    now.setUTCMinutes(minute, 0, 0);
  } else if (granularityUnit === 'h') {
    now.setUTCMinutes(0, 0, 0);
  }
  return now
    .toISOString()
    .replace(/:\d\d\..+|-|T/g, '')
    .replace(':', '');
}

/**
 * @private
 */
export function formatISODate(input: Date | string): string {
  const date = input instanceof Date ? input : new Date(input);
  return date.toISOString().replace(/[:TZ-]/g, '');
}

/**
 * @private
 */
export function getSymKey(number: number): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const base = alphabet.length;
  if (number < 0 || number >= Math.pow(base, 3)) {
    throw new Error('Number out of range');
  }
  const [q1, r1] = divmod(number, base);
  const [q2, r2] = divmod(q1, base);
  return alphabet[q2] + alphabet[r1] + alphabet[r2];
}

/**
 * @private
 */
export function getSymVal(number: number): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const base = alphabet.length;
  if (number < 0 || number >= Math.pow(base, 2)) {
    throw new Error('Number out of range');
  }
  const [q, r] = divmod(number, base);
  return alphabet[q] + alphabet[r];
}

/**
 * @private
 */
function divmod(m: number, n: number): number[] {
  return [Math.floor(m / n), m % n];
}

/**
 * @private
 */
export function getIndexedHash<T>(hash: T, target: string): [number, T] {
  const index = (hash[target] as number) || 0;
  const newHash = { ...hash };
  delete newHash[target];
  return [index, newHash as T];
}

/**
 * @private
 */
export function getValueByPath(obj: { [key: string]: any }, path: string): any {
  const pathParts = path.split('/');
  let currentValue = obj;
  for (const part of pathParts) {
    if (currentValue[part] !== undefined) {
      currentValue = currentValue[part];
    } else {
      return undefined;
    }
  }
  return currentValue;
}

/**
 * @private
 */
export function restoreHierarchy(obj: StringAnyType): StringAnyType {
  const result: StringAnyType = {};
  for (const key in obj) {
    if (obj[key] === undefined) continue;
    const keys = key.split('/');
    let current = result;
    for (let i = 0; i < keys.length; i++) {
      if (i === keys.length - 1) {
        current[keys[i]] = obj[key];
      } else {
        current[keys[i]] = current[keys[i]] || {};
        current = current[keys[i]];
      }
    }
  }
  return result;
}

/**
 * @private
 */
export function isValidCron(cronExpression: string): boolean {
  const cronRegex =
    /^(\*|([0-5]?\d)) (\*|([01]?\d|2[0-3])) (\*|([12]?\d|3[01])) (\*|([1-9]|1[0-2])) (\*|([0-6](?:-[0-6])?(?:,[0-6])?))$/;
  return cronRegex.test(cronExpression);
}

/**
 * Returns the number of seconds for a string using the milliseconds format
 * used by the `ms` npm package as the input.
 */
export const s = (input: string): number => {
  return ms(input) / 1000;
};

/**
 * @private
 */
export const parseStreamMessage = (message: string): StreamData => {
  try {
    return JSON.parse(message);
  } catch (error) {
    logger.error('Error parsing Stream message', { ...error });
    throw error;
  }
};

/**
 * @private
 */
export const isStreamMessage = (result: any): boolean => {
  return Array.isArray(result) && Array.isArray(result[0]);
};

/**
 * Transforms  an array of arrays to an array of objects.
 */
export const arrayToHash = (
  response: [number, ...Array<string | string[]>],
): Record<string, string>[] => {
  const results: Record<string, string>[] = [];
  let key: string | undefined;
  for (let i = 1; i < response.length; i++) {
    // ignore count
    const row = response[i];
    const result: Record<string, string> = {};
    if (Array.isArray(row)) {
      // Check if row is an array
      for (let j = 0; j < row.length; j += 2) {
        const key = row[j];
        const value = row[j + 1];
        result[key] = value;
      }
      if (key) {
        result.$ = key;
      }
      results.push(result);
      key = undefined;
    } else {
      key = row as string;
    }
  }
  return results;
};
