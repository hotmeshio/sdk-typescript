import os from 'os';

import si from 'systeminformation';
import { nanoid } from 'nanoid';

import { StoreService } from '../services/store';
import { AppSubscriptions, AppTransitions, AppVID } from '../types/app';
import { RedisClient, RedisMulti } from '../types/redis';
import { StringAnyType } from '../types/serializer';
import { StreamCode, StreamStatus } from '../types/stream';
import { SystemHealth } from '../types/quorum';

import { HMSH_GUID_SIZE } from './enums';

async function safeExecute<T>(
  operation: Promise<T>,
  defaultValue: T,
): Promise<T> {
  try {
    return await operation;
  } catch (error) {
    console.error(`Operation Error: ${error}`);
    return defaultValue;
  }
}

export async function getSystemHealth(): Promise<SystemHealth> {
  const totalMemory = os.totalmem();
  const freeMemory = os.freemem();
  const usedMemory = totalMemory - freeMemory;
  const cpus = os.cpus();

  // CPU load calculation remains unchanged
  const cpuLoad = cpus.map((cpu, i) => {
    const total = Object.values(cpu.times).reduce((acc, tv) => acc + tv, 0);
    const idle = cpu.times.idle;
    const usage = ((total - idle) / total) * 100;
    return { [`CPU ${i} Usage`]: `${usage.toFixed(2)}%` };
  });

  // Wrap each systeminformation call with safeExecute
  const networkStats = await safeExecute(si.networkStats(), []);

  // Construct the system health object with error handling in mind
  const systemHealth = {
    TotalMemoryGB: `${(totalMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    FreeMemoryGB: `${(freeMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    UsedMemoryGB: `${(usedMemory / 1024 / 1024 / 1024).toFixed(2)} GB`,
    CPULoad: cpuLoad,
    NetworkStats: networkStats,
  };

  return systemHealth as SystemHealth;
}

export async function sleepFor(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function sleepImmediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export function guid(size: number = HMSH_GUID_SIZE): string {
  return `H` + nanoid(size);
}

export function deterministicRandom(seed: number): number {
  const x = Math.sin(seed) * 10000;
  return x - Math.floor(x);
}

export function identifyRedisType(
  redisInstance: any,
): 'redis' | 'ioredis' | null {
  const prototype = Object.getPrototypeOf(redisInstance);
  if (
    'defineCommand' in prototype ||
    Object.keys(prototype).includes('multi')
  ) {
    return 'ioredis';
  } else if (Object.keys(prototype).includes('Multi')) {
    return 'redis';
  }
  if (redisInstance.constructor) {
    if (
      redisInstance.constructor.name === 'Redis' ||
      redisInstance.constructor.name === 'EventEmitter'
    ) {
      if ('hset' in redisInstance) {
        return 'ioredis';
      }
    } else if (
      redisInstance.constructor.name === 'RedisClient' ||
      redisInstance.constructor.name === 'Commander'
    ) {
      if ('HSET' in redisInstance) {
        return 'redis';
      }
    }
  }
  return null;
}

//todo: the polyfill methods will all be deleted in the `beta` release.
export const polyfill = {
  resolveActivityType(activityType: string): string {
    if (activityType === 'activity') {
      return 'hook';
    }
    return activityType;
  },
};

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

export function matchesStatus(
  status: StreamStatus,
  targetStatus: StreamStatus,
): boolean {
  return status === targetStatus;
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

export function findTopKey(obj: AppTransitions, input: string): string | null {
  for (const [key, value] of Object.entries(obj)) {
    if (value.hasOwnProperty(input)) {
      const parentKey = findTopKey(obj, key.replace(/^\./, ''));
      return (parentKey || key).replace(/^\./, '');
    }
  }
  return null;
}

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
 * Get the subscription topic for the flow to which @activityId belongs.
 * TODO: resolve this value in the compiler...do not call this at runtime
 */
export async function getSubscriptionTopic(
  activityId: string,
  store: StoreService<RedisClient, RedisMulti>,
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

export function formatISODate(input: Date | string): string {
  const date = input instanceof Date ? input : new Date(input);
  return date.toISOString().replace(/[:TZ-]/g, '');
}

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

export function getSymVal(number: number): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const base = alphabet.length;
  if (number < 0 || number >= Math.pow(base, 2)) {
    throw new Error('Number out of range');
  }
  const [q, r] = divmod(number, base);
  return alphabet[q] + alphabet[r];
}

function divmod(m: number, n: number): number[] {
  return [Math.floor(m / n), m % n];
}

export function getIndexedHash<T>(hash: T, target: string): [number, T] {
  const index = (hash[target] as number) || 0;
  const newHash = { ...hash };
  delete newHash[target];
  return [index, newHash as T];
}

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
