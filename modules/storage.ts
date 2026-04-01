import { AsyncLocalStorage } from 'async_hooks';

export const asyncLocalStorage = new AsyncLocalStorage<Map<string, any>>();
export const activityAsyncLocalStorage = new AsyncLocalStorage<Map<string, any>>();
