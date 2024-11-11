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
  //outside callers can execute the transaction, regardless of provider by calling this method
  exec(): Promise<any>;

  // All other transaction methods are provider specific
  [key: string]: any;
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
};
