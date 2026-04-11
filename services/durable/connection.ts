import { Connection } from '../../types/durable';
import { ProviderConfig, ProvidersConfig } from '../../types/provider';

/**
 * Declares connection configuration (driver class + options) without
 * opening a connection. The actual Postgres connection is established
 * lazily when a Worker or Client is started.
 *
 * Supports both single-connection and split-connection layouts (separate
 * `store`, `stream`, and `sub` connections for advanced deployments).
 */
export class ConnectionService {
  /**
   * @private
   */
  constructor() {}

  /**
   * Create a connection configuration object.
   *
   * @param config - Single `{ class, options }` or split `{ store, stream, sub }`.
   * @returns The normalized connection configuration.
   */
  static async connect(
    config: ProviderConfig | ProvidersConfig,
  ): Promise<Connection> {
    return 'store' in config
      ? config
      : ({
          class: config.class,
          options: { ...config.options },
          provider: config.provider,
        } as Connection);
  }

  /**
   * Alias for {@link connect}.
   */
  static create = ConnectionService.connect;
}
