import { Connection } from '../../types/memflow';
import { ProviderConfig, ProvidersConfig } from '../../types/provider';

/**
 * The Connection service is used to declare the class
 * and connection options but does not connect quite yet. Connection
 * happens at a later lifecycle stage when a workflow
 * is started by the MemFlow Client module (`(new MemFlow.Client())).start()`).
 *
 * The config options optionall support a multi-connection setup
 * where the `store` connection explicitly defined along with `stream`, `sub`, etc.
 * For example, Postgres can be used for stream and store while
 * Redis is used for sub.
 */
export class ConnectionService {
  /**
   * @private
   */
  constructor() {}

  /**
   * Instance initializer
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
}
