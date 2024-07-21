import { Connection, ConnectionConfig } from '../../types/meshflow';

export class ConnectionService {
  /**
   * @private
   */
  constructor() {}

  /**
   * Instance initializer
   */
  static async connect(config: ConnectionConfig): Promise<Connection> {
    return {
      class: config.class,
      options: { ...config.options },
    } as Connection;
  }
}
