import { Connection, ConnectionConfig } from '../../types/meshflow';

/**
 * The Connection service is used to declare the Redis class
 * and connection options but does not connect to Redis. Connection
 * to Redis happens at a later lifecycle stage when a workflow
 * is started by the MeshFlow Client module (`(new MeshFlow.Client())).start()`).
 */
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
