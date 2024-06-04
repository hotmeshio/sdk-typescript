import { Connection, ConnectionConfig } from '../../types/durable';

export class ConnectionService {
  static async connect(config: ConnectionConfig): Promise<Connection> {
    return {
      class: config.class,
      options: { ...config.options },
    } as Connection;
  }
}
