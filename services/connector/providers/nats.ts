import { AbstractConnection } from '..';
import {
  NatsClientOptions,
  NatsClientType,
  NatsClassType,
} from '../../../types/nats';

class NatsConnection extends AbstractConnection<
  NatsClassType,
  NatsClientOptions
> {
  defaultOptions: NatsClientOptions = {
    servers: ['nats:4222'],
    timeout: 5000,
  };

  async createConnection(
    Connect: NatsClassType,
    options: NatsClientOptions,
  ): Promise<NatsClientType> {
    try {
      return (await Connect(options)) as NatsClientType;
    } catch (error) {
      NatsConnection.logger.error(`nats-provider-connection-failed`, {
        error,
      });
      throw new Error(`nats-provider-connection-failed: ${error.message}`);
    }
  }

  public getClient(): NatsClientType {
    if (!this.connection) {
      throw new Error('nats-provider-connection-failed');
    }
    return this.connection;
  }

  public async closeConnection(connection: NatsClientType): Promise<void> {
    await connection.close();
  }
}

export { NatsConnection, NatsClientOptions };
