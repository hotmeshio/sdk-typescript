import { connect, NatsConnection, ConnectionOptions } from 'nats';

class NATSConnection {
  private static instances: Map<string, NATSConnection> = new Map();
  private connection: NatsConnection | null = null;
  private id: string | null = null;

  private constructor() {}

  public getClient(): NatsConnection {
    if (!this.connection) {
      throw new Error('NATS client is not connected');
    }
    return this.connection;
  }

  public async disconnect(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
    if (this.id) {
      NATSConnection.instances.delete(this.id);
    }
  }

  public static async connect(
    id: string,
    options: ConnectionOptions,
  ): Promise<NATSConnection> {
    if (this.instances.has(id)) {
      return this.instances.get(id) as NATSConnection;
    }
    const instance = new NATSConnection();
    instance.connection = await connect(options);
    instance.id = id;
    this.instances.set(id, instance);
    return instance;
  }

  public static async disconnectAll(): Promise<void> {
    await Promise.all(
      Array.from(this.instances.values()).map((instance) =>
        instance.disconnect(),
      ),
    );
    this.instances.clear();
  }
}

export { NATSConnection };
