/**
 * Abstract class for creating connections to different services.
 * All implementations should extend this class and implement
 * the abstract methods. To register another connector provider:
 *
 * 1) Add the provider to ./providers/<name>.ts
 * 2) Update ./factory.ts to reference the provider
 * 3) Register the name with the `Provider` type in ./types/provider.ts.
 * 4) Declare the specific provider types in ./types/<name>.ts
 * 5) Update ./modules/utils.ts (identifyProvider) with logic to resolve the provider
 */
abstract class AbstractConnection<TClient, TOptions> {
  protected connection: any | null = null;
  protected static instances: Map<string, AbstractConnection<any, any>> =
    new Map();
  protected id: string | null = null;

  protected abstract defaultOptions: any;

  protected abstract createConnection(
    client: TClient,
    options: TOptions,
  ): Promise<any>;

  public abstract getClient(): any;

  public async disconnect(): Promise<void> {
    if (this.connection) {
      await this.closeConnection(this.connection);
      this.connection = null;
    }
    if (this.id) {
      AbstractConnection.instances.delete(this.id);
    }
  }

  protected abstract closeConnection(connection: any): Promise<void>;

  public static async connect<T extends AbstractConnection<any, any>>(
    this: new () => T,
    id: string,
    client: any,
    options?: any,
  ): Promise<T> {
    if (AbstractConnection.instances.has(id)) {
      return AbstractConnection.instances.get(id) as T;
    }
    const instance = new this();
    const opts = options ? { ...options } : { ...instance.defaultOptions };

    instance.connection = await instance.createConnection(client, opts);
    instance.id = id;
    AbstractConnection.instances.set(id, instance);
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

export { AbstractConnection };
