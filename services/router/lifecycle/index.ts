import { sleepFor } from '../../../modules/utils';
import { HMSH_BLOCK_TIME_MS } from '../config';
import { ILogger } from '../../logger';
import { StreamService } from '../../stream';
import { ProviderClient, ProviderTransaction } from '../../../types/provider';

export class InstanceRegistry {
  private static instances: Set<any> = new Set();

  static add(router: any): void {
    InstanceRegistry.instances.add(router);
  }

  static remove(router: any): void {
    InstanceRegistry.instances.delete(router);
  }

  static async stopAll(): Promise<void> {
    const stopPromises = [];
    for (const instance of [...InstanceRegistry.instances]) {
      stopPromises.push(instance.stopConsuming());
    }
    await Promise.all(stopPromises);
    await sleepFor(HMSH_BLOCK_TIME_MS * 2);
  }

  static getInstances(): Set<any> {
    return new Set(InstanceRegistry.instances);
  }
}

export class LifecycleManager<S extends StreamService<ProviderClient, ProviderTransaction>> {
  private shouldConsume: boolean = false;
  private readonly: boolean;
  private topic: string | undefined;
  private logger: ILogger;
  private stream: S;
  private isUsingNotifications: boolean = false;

  constructor(
    readonly: boolean,
    topic: string | undefined,
    logger: ILogger,
    stream: S,
  ) {
    this.readonly = readonly;
    this.topic = topic;
    this.logger = logger;
    this.stream = stream;
  }

  getShouldConsume(): boolean {
    return this.shouldConsume;
  }

  setShouldConsume(value: boolean): void {
    this.shouldConsume = value;
  }

  getIsUsingNotifications(): boolean {
    return this.isUsingNotifications;
  }

  setIsUsingNotifications(value: boolean): void {
    this.isUsingNotifications = value;
  }

  isReadonly(): boolean {
    return this.readonly;
  }

  isStopped(group: string, consumer: string, stream: string): boolean {
    if (!this.shouldConsume) {
      this.logger.info(`router-stream-stopped`, {
        group,
        consumer,
        stream,
      });
    }
    return !this.shouldConsume;
  }

  async startConsuming(router: any): Promise<void> {
    this.shouldConsume = true;
    InstanceRegistry.add(router);
  }

  async stopConsuming(router: any): Promise<void> {
    this.shouldConsume = false;
    this.logger.info(
      `router-stream-stopping`,
      this.topic ? { topic: this.topic } : undefined,
    );
    InstanceRegistry.remove(router);
    
    // If using notifications, properly clean up
    if (this.isUsingNotifications && this.stream.stopNotificationConsumer) {
      try {
        await this.stream.cleanup?.();
      } catch (error) {
        this.logger.error('router-stream-cleanup-error', { error });
      }
    }
  }
} 