import { EventEmitter } from 'events';

import {
  ConnectionHealthEvent,
  ConnectionHealthSnapshot,
  ConnectionLostEvent,
  ConnectionRestoredEvent,
} from '../../types/provider';
import { ILogger, LoggerService } from '../logger';

type HealthListener<E extends ConnectionHealthEvent> = E extends 'lost'
  ? (event: ConnectionLostEvent) => unknown
  : (event: ConnectionRestoredEvent) => unknown;

/**
 * Process-wide availability of the resilient PostgreSQL connections.
 *
 * Every connection HotMesh opens through a `pg.Client` class registers
 * here. `state` is `down` while any registered connection is
 * reconnecting; `downSince` is the earliest loss among them. Listener
 * faults are logged and never reach the reconnect loop.
 *
 * ```typescript
 * import { ConnectionHealth } from '@hotmeshio/hotmesh';
 *
 * ConnectionHealth.on('lost', (e) => log.warn('db lost', e));
 * ConnectionHealth.on('restored', (e) => log.info('db back', e));
 * ConnectionHealth.snapshot(); // { state: 'up', total: 3, down: 0 }
 * ```
 */
class ConnectionHealthService {
  private readonly emitter = new EventEmitter();
  private readonly connections = new Set<string>();
  private readonly lost = new Map<string, number>();
  private logger: ILogger = new LoggerService('hotmesh', 'connection-health');

  constructor() {
    this.emitter.setMaxListeners(0);
  }

  on<E extends ConnectionHealthEvent>(
    event: E,
    listener: HealthListener<E>,
  ): this {
    this.emitter.on(event, listener as (...args: any[]) => void);
    return this;
  }

  once<E extends ConnectionHealthEvent>(
    event: E,
    listener: HealthListener<E>,
  ): this {
    this.emitter.once(event, listener as (...args: any[]) => void);
    return this;
  }

  off<E extends ConnectionHealthEvent>(
    event: E,
    listener: HealthListener<E>,
  ): this {
    this.emitter.off(event, listener as (...args: any[]) => void);
    return this;
  }

  snapshot(): ConnectionHealthSnapshot {
    const down = this.lost.size;
    const snapshot: ConnectionHealthSnapshot = {
      state: down > 0 ? 'down' : 'up',
      total: this.connections.size,
      down,
    };
    if (down > 0) {
      snapshot.downSince = Math.min(...this.lost.values());
    }
    return snapshot;
  }

  /** @private */
  register(connectionId: string): void {
    this.connections.add(connectionId);
  }

  /** @private */
  unregister(connectionId: string): void {
    this.connections.delete(connectionId);
    this.lost.delete(connectionId);
  }

  /** @private */
  markLost(event: ConnectionLostEvent): void {
    this.lost.set(event.connectionId, event.at);
    this.dispatch('lost', event);
  }

  /** @private */
  markRestored(event: ConnectionRestoredEvent): void {
    this.lost.delete(event.connectionId);
    this.dispatch('restored', event);
  }

  /** @private test isolation: forget every registered connection and listener. */
  reset(): void {
    this.connections.clear();
    this.lost.clear();
    this.emitter.removeAllListeners();
  }

  private dispatch(
    event: ConnectionHealthEvent,
    payload: ConnectionLostEvent | ConnectionRestoredEvent,
  ): void {
    //raw listeners keep once() semantics: the wrapper removes itself when invoked
    for (const listener of this.emitter.rawListeners(event)) {
      try {
        const result = (listener as (e: unknown) => unknown)(payload);
        if (
          result &&
          typeof (result as Promise<unknown>).catch === 'function'
        ) {
          (result as Promise<unknown>).catch((error) =>
            this.logger.error('connection-health-listener-error', {
              event,
              error,
            }),
          );
        }
      } catch (error) {
        this.logger.error('connection-health-listener-error', { event, error });
      }
    }
  }
}

const ConnectionHealth = new ConnectionHealthService();

export { ConnectionHealth, ConnectionHealthService };
