import { EventEmitter } from 'events';

import {
  HMSH_PG_HEARTBEAT_MS,
  HMSH_PG_HEARTBEAT_TIMEOUT_MS,
  HMSH_PG_RECONNECT_BASE_MS,
  HMSH_PG_RECONNECT_MAX_MS,
  HMSH_PG_TX_LOST_WINDOW_MS,
} from '../../../modules/enums';
import {
  HotMeshConnectionError,
  isConnectionError,
} from '../../../modules/errors';
import { ILogger } from '../../logger';
import {
  PostgresClassType,
  PostgresClientOptions,
  PostgresClientType,
} from '../../../types/postgres';
import { ConnectionHealth } from '../health';

type ResilientState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'ended';

/** A statement that opens a multi-statement transaction on the session. */
const TX_OPEN = /^\s*(BEGIN|START\s+TRANSACTION)\b[^;]*;?\s*$/i;
/** A statement that closes the session's open transaction. */
const TX_CLOSE = /^\s*(COMMIT|END|ROLLBACK|ABORT)\b/i;
const TX_ROLLBACK = /^\s*(ROLLBACK|ABORT)\b/i;

/** Bound on awaiting a native client's `end()`; a dead socket never blocks shutdown. */
const END_TIMEOUT_MS = 5_000;

const noop = () => undefined;

/** Timing policy of a resilient client; defaults come from the `HMSH_PG_*` settings. */
export interface ResilientPostgresPolicy {
  reconnectBaseMs: number;
  reconnectMaxMs: number;
  heartbeatMs: number;
  heartbeatTimeoutMs: number;
  txLostWindowMs: number;
}

function resolvePolicy(
  policy: Partial<ResilientPostgresPolicy> = {},
): ResilientPostgresPolicy {
  return {
    reconnectBaseMs: policy.reconnectBaseMs ?? HMSH_PG_RECONNECT_BASE_MS,
    reconnectMaxMs: policy.reconnectMaxMs ?? HMSH_PG_RECONNECT_MAX_MS,
    heartbeatMs: policy.heartbeatMs ?? HMSH_PG_HEARTBEAT_MS,
    heartbeatTimeoutMs:
      policy.heartbeatTimeoutMs ?? HMSH_PG_HEARTBEAT_TIMEOUT_MS,
    txLostWindowMs: policy.txLostWindowMs ?? HMSH_PG_TX_LOST_WINDOW_MS,
  };
}

/**
 * A PostgreSQL client that survives server restarts, failovers and
 * dropped sockets.
 *
 * It is a stable object that owns exactly one native `pg.Client` at a
 * time. Store, stream and sub services hold a reference to this object
 * for their lifetime, so the native socket underneath can be replaced
 * without any of them noticing.
 *
 * Contract:
 * - A lost socket never emits an unhandled `'error'`. The wrapper
 *   re-emits `'error'` only when the caller registered a listener.
 * - While reconnecting, `query()` rejects at once with a
 *   `HotMeshConnectionError`; callers already retry on query failure.
 * - A failure caused by the connection (not by the statement) is
 *   always surfaced as a `HotMeshConnectionError`.
 * - Reconnect uses full-jitter exponential backoff and never gives up
 *   until `end()`.
 * - `'reconnected'` fires after every successful reconnect so LISTEN
 *   owners can re-arm their channels on the new session.
 * - A transaction opened on one session never continues on the next:
 *   statements after a reconnect reject until the owner closes it.
 * - An idle session is probed on a heartbeat; a probe that times out
 *   destroys the socket, so a blackholed host is detected.
 *
 * Unknown properties (`processID`, `escapeIdentifier`, ...) resolve
 * against the current native client, and `instanceof` checks against
 * the native class hold, so existing callers keep working.
 */
class ResilientPostgresClient extends EventEmitter {
  readonly id: string;
  private readonly ClientClass: PostgresClassType;
  private readonly options: PostgresClientOptions;
  private readonly logger: ILogger;
  private readonly policy: ResilientPostgresPolicy;

  private current: PostgresClientType | null = null;
  //the most recent native session; answers property introspection while
  //reconnecting (queries never use it)
  private lastNative: PostgresClientType | null = null;
  private state: ResilientState = 'idle';
  private generation = 0;
  private attempts = 0;
  private lostAt = 0;
  private inflight = 0;
  private beating = false;
  private txGeneration: number | null = null;
  private txLostUntil = 0;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private heartbeatTimer: NodeJS.Timeout | undefined;

  constructor(
    ClientClass: PostgresClassType,
    options: PostgresClientOptions,
    id: string,
    logger: ILogger,
    policy?: Partial<ResilientPostgresPolicy>,
  ) {
    super();
    this.ClientClass = ClientClass;
    this.options = options;
    this.id = id;
    this.logger = logger;
    this.policy = resolvePolicy(policy);
    const nativePrototype = ClientClass?.prototype;
    const nativeOf = (target: ResilientPostgresClient): any =>
      target.current ?? target.lastNative;
    return new Proxy(this, {
      get(target, prop, receiver) {
        if (prop in target) {
          return Reflect.get(target, prop, receiver);
        }
        const native = nativeOf(target);
        const value = native?.[prop];
        return typeof value === 'function' ? value.bind(native) : value;
      },
      has(target, prop) {
        const native = nativeOf(target);
        return prop in target || (native ? prop in native : false);
      },
      //provider detection and serializers read own keys (`database`,
      //`connection`); expose the native client's alongside the wrapper's
      ownKeys(target) {
        const keys = new Set<string | symbol>(Reflect.ownKeys(target));
        const native = nativeOf(target);
        if (native) {
          for (const key of Reflect.ownKeys(native)) keys.add(key);
        }
        return Array.from(keys);
      },
      getOwnPropertyDescriptor(target, prop) {
        const own = Reflect.getOwnPropertyDescriptor(target, prop);
        if (own) {
          return own;
        }
        const native = nativeOf(target);
        const descriptor = native
          ? Reflect.getOwnPropertyDescriptor(native, prop)
          : undefined;
        //a key the target does not own must be reported configurable
        return descriptor ? { ...descriptor, configurable: true } : undefined;
      },
      getPrototypeOf(target) {
        return nativePrototype ?? Reflect.getPrototypeOf(target);
      },
    });
  }

  /** Session generation; increments on every successful reconnect. */
  get connectionGeneration(): number {
    return this.generation;
  }

  /** True while a live session is attached. */
  get isConnected(): boolean {
    return this.state === 'connected';
  }

  /** pg compatibility: callers test these before issuing best-effort queries. */
  get _ending(): boolean {
    return this.state === 'ended';
  }

  get _ended(): boolean {
    return this.state === 'ended';
  }

  /** The native client of the current session (undefined while reconnecting). */
  getNativeClient(): PostgresClientType | undefined {
    return this.current ?? undefined;
  }

  async connect(): Promise<void> {
    if (this.state !== 'idle') {
      return;
    }
    this.state = 'connecting';
    try {
      this.current = await this.open();
      this.lastNative = this.current;
    } catch (error) {
      this.state = 'idle';
      throw error;
    }
    this.state = 'connected';
    ConnectionHealth.register(this.id);
    this.startHeartbeat();
  }

  query(...args: any[]): any {
    const callback =
      typeof args[args.length - 1] === 'function' ? args.pop() : undefined;

    // submittables (cursors, streams) are driven by the native client
    if (args[0] && typeof args[0].submit === 'function') {
      if (!this.current) {
        throw this.unavailableError();
      }
      const passthrough = callback ? [...args, callback] : args;
      return (this.current as any).query(...passthrough);
    }

    const result = this.execute(args);
    if (callback) {
      result.then(
        (value) => callback(null, value),
        (error) => callback(error),
      );
      return undefined;
    }
    return result;
  }

  async end(): Promise<void> {
    if (this.state === 'ended') {
      return;
    }
    this.state = 'ended';
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.stopHeartbeat();
    ConnectionHealth.unregister(this.id);
    const client = this.current;
    this.current = null;
    if (client) {
      await this.endNative(client);
    }
    this.safeEmit('end');
  }

  /**
   * The error for a statement issued without a live session. An ended
   * client keeps pg's own message, which shutdown paths recognize; a
   * reconnecting client raises a connection error whose message never
   * matches those shutdown checks, so no caller mistakes an outage for
   * a closed client and returns an empty result.
   */
  private unavailableError(): Error {
    if (this.state === 'ended') {
      return new Error('Client was closed and is not queryable');
    }
    return new HotMeshConnectionError(`postgres-client-${this.state}`, this.id);
  }

  /** Listener faults are logged; they never reach the socket or the reconnect loop. */
  private safeEmit(event: string, payload?: unknown): void {
    try {
      this.emit(event, payload);
    } catch (error) {
      this.logger.error('postgres-client-listener-error', {
        connectionId: this.id,
        event,
        error,
      });
    }
  }

  private async execute(args: any[]): Promise<any> {
    const text: unknown = typeof args[0] === 'string' ? args[0] : args[0]?.text;
    const statement = typeof text === 'string' ? text : '';

    if (this.txGeneration !== null && this.txGeneration !== this.generation) {
      if (Date.now() > this.txLostUntil) {
        this.txGeneration = null;
      } else if (TX_CLOSE.test(statement)) {
        this.txGeneration = null;
        if (TX_ROLLBACK.test(statement)) {
          //the server already rolled the lost session back
          return { rows: [], rowCount: 0, command: 'ROLLBACK' };
        }
        throw new HotMeshConnectionError('postgres-transaction-lost', this.id);
      } else {
        throw new HotMeshConnectionError('postgres-transaction-lost', this.id);
      }
    }

    const client = this.current;
    if (this.state !== 'connected' || !client) {
      throw this.unavailableError();
    }

    const generation = this.generation;
    if (TX_OPEN.test(statement)) {
      this.txGeneration = generation;
    } else if (TX_CLOSE.test(statement)) {
      this.txGeneration = null;
    }

    this.inflight++;
    try {
      return await client.query(...(args as [string, any[]]));
    } catch (error) {
      if (
        error instanceof HotMeshConnectionError ||
        client !== this.current ||
        this.state !== 'connected' ||
        isConnectionError(error)
      ) {
        //state changes across the await; an ended client keeps pg's error
        if (this._ended) {
          throw error;
        }
        throw new HotMeshConnectionError(
          'postgres-session-lost',
          this.id,
          error,
        );
      }
      throw error;
    } finally {
      this.inflight--;
    }
  }

  private async open(): Promise<PostgresClientType> {
    const client: any = new this.ClientClass(this.options);
    client.on('error', (error: Error) => this.onLost(client, error));
    client.on('end', () =>
      this.onLost(client, new Error('Connection terminated unexpectedly')),
    );
    client.on('notification', (message: unknown) =>
      this.safeEmit('notification', message),
    );
    client.on('notice', (message: unknown) => this.safeEmit('notice', message));
    try {
      await client.connect();
      await client.query('SELECT 1');
      return client;
    } catch (error) {
      void this.endNative(client);
      throw error;
    }
  }

  private onLost(client: PostgresClientType, error: Error): void {
    if (client !== this.current || this.state !== 'connected') {
      return;
    }
    this.state = 'reconnecting';
    this.current = null;
    this.lostAt = Date.now();
    this.attempts = 0;
    this.stopHeartbeat();
    const code = (error as any)?.code;
    this.logger.warn('postgres-connection-lost', {
      connectionId: this.id,
      generation: this.generation,
      error: error?.message,
      code,
    });
    ConnectionHealth.markLost({
      connectionId: this.id,
      at: this.lostAt,
      error: { message: error?.message, code },
    });
    if (this.listenerCount('error') > 0) {
      this.safeEmit('error', error);
    }
    void this.endNative(client);
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.state !== 'reconnecting') {
      return;
    }
    const ceiling = Math.min(
      this.policy.reconnectMaxMs,
      this.policy.reconnectBaseMs * 2 ** Math.min(this.attempts, 30),
    );
    const delay = Math.round(Math.random() * ceiling);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.reconnect();
    }, delay);
    //a pending reconnect never holds a finished process open
    this.reconnectTimer.unref?.();
  }

  private async reconnect(): Promise<void> {
    if (this.state !== 'reconnecting') {
      return;
    }
    this.attempts++;
    let client: PostgresClientType;
    try {
      client = await this.open();
    } catch (error) {
      if (this.state === 'reconnecting') {
        this.logger.debug('postgres-reconnect-failed', {
          connectionId: this.id,
          attempts: this.attempts,
          error: error?.message,
        });
        this.scheduleReconnect();
      }
      return;
    }
    if (this.state !== 'reconnecting') {
      //ended while the session was opening
      void this.endNative(client);
      return;
    }
    this.current = client;
    this.lastNative = client;
    this.generation++;
    this.state = 'connected';
    if (this.txGeneration !== null) {
      this.txLostUntil = Date.now() + this.policy.txLostWindowMs;
    }
    const now = Date.now();
    const restored = {
      connectionId: this.id,
      at: now,
      downtimeMs: now - this.lostAt,
      attempts: this.attempts,
    };
    this.logger.info('postgres-connection-restored', {
      ...restored,
      generation: this.generation,
    });
    ConnectionHealth.markRestored(restored);
    this.startHeartbeat();
    this.safeEmit('reconnected', { ...restored, generation: this.generation });
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    if (this.policy.heartbeatMs <= 0) {
      return;
    }
    this.heartbeatTimer = setInterval(() => {
      void this.beat();
    }, this.policy.heartbeatMs);
    this.heartbeatTimer.unref?.();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  /** Probe an idle session; a busy session is proving itself already. */
  private async beat(): Promise<void> {
    const client = this.current;
    if (
      this.state !== 'connected' ||
      !client ||
      this.inflight > 0 ||
      this.beating
    ) {
      return;
    }
    this.beating = true;
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        client.query('SELECT 1'),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error('postgres-heartbeat-timeout')),
            this.policy.heartbeatTimeoutMs,
          );
        }),
      ]);
    } catch (error) {
      if (client === this.current && this.state === 'connected') {
        this.logger.warn('postgres-heartbeat-failed', {
          connectionId: this.id,
          error: error?.message,
        });
        this.onLost(client, error);
      }
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
      this.beating = false;
    }
  }

  /** End a native client without ever waiting on a dead socket. */
  private async endNative(client: PostgresClientType): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        Promise.resolve(client.end()).catch(noop),
        new Promise<void>((resolve) => {
          timer = setTimeout(() => {
            try {
              (client as any).connection?.stream?.destroy?.();
            } catch {
              //already destroyed
            }
            resolve();
          }, END_TIMEOUT_MS);
          timer.unref?.();
        }),
      ]);
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }
}

export { ResilientPostgresClient };
