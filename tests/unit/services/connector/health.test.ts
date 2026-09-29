import { describe, it, expect, vi, beforeEach } from 'vitest';

import { ConnectionHealthService } from '../../../../services/connector/health';
import { withConnectionDefaults } from '../../../../services/connector/providers/postgres';
import { sleepFor } from '../../../../modules/utils';

describe('ConnectionHealth', () => {
  let health: ConnectionHealthService;

  beforeEach(() => {
    health = new ConnectionHealthService();
  });

  it('reports up with no registered connections', () => {
    expect(health.snapshot()).toEqual({ state: 'up', total: 0, down: 0 });
  });

  it('reports down while any connection is reconnecting, with the earliest loss', () => {
    health.register('a');
    health.register('b');
    health.markLost({ connectionId: 'a', at: 2000, error: { message: 'x' } });
    health.markLost({ connectionId: 'b', at: 1000, error: { message: 'y' } });
    expect(health.snapshot()).toEqual({ state: 'down', total: 2, down: 2, downSince: 1000 });

    health.markRestored({ connectionId: 'b', at: 3000, downtimeMs: 2000, attempts: 1 });
    expect(health.snapshot()).toEqual({ state: 'down', total: 2, down: 1, downSince: 2000 });

    health.markRestored({ connectionId: 'a', at: 3000, downtimeMs: 1000, attempts: 1 });
    expect(health.snapshot()).toEqual({ state: 'up', total: 2, down: 0 });
  });

  it('forgets a connection that ends while reconnecting', () => {
    health.register('a');
    health.markLost({ connectionId: 'a', at: 1, error: { message: 'x' } });
    health.unregister('a');
    expect(health.snapshot()).toEqual({ state: 'up', total: 0, down: 0 });
  });

  it('delivers lost and restored events to listeners', () => {
    const lost = vi.fn();
    const restored = vi.fn();
    health.on('lost', lost);
    health.on('restored', restored);
    const lostEvent = { connectionId: 'a', at: 1, error: { message: 'x', code: '57P01' } };
    const restoredEvent = { connectionId: 'a', at: 2, downtimeMs: 1, attempts: 1 };
    health.markLost(lostEvent);
    health.markRestored(restoredEvent);
    expect(lost).toHaveBeenCalledWith(lostEvent);
    expect(restored).toHaveBeenCalledWith(restoredEvent);

    health.off('lost', lost);
    health.markLost(lostEvent);
    expect(lost).toHaveBeenCalledTimes(1);
  });

  it('isolates listener faults (sync throw and async rejection)', async () => {
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    try {
      const healthy = vi.fn();
      health.on('lost', () => {
        throw new Error('sync fault');
      });
      health.on('lost', async () => {
        throw new Error('async fault');
      });
      health.on('lost', healthy);
      expect(() =>
        health.markLost({ connectionId: 'a', at: 1, error: { message: 'x' } }),
      ).not.toThrow();
      await sleepFor(20);
      expect(healthy).toHaveBeenCalledTimes(1);
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it('fires a once listener a single time', () => {
    const listener = vi.fn();
    health.once('restored', listener);
    const event = { connectionId: 'a', at: 1, downtimeMs: 1, attempts: 1 };
    health.markRestored(event);
    health.markRestored(event);
    expect(listener).toHaveBeenCalledTimes(1);
  });
});

describe('withConnectionDefaults', () => {
  it('fills timeouts, keepalive and application_name when absent', () => {
    const resolved = withConnectionDefaults({ host: 'db' });
    expect(resolved).toMatchObject({
      host: 'db',
      connectionTimeoutMillis: 10_000,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10_000,
      application_name: 'hotmesh',
    });
  });

  it('never overrides a value the caller set', () => {
    const resolved = withConnectionDefaults({
      connectionTimeoutMillis: 0,
      keepAlive: false,
      keepAliveInitialDelayMillis: 1,
      application_name: 'my-service',
    });
    expect(resolved).toMatchObject({
      connectionTimeoutMillis: 0,
      keepAlive: false,
      keepAliveInitialDelayMillis: 1,
      application_name: 'my-service',
    });
  });

  it('does not mutate the caller options', () => {
    const options = { host: 'db' };
    withConnectionDefaults(options);
    expect(options).toEqual({ host: 'db' });
  });
});
