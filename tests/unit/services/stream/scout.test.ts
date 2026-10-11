import { describe, it, expect, vi, afterEach } from 'vitest';

import { ScoutManager } from '../../../../services/stream/providers/postgres/scout';
import { StreamService } from '../../../../services/stream';

const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } as any;

const makeScout = (query: ReturnType<typeof vi.fn>) =>
  new ScoutManager(
    { query } as any,
    'app',
    () => 'app.engine_streams',
    () => 'hmsh:app:w:',
    logger,
  );

afterEach(() => {
  vi.useRealTimers();
});

describe('UNIT | stream | ScoutManager shutdown', () => {
  it('stops a scout that lost the role without waiting out its pause', async () => {
    vi.useFakeTimers();
    const query = vi.fn().mockResolvedValue({ rows: [] });
    const scout = makeScout(query);
    scout.startRouterScoutPoller();
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(1);

    await scout.stopRouterScoutPoller();
    await vi.advanceTimersByTimeAsync(0);
    expect(scout.isStopped()).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    const calls = query.mock.calls.length;
    await vi.advanceTimersByTimeAsync(120_000);
    expect(query.mock.calls.length).toBe(calls);
  });

  it('clears the role reset timer and releases the role when it is the scout', async () => {
    vi.useFakeTimers();
    const query = vi.fn().mockImplementation(async (sql: string) =>
      sql.includes('INSERT') ? { rows: [{ key: 'k' }] } : { rows: [{ count: 0 }], rowCount: 1 },
    );
    const scout = makeScout(query);
    scout.startRouterScoutPoller();
    await vi.advanceTimersByTimeAsync(0);
    expect(scout.isCurrentlyScout()).toBe(true);
    expect(vi.getTimerCount()).toBe(2);

    await scout.stopRouterScoutPoller();
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    expect(query.mock.calls.some(([sql]) => String(sql).includes('DELETE'))).toBe(true);
  });
});

describe('UNIT | stream | StreamService.stopAll', () => {
  class TrackedStream {
    cleanup = vi.fn(async () => {
      (StreamService as any).running.delete(this);
    });
    track() {
      (StreamService as any).running.add(this);
    }
  }

  it('runs cleanup on every tracked service once and forgets them', async () => {
    const a = new TrackedStream();
    const b = new TrackedStream();
    a.track();
    b.track();
    await StreamService.stopAll();
    expect(a.cleanup).toHaveBeenCalledTimes(1);
    expect(b.cleanup).toHaveBeenCalledTimes(1);
    await StreamService.stopAll();
    expect(a.cleanup).toHaveBeenCalledTimes(1);
  });

  it('keeps going when one cleanup fails', async () => {
    const bad = { cleanup: vi.fn().mockRejectedValue(new Error('closed')) };
    const good = new TrackedStream();
    (StreamService as any).running.add(bad);
    good.track();
    await expect(StreamService.stopAll()).resolves.toBeUndefined();
    expect(good.cleanup).toHaveBeenCalledTimes(1);
  });
});
