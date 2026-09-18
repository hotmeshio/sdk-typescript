import { describe, it, expect, vi } from 'vitest';

import { Hook } from '../../../../services/activities/hook';

// ─────────────────────────────────────────────────────────────────────────────
// Timeout-path delivery
//
// The expiry statement returns the row it moved to `expired` plus the row's
// prior status. An accumulator row carries the delivered collection in
// `resolver_payload`; the hook must hand it back so Leg2 can put it on the
// waiter's timeout marker. A row a resolve already settled must make the hook
// skip Leg2 entirely. A missing row (the wait had no escalation) proceeds as a
// plain timeout, and the legacy null return shape still means "no row".
// ─────────────────────────────────────────────────────────────────────────────

const SIGNAL_KEY = 'sig-task-123';

function buildHook(expireResult: unknown) {
  const restoredState = {
    'metadata/gid': 'gid-1',
    'metadata/jid': 'task-job-1',
    'metadata/js': 900000000000000,
    't1/output/data/signalId': SIGNAL_KEY,
  };
  const store = {
    getState: vi.fn().mockResolvedValue([restoredState, 3]),
    getHookRules: vi.fn().mockResolvedValue({
      'wfs.signal': [
        {
          to: 'waiter',
          conditions: { match: [{ expected: '{t1.output.data.signalId}', actual: '{$self.hook.data.id}' }] },
        },
      ],
    }),
    expireEscalationBySignalKey: vi.fn().mockResolvedValue(expireResult),
  };
  const engine = {
    appId: 'durable',
    namespace: 'durable',
    logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    store,
  };
  const config = {
    type: 'hook',
    subscribes: 'durable.wfs',
    ancestors: ['t1', 'cycle_hook', 'worker'],
    consumes: { t1: ['output/data/signalId'] },
    hook: { topic: 'wfs.signal' },
    sleep: '{worker.output.data.duration}',
    escalation: { role: '{worker.output.data.queueConfig.role}' },
  };
  const dispatchContext = {
    metadata: { guid: 'guid-1', jid: 'task-job-1', gid: 'gid-1', dad: ',0', aid: 'waiter' },
    data: { timestamp: 1234567890 },
  };
  const hook = new Hook(
    config as any,
    dispatchContext.data as any,
    { aid: 'waiter', atp: 'hook', stp: '', ac: '', au: '' } as any,
    null,
    engine as any,
    dispatchContext as any,
  );
  return { hook, store };
}

describe('Hook timeout path — delivery and settle detection', () => {
  it('hands back an expired accumulator row collection', async () => {
    const collection = { $accumulated: [{ itemKey: 'a', at: 'now' }], $trigger: 'timeout' };
    const { hook } = buildHook({
      entry: { id: 'row-1', status: 'expired', resolver_payload: collection },
      priorStatus: 'pending',
    });
    const result = await (hook as any).expireEscalationOnTimeout();
    expect(result).toEqual({ delivered: collection, settledElsewhere: false });
  });

  it('proceeds with no delivery when the expired row carries no payload', async () => {
    const { hook } = buildHook({
      entry: { id: 'row-1', status: 'expired', resolver_payload: null },
      priorStatus: 'pending',
    });
    expect(await (hook as any).expireEscalationOnTimeout()).toEqual({ settledElsewhere: false });
  });

  it('reports settledElsewhere when a resolve or cancel already moved the row', async () => {
    for (const priorStatus of ['resolved', 'cancelled', 'expired']) {
      const { hook } = buildHook({ entry: null, priorStatus });
      expect(await (hook as any).expireEscalationOnTimeout()).toEqual({ settledElsewhere: true });
    }
  });

  it('proceeds as a plain timeout when no row exists, in both result shapes', async () => {
    expect(await (buildHook({ entry: null, priorStatus: null }).hook as any).expireEscalationOnTimeout())
      .toEqual({ settledElsewhere: false });
    expect(await (buildHook(null).hook as any).expireEscalationOnTimeout())
      .toEqual({ settledElsewhere: false });
  });

  it('proceeds when the expiry lookup throws', async () => {
    const { hook, store } = buildHook(null);
    store.expireEscalationBySignalKey.mockRejectedValueOnce(new Error('boom'));
    expect(await (hook as any).expireEscalationOnTimeout()).toEqual({ settledElsewhere: false });
  });
});
