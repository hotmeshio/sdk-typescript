import { describe, it, expect, vi } from 'vitest';

import { Hook } from '../../../../services/activities/hook';

// ─────────────────────────────────────────────────────────────────────────────
// Timeout-path instance-state preservation
//
// dispatchTimeHook constructs the hook activity with a minimal context whose
// metadata.dad (the dimensional address) comes from the stream message — the
// message is the ONLY carrier of that value for this leg. The escalation-expiry
// step hydrates job state to derive the signal key, and getState() REPLACES
// instance.context with restored job state (which has no per-message dad).
//
// The invariant this suite pins: after expireEscalationOnTimeout() runs,
// processEvent must receive the IDENTICAL instance state dispatch built —
// same context object, same metadata.dad — or verifyReentry's own getState()
// reads dad as undefined, initDimensionalAddress clobbers the activity copy,
// and dimensional Leg2 notarization crashes (undefined.substring) for every
// wait that executes below the root (e.g. under an interceptor dimension).
// ─────────────────────────────────────────────────────────────────────────────

const DAD = ',0,3,0,0';
const SIGNAL_KEY = 'sig-task-123';

function buildHook() {
  // Restored job state, as the store returns it (flat paths → restoreHierarchy).
  // Carries the signal id the hook rule's pipe resolves — and NO dad.
  const restoredState = {
    'metadata/gid': 'gid-1',
    'metadata/jid': 'task-job-1',
    'metadata/js': 900000000000000,
    't1/output/data/signalId': SIGNAL_KEY,
  };

  const store = {
    getState: vi.fn().mockResolvedValue([restoredState, 3]),
    getHookRules: vi.fn().mockResolvedValue({
      // Two rules on the topic, as the durable factory registers: the rule
      // for a SIBLING branch first, this activity's second. Rule selection
      // must match on `to === aid`, never position.
      'wfs.signal': [
        {
          to: 'signaler_waiter',
          conditions: { match: [{ expected: '{missing.output.data.signalId}', actual: '{$self.hook.data.id}' }] },
        },
        {
          to: 'waiter',
          conditions: { match: [{ expected: '{t1.output.data.signalId}', actual: '{$self.hook.data.id}' }] },
        },
      ],
    }),
    expireEscalationBySignalKey: vi.fn().mockResolvedValue(null),
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

  // Exactly what dispatchTimeHook builds: metadata-only context, dad from the
  // stream message, plus the timehook payload.
  const dispatchContext = {
    metadata: { guid: 'guid-1', jid: 'task-job-1', gid: 'gid-1', dad: DAD, aid: 'waiter' },
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

  return { hook, store, dispatchContext };
}

describe('Hook timeout path — dispatch state preservation', () => {
  it('expireEscalationOnTimeout derives the key from hydrated state, then restores the pristine dispatch context', async () => {
    const { hook, store, dispatchContext } = buildHook();
    const originalMetadataDad = (hook as any).metadata.dad;

    await (hook as any).expireEscalationOnTimeout();

    // The hydration worked: the guarded UPDATE was addressed by the signal key
    // resolved from restored job state.
    expect(store.expireEscalationBySignalKey).toHaveBeenCalledWith(
      SIGNAL_KEY,
      'durable',
      'durable',
    );

    // The invariant: processEvent receives EXACTLY what dispatch built.
    expect((hook as any).context).toBe(dispatchContext);
    expect((hook as any).context.metadata.dad).toBe(DAD);
    expect((hook as any).metadata.dad).toBe(originalMetadataDad);
  });

  it('restores dispatch state even when the expiry lookup throws', async () => {
    const { hook, store, dispatchContext } = buildHook();
    store.expireEscalationBySignalKey.mockRejectedValueOnce(new Error('boom'));

    await (hook as any).expireEscalationOnTimeout();

    expect((hook as any).context).toBe(dispatchContext);
    expect((hook as any).context.metadata.dad).toBe(DAD);
  });
});
