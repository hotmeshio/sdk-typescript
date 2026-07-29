import { Durable } from '../../../../services/durable';

// Pauses until a signal arrives, writing an hmsh_escalations row at suspension.
export async function approvalWorkflow(orderId: string, region: string): Promise<{ approved: boolean; approvedBy: string }> {
  const signalId = `approval-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean; approvedBy: string }>(
    signalId,
    {
      role: 'approver',
      type: 'order-approval',
      subtype: 'regional',
      priority: 2,
      description: `Approve order ${orderId}`,
      metadata: { orderId, region },
      envelope: { instructions: 'Review and approve or reject the order' },
    },
  );
  return result as { approved: boolean; approvedBy: string };
}

// One wait, two outcomes: an escalation row AND a resume timer. Resolving the
// row delivers the payload; the timer firing first resumes with false (and the
// engine transitions the row to 'expired').
export async function slaGatedWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: 'resolved' | 'timed-out'; payload: unknown }> {
  const signalId = `sla-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean }>(
    signalId,
    {
      role: 'sla-approver',
      type: 'sla-gate',
      priority: 2,
      description: `SLA-gated approval for ${orderId}`,
      metadata: { orderId },
      timeout,
    },
  );
  return result === false
    ? { outcome: 'timed-out', payload: false }
    : { outcome: 'resolved', payload: result };
}

// The SLA wait AFTER other durable operations — each op cycles the main loop,
// so the waiter executes at a cycled dimensional address (dad ',0,N,0,0').
// This is the interceptor-wrapped host-app topology: the timeout leg must hand
// processEvent the pristine dispatch context or dimensional Leg2 notarization
// crashes on the corrupted address.
export async function slaCycledWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: 'resolved' | 'timed-out'; payload: unknown }> {
  // Two durable ops advance the cycle index before the wait arms.
  await Durable.workflow.sleep('1 second');
  await Durable.workflow.sleep('1 second');

  const signalId = `sla-cycled-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved: boolean }>(signalId, {
    role: 'sla-cycled-approver',
    type: 'sla-cycled-gate',
    priority: 2,
    description: `Cycled SLA gate for ${orderId}`,
    metadata: { orderId },
    timeout,
  });
  return result === false
    ? { outcome: 'timed-out', payload: false }
    : { outcome: 'resolved', payload: result };
}

// Promise.all kicks the waits out to the COLLATOR flow — the second reentry
// path for signal/escalation delivery (wfs.signal → collator_waiter), with
// per-item dimensions. Item A is SLA-gated; item B is open-ended: A's timer
// must expire only A's row and settle A with false while B keeps waiting,
// and Promise.all completes once B resolves.
export async function slaCollatedWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ a: string; b: string; payloadB: unknown }> {
  const sigA = `sla-col-a-${Durable.guid()}`;
  const sigB = `sla-col-b-${Durable.guid()}`;
  const [gated, open] = await Promise.all([
    Durable.workflow.condition<{ approved: boolean }>(sigA, {
      role: 'sla-collated-approver',
      type: 'sla-collated-gate',
      priority: 2,
      description: `Collated SLA gate A for ${orderId}`,
      metadata: { orderId, item: 'a' },
      timeout,
    }),
    Durable.workflow.condition<{ approved: boolean }>(sigB, {
      role: 'sla-collated-approver',
      type: 'sla-collated-gate',
      priority: 2,
      description: `Collated open gate B for ${orderId}`,
      metadata: { orderId, item: 'b' },
    }),
  ]);
  return {
    a: gated === false ? 'timed-out' : 'resolved',
    b: open === false ? 'timed-out' : open === null ? 'cancelled' : 'resolved',
    payloadB: open,
  };
}

// Runs INSIDE a hook function (spawned via execHook) — the signaler-branch
// waiter, the deepest dimensional topology.
export async function slaGateHook(orderId: string, timeout: string): Promise<void> {
  const signalId = `sla-hook-${orderId}`;
  const result = await Durable.workflow.condition<{ approved?: boolean }>(signalId, {
    role: 'sla-hook-approver',
    type: 'sla-hook-gate',
    priority: 3,
    description: `Dimensional SLA gate for ${orderId}`,
    metadata: { orderId },
    timeout,
  });
  await Durable.workflow.signal(`sla-hook-done-${orderId}`, {
    outcome: result === false ? 'timed-out' : result === null ? 'cancelled' : 'resolved',
    payload: result === false || result === null ? {} : result,
  });
}

// The parent: spawns the SLA gate as a hook (same job, new dimension) and
// awaits its verdict.
export async function slaHookGatedWorkflow(
  orderId: string,
  timeout: string,
): Promise<{ outcome: string; payload: unknown }> {
  const gate = await Durable.workflow.execHook<{ outcome: string; payload: unknown }>({
    taskQueue: 'escalation-test',
    workflowName: 'slaGateHook',
    args: [orderId, timeout],
    signalId: `sla-hook-done-${orderId}`,
  });
  return gate;
}

// Parks on condition() as one member of a gang — resolveAllOrNone() hands
// each member its own mandate; the workflow returns exactly the payload it
// received so the test can prove per-row delivery.
export async function gangMemberWorkflow(gangId: string, unitId: string): Promise<unknown> {
  const signalId = `gang-${gangId}-${unitId}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(
    signalId,
    {
      role: 'printer',
      type: 'printer-availability',
      priority: 2,
      description: `Gang ${gangId} member ${unitId}`,
      metadata: { gangId, unitId },
    },
  );
  return result;
}

// Born pre-assigned: the escalation row is created already routed to a named
// user (assigned_to set, no TTL window) in the same Leg1 commit as the wait.
export async function preAssignedWorkflow(
  orderId: string,
  assignee: string,
  parentId: string,
): Promise<unknown> {
  const signalId = `pre-assigned-${Durable.guid()}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'handoff-approver',
    type: 'handoff-soft',
    priority: 2,
    description: `Hand-off for ${orderId}`,
    metadata: { orderId },
    assignee,
    parentId,
  });
  return result;
}

// Born hard-claimed: assignee + durationMinutes arms the claim TTL window at
// creation, locking the row to the assignee exactly as a post-create claim().
export async function hardClaimWorkflow(
  orderId: string,
  assignee: string,
  durationMinutes: number,
): Promise<unknown> {
  const signalId = `hard-claim-${Durable.guid()}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'handoff-approver',
    type: 'handoff-hard',
    priority: 2,
    description: `Locked hand-off for ${orderId}`,
    metadata: { orderId },
    assignee,
    durationMinutes,
  });
  return result;
}

// Born-assigned inside Promise.all — proves the collator_waiter path writes
// the assignment. Item A is assigned; item B is a plain open wait.
export async function collatedAssignedWorkflow(
  orderId: string,
  assignee: string,
): Promise<{ a: unknown; b: unknown }> {
  const sigA = `col-assigned-a-${Durable.guid()}`;
  const sigB = `col-assigned-b-${Durable.guid()}`;
  const [a, b] = await Promise.all([
    Durable.workflow.condition<Record<string, unknown>>(sigA, {
      role: 'handoff-collated',
      type: 'handoff-collated',
      priority: 2,
      metadata: { orderId, item: 'a' },
      assignee,
    }),
    Durable.workflow.condition<Record<string, unknown>>(sigB, {
      role: 'handoff-collated',
      type: 'handoff-collated',
      priority: 2,
      metadata: { orderId, item: 'b' },
    }),
  ]);
  return { a, b };
}

// Born-assigned inside a hook function (execHook) — proves the signaler-branch
// waiter path writes the assignment. Resolution delivery targets the main
// flow's waiter (see condition() placement docs), so this gate completes via
// its SLA timer and reports back through signal(), like slaGateHook.
export async function assignedGateHook(
  orderId: string,
  assignee: string,
  timeout: string,
): Promise<void> {
  const signalId = `assigned-hook-${orderId}`;
  const result = await Durable.workflow.condition<Record<string, unknown>>(signalId, {
    role: 'handoff-hook',
    type: 'handoff-hook',
    priority: 3,
    metadata: { orderId },
    assignee,
    timeout,
  });
  await Durable.workflow.signal(`assigned-hook-done-${orderId}`, {
    outcome: result === false ? 'timed-out' : 'resolved',
  });
}

// The parent: spawns the assigned gate as a hook and awaits its verdict.
export async function assignedHookParent(
  orderId: string,
  assignee: string,
  timeout: string,
): Promise<{ outcome: string }> {
  return Durable.workflow.execHook<{ outcome: string }>({
    taskQueue: 'escalation-test',
    workflowName: 'assignedGateHook',
    args: [orderId, assignee, timeout],
    signalId: `assigned-hook-done-${orderId}`,
  });
}

// Pauses at a condition and returns null when the escalation is cancelled,
// or the resolver payload when resolved normally.
export async function cancelAwareWorkflow(orderId: string): Promise<{ approved?: boolean; __escalation_cancelled?: boolean } | null> {
  const signalId = `cancel-aware-${Durable.guid()}`;
  const result = await Durable.workflow.condition<{ approved?: boolean }>(
    signalId,
    {
      role: 'cancel-test-approver',
      type: 'cancel-test',
      priority: 5,
      metadata: { orderId },
    },
  );
  // null = escalation was cancelled; pass it through so the test can assert it
  return result as { approved?: boolean } | null;
}
