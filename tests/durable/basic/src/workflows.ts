import { Durable } from '../../../../services/durable';

import * as activities from './activities';

// NOTE: when `./activities` exports a `default` function,
//      it is imported as shown here (using the type)
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

type responseType = {
  random1: number;
  proxyGreeting: {
    complex: string;
  };
  random2: number;
  payload: false | { id: string; data: { hello: string; id: string } };
  proxyGreeting3: {
    complex: string;
  };
  proxyGreeting4: {
    complex: string;
  };
  jobId: string;
  jobBody: void;
};

type payloadType = {
  id: string;
  data: {
    hello: string;
    id: string;
  };
};

const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<responseType> {
  const random1 = Durable.workflow.random(); //execIndex: 1
  const proxyGreeting = await greet(name); //execIndex: 2
  const proxyGreeting2 = await greet(`${name}2`); //execIndex: 3
  const random2 = Durable.workflow.random(); //execIndex: 4
  const durationInSeconds = await Durable.workflow //execIndex: 6
    .sleep('2 seconds');

  const response2 = await Durable.workflow //execIndex: 7
    .executeChild({
      workflowName: 'childExample',
      args: [name],
      taskQueue: 'basic-world',
    });
  const response3 = await Durable.workflow //execIndex: 8
    .startChild({
      workflowName: 'childExample',
      args: [`start-${name}`],
      taskQueue: 'basic-world',
    });

  //pause...the test runner will send this signalId after sleeping for 10s
  const payload = await Durable.workflow.condition<payloadType>('abcdefg'); //execIndex: 9

  const [proxyGreeting3, proxyGreeting4] = await Promise.all([
    //execIndex: 10 (this execIndex is reassigned after collation is complete)
    greet(`${name}3`), //execIndex: 10 (first child in the promise inherits the collator id)
    greet(`${name}4`), //execIndex: 11
  ]);

  const [jobId, jobBody] = await Promise.all([
    Durable.workflow.startChild({
      workflowName: 'childExample',
      args: [`start-${name}x`],
      taskQueue: 'basic-world',
      workflowId: 'MyWorkflowId123',
    }),
    Durable.workflow.executeChild<void>({
      workflowName: 'childExample',
      args: [`start-${name}y`],
      taskQueue: 'basic-world',
    }),
  ]);

  //return structured response
  return {
    random1,
    proxyGreeting,
    random2,
    payload,
    proxyGreeting3,
    proxyGreeting4,
    jobId,
    jobBody,
  };
}

export async function childExample(name: string): Promise<void> {
  return;
}

// ──────────────────────────────────────────────
// Feature isolation workflows
// ──────────────────────────────────────────────

/** Feature 1: random only */
export async function testRandom(name: string): Promise<number> {
  return Durable.workflow.random();
}

/** Feature 2: proxy activity only */
export async function testProxyActivity(name: string): Promise<{ complex: string }> {
  return await greet(name);
}

/** Feature 3: sleep only */
export async function testSleep(name: string): Promise<string> {
  await Durable.workflow.sleep('1 second');
  return 'slept';
}

/** Feature 4: execChild only */
export async function testExecChild(name: string): Promise<string> {
  await Durable.workflow.executeChild<void>({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return 'child-done';
}

/** Feature 5: startChild only */
export async function testStartChild(name: string): Promise<string> {
  const jobId = await Durable.workflow.startChild({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return jobId;
}

/** Feature 6: waitFor (signal) only */
export async function testWaitFor(name: string): Promise<any> {
  const payload = await Durable.workflow.condition('test-signal');
  return payload;
}

/** Feature 7: Promise.all with proxy activities */
export async function testParallelActivities(name: string): Promise<{ complex: string }[]> {
  const [r1, r2] = await Promise.all([
    greet(`${name}1`),
    greet(`${name}2`),
  ]);
  return [r1, r2];
}

/** Feature 8: activity THEN waitFor */
export async function testActivityThenSignal(name: string): Promise<{ greeting: any; payload: any }> {
  const greeting = await greet(name);
  const payload = await Durable.workflow.condition('test-signal');
  return { greeting, payload };
}

// ──────────────────────────────────────────────
// Post-signal continuation workflows
// (the original failure happens AFTER waitFor completes)
// ──────────────────────────────────────────────

/** Feature 9: waitFor THEN single activity */
export async function testSignalThenActivity(name: string): Promise<{ payload: any; greeting: any }> {
  const payload = await Durable.workflow.condition('test-signal');
  const greeting = await greet(name);
  return { payload, greeting };
}

/** Feature 10: waitFor THEN parallel activities */
export async function testSignalThenParallel(name: string): Promise<{ payload: any; results: any[] }> {
  const payload = await Durable.workflow.condition('test-signal');
  const [r1, r2] = await Promise.all([
    greet(`${name}1`),
    greet(`${name}2`),
  ]);
  return { payload, results: [r1, r2] };
}

/** Feature 11: waitFor THEN execChild */
export async function testSignalThenExecChild(name: string): Promise<{ payload: any; childResult: string }> {
  const payload = await Durable.workflow.condition('test-signal');
  await Durable.workflow.executeChild<void>({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return { payload, childResult: 'child-done' };
}

/** Feature 12: waitFor THEN startChild */
export async function testSignalThenStartChild(name: string): Promise<{ payload: any; jobId: string }> {
  const payload = await Durable.workflow.condition('test-signal');
  const jobId = await Durable.workflow.startChild({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return { payload, jobId };
}

/** Feature 13: Promise.all([startChild, execChild]) without preceding waitFor */
export async function testParallelChildren(name: string): Promise<{ jobId: string }> {
  const [jobId, _body] = await Promise.all([
    Durable.workflow.startChild({
      workflowName: 'childExample',
      args: [`${name}-start`],
      taskQueue: 'basic-world',
    }),
    Durable.workflow.executeChild<void>({
      workflowName: 'childExample',
      args: [`${name}-exec`],
      taskQueue: 'basic-world',
    }),
  ]);
  return { jobId };
}

/** Feature 14: waitFor THEN Promise.all([startChild, execChild]) — mirrors exact failure */
export async function testSignalThenParallelChildren(name: string): Promise<{ payload: any; jobId: string }> {
  const payload = await Durable.workflow.condition('test-signal');
  const [jobId, _body] = await Promise.all([
    Durable.workflow.startChild({
      workflowName: 'childExample',
      args: [`${name}-start`],
      taskQueue: 'basic-world',
    }),
    Durable.workflow.executeChild<void>({
      workflowName: 'childExample',
      args: [`${name}-exec`],
      taskQueue: 'basic-world',
    }),
  ]);
  return { payload, jobId };
}

/** Feature 15: Worker-registered activities.
 *  proxyActivities uses type-only reference — no activities object passed. */
export async function testWorkerRegisteredActivity(name: string): Promise<{ complex: string }> {
  const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
    retry: { maximumAttempts: 1, throwOnError: true },
  });
  return await greet(name);
}

/** Feature 16: VNF-style remote activity — type-only proxy with explicit taskQueue.
 *  The activity runs on a separate worker registered with a different task queue. */
export async function testVnfActivity(name: string): Promise<string> {
  const { remoteProcess } = Durable.workflow.proxyActivities<{
    remoteProcess: (data: string) => Promise<string>;
  }>({
    taskQueue: 'vnf-activities',
    retry: { maximumAttempts: 1, throwOnError: true },
  });
  return await remoteProcess(name);
}

/** Feature 17: Late worker registration — proves the router's replay mechanism.
 *  This workflow is started BEFORE its worker is registered. The router re-publishes
 *  the message with a 500ms visibility delay until the worker registers. */
export async function testLateRegistration(name: string): Promise<{ complex: string }> {
  const { greet } = Durable.workflow.proxyActivities<ActivitiesType>({
    activities,
    retry: { maximumAttempts: 1, throwOnError: true },
  });
  return await greet(name);
}

/** A child whose whole body is a single `condition()` — the unit a fan-in harvest spawns. */
export async function childWaitFor(signalId: string): Promise<any> {
  return await Durable.workflow.condition(signalId);
}

/** Feature 18: Promise.all of `executeChild`, each child a single `condition()` resolved by
 *  an EXTERNAL signal. This is the supported way to collect N independent durable waits in
 *  parallel — fan them out as child workflows (each one condition) rather than awaiting
 *  concurrent conditions in a single execution (which races the wait registration). Each
 *  child is given a fixed workflowId so the test can signal it directly. */
export async function testParallelConditionChildren(name: string): Promise<{ a: any; b: any }> {
  const [a, b] = await Promise.all([
    Durable.workflow.executeChild<any>({
      workflowName: 'childWaitFor',
      args: ['child-sig-a'],
      taskQueue: 'basic-world',
      workflowId: 'cond-child-a',
    }),
    Durable.workflow.executeChild<any>({
      workflowName: 'childWaitFor',
      args: ['child-sig-b'],
      taskQueue: 'basic-world',
      workflowId: 'cond-child-b',
    }),
  ]);
  return { a, b };
}

/** Feature 19: Promise.all of two DIRECT `condition()` calls — the canonical fan-in
 *  documented on `condition()`. Two wait items (code 595) are collated into the reentrant
 *  collator flow and BOTH must resolve through `collator_waiter` as each external signal
 *  arrives. This exercises the collator's waiter branch directly — unlike fanning the waits
 *  out as child workflows, where each child instead runs a lone inline `waiter`. */
export async function testParallelConditions(sigA: string, sigB: string): Promise<{ a: any; b: any }> {
  const [a, b] = await Promise.all([
    Durable.workflow.condition<any>(sigA),
    Durable.workflow.condition<any>(sigB),
  ]);
  return { a, b };
}

/** Feature 20: Promise.all mixing a `condition()` with a `proxyActivity()`. The collator
 *  must collect a wait (595) alongside a proxy (591) and return both in original order; the
 *  parent does not end until the external signal resolves the wait. */
export async function testConditionPlusProxy(
  name: string,
  sig: string,
): Promise<{ waited: any; greeting: { complex: string } }> {
  const [waited, greeting] = await Promise.all([
    Durable.workflow.condition<any>(sig),
    greet(name),
  ]);
  return { waited, greeting };
}

/** Feature 21: Promise.all mixing a `condition()` with an `executeChild()`. The collator
 *  must collect a wait (595) alongside a child (590) and return both in original order. */
export async function testConditionPlusChild(
  name: string,
  sig: string,
): Promise<{ waited: any; childDone: boolean }> {
  const [waited, _child] = await Promise.all([
    Durable.workflow.condition<any>(sig),
    Durable.workflow.executeChild<void>({
      workflowName: 'childExample',
      args: [name],
      taskQueue: 'basic-world',
    }),
  ]);
  return { waited, childDone: true };
}

/** Feature 22: Promise.all mixing a `condition()` that carries an ESCALATION queueConfig
 *  with a `proxyActivity()`. The collated wait must write its `hmsh_escalations` row at
 *  suspension (so it is listable/claimable/resolvable) and resume when the escalation is
 *  resolved — proving `collator_waiter` honors the escalation config exactly like the inline
 *  single-condition `waiter` does. The `signalId` is derived from `orderId` so it is stable
 *  across replays; the test discovers the row by `role` + `metadata.orderId`. */
export async function testConditionEscalationPlusProxy(
  name: string,
  orderId: string,
  role: string,
): Promise<{ decision: any; greeting: { complex: string } }> {
  const signalId = `collator-esc-${orderId}`;
  const [decision, greeting] = await Promise.all([
    Durable.workflow.condition<any>(signalId, {
      role,
      type: 'collator-escalation',
      priority: 3,
      description: `Approve collated order ${orderId}`,
      metadata: { orderId },
    }),
    greet(name),
  ]);
  return { decision, greeting };
}

/** Lineage: a child that itself spawns a grandchild. Proves `origin_id` stays pinned to the
 *  root across composition depth while `parent_id` always tracks the direct spawner. */
export async function testLineageChild(grandchildId: string): Promise<{ done: boolean }> {
  await Durable.workflow.executeChild<void>({
    workflowName: 'childExample',
    args: ['grandchild'],
    taskQueue: 'basic-world',
    workflowId: grandchildId,
  });
  return { done: true };
}

/** Lineage root: spawns a child (which spawns a grandchild) through the `childer` path.
 *  The full root → child → grandchild chain must be reconstructable from the jobs table. */
export async function testLineageRoot(
  childId: string,
  grandchildId: string,
): Promise<{ done: boolean }> {
  await Durable.workflow.executeChild<void>({
    workflowName: 'testLineageChild',
    args: [grandchildId],
    taskQueue: 'basic-world',
    workflowId: childId,
  });
  return { done: true };
}

/** Lineage via the collator: `Promise.all([executeChild, condition])` routes the child through
 *  `collator_childer`. The child's `parent_id` must be THIS spawning workflow — not the
 *  synthetic collator (`$C`) job that internally fans the work out. */
export async function testLineageCollator(
  childId: string,
  sig: string,
): Promise<{ done: boolean }> {
  await Promise.all([
    Durable.workflow.executeChild<void>({
      workflowName: 'childExample',
      args: ['collated-child'],
      taskQueue: 'basic-world',
      workflowId: childId,
    }),
    Durable.workflow.condition<any>(sig),
  ]);
  return { done: true };
}

/** Lineage nesting THROUGH the collator: the collated child (spawned via `collator_childer`)
 *  is itself a composer that spawns a grandchild. Proves `origin_id` stays pinned to the root
 *  and `parent_id` keeps chaining even when the middle hop is a Promise.all/collator fan-out —
 *  i.e. lineage travels through the synthetic collator job without absorbing its id. */
export async function testLineageCollatorNested(
  childId: string,
  grandchildId: string,
  sig: string,
): Promise<{ done: boolean }> {
  await Promise.all([
    Durable.workflow.executeChild<void>({
      workflowName: 'testLineageChild', // this collated child spawns a grandchild
      args: [grandchildId],
      taskQueue: 'basic-world',
      workflowId: childId,
    }),
    Durable.workflow.condition<any>(sig),
  ]);
  return { done: true };
}

/** Feature 23: a collated `Promise.all([condition, condition])` inside a CONTINUED generation
 *  (post-`continueAsNew`). The first execution immediately continues into a "harvest" generation
 *  that fans two conditions into the collator. A continued generation's replay dimension is the
 *  generation prefix (`$N`), not the empty first-execution dimension — so the collator must be
 *  emitted with that same dimension for the harvested waits to resume. Mirrors Feature 19, but
 *  one continueAsNew hop in. */
export async function testContinueAsNewThenParallelConditions(
  phase: string,
  sigA: string,
  sigB: string,
): Promise<{ a: any; b: any }> {
  if (phase === 'first') {
    // First execution: immediately restart into the harvest generation.
    await Durable.workflow.continueAsNew('harvest', sigA, sigB);
  }
  // Continued generation: collated parallel waits.
  const [a, b] = await Promise.all([
    Durable.workflow.condition<any>(sigA),
    Durable.workflow.condition<any>(sigB),
  ]);
  return { a, b };
}
