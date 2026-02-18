import { MemFlow } from '../../../../services/memflow';

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
  payload: { id: string; data: { hello: string; id: string } };
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

const { greet } = MemFlow.workflow.proxyActivities<ActivitiesType>({
  activities,
});

export async function example(name: string): Promise<responseType> {
  const random1 = MemFlow.workflow.random(); //execIndex: 1
  const proxyGreeting = await greet(name); //execIndex: 2
  const proxyGreeting2 = await greet(`${name}2`); //execIndex: 3
  const random2 = MemFlow.workflow.random(); //execIndex: 4
  const durationInSeconds = await MemFlow.workflow //execIndex: 6
    .sleepFor('2 seconds');

  const response2 = await MemFlow.workflow //execIndex: 7
    .execChild({
      workflowName: 'childExample',
      args: [name],
      taskQueue: 'basic-world',
    });
  const response3 = await MemFlow.workflow //execIndex: 8
    .startChild({
      workflowName: 'childExample',
      args: [`start-${name}`],
      taskQueue: 'basic-world',
    });

  //pause...the test runner will send this signalId after sleeping for 10s
  const payload = await MemFlow.workflow.waitFor<payloadType>('abcdefg'); //execIndex: 9

  const [proxyGreeting3, proxyGreeting4] = await Promise.all([
    //execIndex: 10 (this execIndex is reassigned after collation is complete)
    greet(`${name}3`), //execIndex: 10 (first child in the promise inherits the collator id)
    greet(`${name}4`), //execIndex: 11
  ]);

  const [jobId, jobBody] = await Promise.all([
    MemFlow.workflow.startChild({
      workflowName: 'childExample',
      args: [`start-${name}x`],
      taskQueue: 'basic-world',
      workflowId: 'MyWorkflowId123',
    }),
    MemFlow.workflow.execChild<void>({
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
  return MemFlow.workflow.random();
}

/** Feature 2: proxy activity only */
export async function testProxyActivity(name: string): Promise<{ complex: string }> {
  return await greet(name);
}

/** Feature 3: sleepFor only */
export async function testSleep(name: string): Promise<string> {
  await MemFlow.workflow.sleepFor('1 second');
  return 'slept';
}

/** Feature 4: execChild only */
export async function testExecChild(name: string): Promise<string> {
  await MemFlow.workflow.execChild<void>({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return 'child-done';
}

/** Feature 5: startChild only */
export async function testStartChild(name: string): Promise<string> {
  const jobId = await MemFlow.workflow.startChild({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return jobId;
}

/** Feature 6: waitFor (signal) only */
export async function testWaitFor(name: string): Promise<any> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
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
  const payload = await MemFlow.workflow.waitFor('test-signal');
  return { greeting, payload };
}

// ──────────────────────────────────────────────
// Post-signal continuation workflows
// (the original failure happens AFTER waitFor completes)
// ──────────────────────────────────────────────

/** Feature 9: waitFor THEN single activity */
export async function testSignalThenActivity(name: string): Promise<{ payload: any; greeting: any }> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
  const greeting = await greet(name);
  return { payload, greeting };
}

/** Feature 10: waitFor THEN parallel activities */
export async function testSignalThenParallel(name: string): Promise<{ payload: any; results: any[] }> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
  const [r1, r2] = await Promise.all([
    greet(`${name}1`),
    greet(`${name}2`),
  ]);
  return { payload, results: [r1, r2] };
}

/** Feature 11: waitFor THEN execChild */
export async function testSignalThenExecChild(name: string): Promise<{ payload: any; childResult: string }> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
  await MemFlow.workflow.execChild<void>({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return { payload, childResult: 'child-done' };
}

/** Feature 12: waitFor THEN startChild */
export async function testSignalThenStartChild(name: string): Promise<{ payload: any; jobId: string }> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
  const jobId = await MemFlow.workflow.startChild({
    workflowName: 'childExample',
    args: [name],
    taskQueue: 'basic-world',
  });
  return { payload, jobId };
}

/** Feature 13: Promise.all([startChild, execChild]) without preceding waitFor */
export async function testParallelChildren(name: string): Promise<{ jobId: string }> {
  const [jobId, _body] = await Promise.all([
    MemFlow.workflow.startChild({
      workflowName: 'childExample',
      args: [`${name}-start`],
      taskQueue: 'basic-world',
    }),
    MemFlow.workflow.execChild<void>({
      workflowName: 'childExample',
      args: [`${name}-exec`],
      taskQueue: 'basic-world',
    }),
  ]);
  return { jobId };
}

/** Feature 14: waitFor THEN Promise.all([startChild, execChild]) — mirrors exact failure */
export async function testSignalThenParallelChildren(name: string): Promise<{ payload: any; jobId: string }> {
  const payload = await MemFlow.workflow.waitFor('test-signal');
  const [jobId, _body] = await Promise.all([
    MemFlow.workflow.startChild({
      workflowName: 'childExample',
      args: [`${name}-start`],
      taskQueue: 'basic-world',
    }),
    MemFlow.workflow.execChild<void>({
      workflowName: 'childExample',
      args: [`${name}-exec`],
      taskQueue: 'basic-world',
    }),
  ]);
  return { payload, jobId };
}
