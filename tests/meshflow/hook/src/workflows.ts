import { MeshFlow } from '../../../../services/meshflow';

import * as activities from './activities';

const { greet, bye } = MeshFlow.workflow.proxyActivities<typeof activities>({
  activities,
});

/**
 * This is the main workflow function that will be executed. The workflow
 * will end and self-clean once the final statement executes. While it
 * is active other hook functions may also run in parallel. Once it ends
 * no hook functions may run.
 *
 * @param {string} name
 * @returns {Promise<string>}
 */
export async function example(name: string): Promise<string> {
  //create a search session and add some job data (this is NOT the same as job state)
  const search = await MeshFlow.workflow.search();
  await search.set('custom1', 'meshflow');
  await search.set('custom2', '55');
  //note: `exampleHook` function will change to 'jackson'
  await search.set('jimbo', 'jones');
  await search.incr('counter', 10);
  await search.incr('counter', 1);
  await search.mult('multer', 12);
  await search.mult('multer', 10);

  //start a child workflow and wait for the result
  await MeshFlow.workflow.execChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
  });

  //start a child workflow and only confirm it started (don't wait for result)
  await MeshFlow.workflow.startChild({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
  });

  //call a few activities in parallel (proxyActivities)
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);

  //wait for the `abcdefg` signal ('exampleHook' will send it)
  await MeshFlow.workflow.waitFor('abcdefg');

  //sleep for 5
  await MeshFlow.workflow.sleepFor('5 seconds');

  //return the result (the job state)
  return `${hello} - ${goodbye}`;
}

/**
 * This is a hook function that can be used to update the shared workflow state
 * The function will run in a separate thread and has no blocking effect
 * on the main thread outside of the signal command that is being used in
 * this test. This function is called by the test runner a few seconds
 * after it starts the main workflow.
 * @param {string} name
 */
export async function exampleHook(name: string): Promise<void> {
  //update shared job state (the workflow HASH)
  const search = await MeshFlow.workflow.search();
  await search.incr('counter', 100);
  await search.set('jimbo', 'jackson');

  //Promise.all: call in parallel; use a sleepFor
  // and compare to an activity that uses a standard
  const [greeting, _timeInSeconds] = await Promise.all([
    bye(name, 1_000),
    MeshFlow.workflow.sleepFor('1 second'),
  ]);

  //start a child workflow and wait for the result
  await MeshFlow.workflow.execChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
  });

  //start a child workflow and only confirm it started (don't wait for result)
  await MeshFlow.workflow.startChild({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
  });

  //test out sleeping
  await MeshFlow.workflow.sleepFor('2 seconds');

  //awake the parent/main thread by sending the 'abcdefg' signal
  await MeshFlow.workflow.signal('abcdefg', { data: greeting });
}
