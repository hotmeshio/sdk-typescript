import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { greet, bye } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

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
  //create a search session and add some values
  const search = await Durable.workflow.search();
  await search.set('custom1', 'durable');
  await search.set('custom2', '55');
  //the hook function will update this value to 'jackson'
  await search.set('jimbo', 'jones');

  const childWorkflowOutput = await Durable.workflow.executeChild<string>({
    args: [`${name} to CHILD`],
    taskQueue: 'child-world',
    workflowName: 'childExample',
    workflowId: '-'
  });
  console.log('childWorkflowOutput is=>', childWorkflowOutput);

  //call a few activities in parallel (proxyActivities)
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);

  //bind some more search data to workflow state (multiply, increment)
  await search.get('jimbo');
  await search.incr('counter', 10);
  await search.incr('counter', 1);
  await search.get('counter');
  await search.mult('multer', 12);
  await search.mult('multer', 10);

  //wait for the `abcdefg` signal ('exampleHook' will send it)
  const [signal1] = await Durable.workflow.waitForSignal(['abcdefg']);
  console.log('awakened with signal=>', signal1);
  console.log('jimbo should be jackson=>', await search.get('jimbo'));

  //sleep for 5 and then return
  await Durable.workflow.sleep('5 seconds');
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
  const search = await Durable.workflow.search();
  await search.incr('counter', 100);
  await search.set('jimbo', 'jackson');
  const greeting = await bye(name);

  //test out sleeping
  await Durable.workflow.sleep('1 second');

  //awake the parent/main thread by sending the 'abcdefg' signal
  await Durable.workflow.signal('abcdefg', { data: greeting });
}
