import { Durable } from '../../../../services/durable';
import * as activities from './activities';

// NOTE: when `./activities` exports a `default` function,
//      it is imported as shown here (using the type)
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = Durable.workflow
  .proxyActivities<ActivitiesType>({ activities });

export async function example(name: string): Promise<string> {
  const random1 = Durable.workflow.random();                   //execIndex: 1
  const proxyGreeting = await greet(name);                     //execIndex: 2
  const proxyGreeting2 = await greet(`${name}2`);              //execIndex: 3
  const random2 = Durable.workflow.random();                   //execIndex: 4
  const oneTimeGreeting = await Durable.workflow               //execIndex: 5
    .once<{complex: string}>(activities.default, name);
  const durationInSeconds = await Durable.workflow             //execIndex: 6
    .sleepFor('2 seconds');
  console.log('response and duration >', proxyGreeting2, durationInSeconds);

  const response2 = await Durable.workflow                     //execIndex: 7
    .executeChild({
      workflowName: 'childExample',
      args: [name],
      taskQueue: 'basic-world'
    });
  const response3 = await Durable.workflow                     //execIndex: 8
    .startChild({
      workflowName: 'childExample',
      args: [`start-${name}`],
      taskQueue: 'basic-world'
    });
  console.log('response2 >', JSON.stringify(response2), 'response3 >', JSON.stringify(response3));

  //pause...the test runner will send this signalId after sleeping for 10s
  type payloadType = {id: string, data: {hello: string, id: string}};
  const payload = await Durable.workflow
    .waitFor<payloadType>('abcdefg');                          //execIndex: 9
  console.log('payload >', payload);

  const [proxyGreeting3, proxyGreeting4] = await Promise.all([ //execIndex: 10 (this execIndex is reassigned after collation is complete)
    greet(`${name}3`),                                         //execIndex: 10 (first child in the promise inherits the collator id)
    greet(`${name}4`),                                         //execIndex: 11
  ]);
  console.log('greet3 >', proxyGreeting3, 'greet4 >', proxyGreeting4);

  const [jobidx, jobBody] = await Promise.all([
    Durable.workflow.startChild({
      workflowName: 'childExample',
      args: [`start-${name}x`],
      taskQueue: 'basic-world'
    }),
    Durable.workflow.executeChild({
      workflowName: 'childExample',
      args: [`start-${name}y`],
      taskQueue: 'basic-world'
    }),
  ]);
  console.log('jobidx >', JSON.stringify(jobidx), 'jobBody >', JSON.stringify(jobBody));

  return `${random1} ${JSON.stringify(proxyGreeting)} ${random2} ${JSON.stringify(oneTimeGreeting)} ${payload.data.hello}`;
}

export async function childExample(name: string): Promise<string> {
  return `Hello from child workflow, ${name}!`;
}
