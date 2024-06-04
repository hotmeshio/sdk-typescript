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
  oneTimeGreeting: {
    complex: string;
  };
  payload: { id: string; data: { hello: string; id: string } };
  proxyGreeting3: {
    complex: string;
  };
  proxyGreeting4: {
    complex: string;
  };
  jobId: string;
  jobBody: string;
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
  const oneTimeGreeting = await Durable.workflow //execIndex: 5
    .once<{ complex: string }>(activities.default, name);
  const durationInSeconds = await Durable.workflow //execIndex: 6
    .sleepFor('2 seconds');

  const response2 = await Durable.workflow //execIndex: 7
    .execChild({
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
  const payload = await Durable.workflow.waitFor<payloadType>('abcdefg'); //execIndex: 9

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
    Durable.workflow.execChild<string>({
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
    oneTimeGreeting,
    payload,
    proxyGreeting3,
    proxyGreeting4,
    jobId,
    jobBody,
  };
}

export async function childExample(name: string): Promise<string> {
  return `Hello from child workflow, ${name}!`;
}
