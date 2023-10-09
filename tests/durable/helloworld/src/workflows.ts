import { Durable } from '../../../../services/durable';

//when activity exports a default function, it is imported as shown here
import type greetFunctionType from './activities';
type ActivitiesType = {
  greet: typeof greetFunctionType;
};

const { greet } = Durable.workflow.proxyActivities<ActivitiesType>();

export async function example(name: string): Promise<string> {
  return await greet(name);
}
