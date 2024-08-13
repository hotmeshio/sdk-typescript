import { sleepFor } from '../../../../modules/utils';

export default async function greet(name: string): Promise<string> {
  //NOTE: This is a standard node sleep timeout, not a `meshflow` 
  //      idempotent/transactional sleep.
  //      It's ok to use this here, but not in a workflow;
  await sleepFor(5_000);
  return `Hello, ${name}!`;
}
