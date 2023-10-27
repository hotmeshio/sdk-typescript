import { sleepFor } from "../../../../modules/utils";

export default async function greet(name: string): Promise<string> {
  //NOTE: This is a standard node sleep timeout, not a `durable` sleep.
  //      It's ok to use this here, but not in a workflow; workflows must
  //      run determistically, since they are black boxes and must
  //      provide reliable execution, even if the internals are unknown.
  await sleepFor(1_000);
  return `Hello, ${name}!`;
}
