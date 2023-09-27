import { sleepFor } from "../../../../modules/utils";

export async function greet(name: string): Promise<string> {
  await sleepFor(5_000);
  return `Hello, ${name}!`;
}
