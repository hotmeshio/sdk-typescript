import { sleepFor } from "../../../../modules/utils";

export default async function greet(name: string): Promise<string> {
  await sleepFor(1_000);
  return `Hello, ${name}!`;
}
