import { sleepFor } from "../../../../modules/utils";

export default async function greet(name: string): Promise<string> {
  await sleepFor(500);
  return `Hello, ${name}!`;
}
