import { sleepFor } from '../../../../modules/utils';

export default async function greet(
  name: string,
): Promise<{ complex: string }> {
  await sleepFor(500);
  return { complex: `Basic, ${name}!` };
}
