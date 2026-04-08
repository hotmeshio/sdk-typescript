import { sleepFor } from '../../../../modules/utils';

export default async function greet(
  name: string,
): Promise<{ complex: string }> {
  await sleepFor(500);
  return { complex: `Basic, ${name}!` };
}

/** A remote activity that simulates a VNF on a separate server.
 *  Only the type signature is imported by the workflow. */
export async function remoteProcess(data: string): Promise<string> {
  return `vnf-processed:${data}`;
}
