import { Durable } from '../../../../services/durable';
import * as activities from './activities';

export async function codecWorkflow(
  name: string,
  value: number,
): Promise<Record<string, any>> {
  const { processData } = Durable.workflow.proxyActivities<typeof activities>({
    activities,
    retry: { maximumAttempts: 1 },
  });

  const result = await processData(name, value);
  return result;
}
