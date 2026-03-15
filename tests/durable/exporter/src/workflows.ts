import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { workflow } = Durable;

/**
 * Test workflow that calls multiple activities to verify input enrichment.
 */
export async function testWorkflow(
  name: string,
  value: number,
): Promise<{ greeting: string; doubled: number }> {
  const { greet, doubleValue } = workflow.proxyActivities<typeof activities>({
    activities,
  });

  const greeting = await greet(name);
  const doubled = await doubleValue(value);

  return { greeting, doubled };
}
