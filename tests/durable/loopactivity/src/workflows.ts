import { Durable } from '../../../../services/durable';
import type * as activities from './activities';

const { greet } = Durable.workflow.proxyActivities<typeof activities>();

export async function example(name: string): Promise<string[]> {
  return await Promise.all([greet('1'), greet('2'), greet('3')]);
}
