import { Durable } from '../../../../services/durable';
import type * as activities from './activities';

const { count } = Durable.workflow.proxyActivities<typeof activities>();

async function example(): Promise<number> {
  return await count();
}

export default { example };
