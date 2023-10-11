import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { count } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

async function example({ amount }): Promise<number> {
  return await count(amount);
}

export default { example };
