import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { myFatalActivity } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

async function example({ name }: Record<'name', string>): Promise<void> {
  return await myFatalActivity(name);
}

export default { example };
