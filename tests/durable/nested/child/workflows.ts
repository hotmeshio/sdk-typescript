import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { childActivity } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

export async function childExample(name: string): Promise<string> {
  return await childActivity(name);
}
