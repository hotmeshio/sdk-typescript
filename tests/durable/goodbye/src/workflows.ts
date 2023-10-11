import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { greet, bye } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

export async function example(name: string): Promise<string> {
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);
  return `${hello} - ${goodbye}`;
}
