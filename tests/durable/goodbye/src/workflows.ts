import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { greet, bye } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

export async function example(name: string): Promise<string> {
  //set values in the durable workflow data store (these are indexed)
  await Durable.workflow.data('set', 'custom1', 'durable');
  await Durable.workflow.data('set', 'custom2', '55');
  //this is not indexed
  await Durable.workflow.data('set', 'jimbo', 'jones');
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);
  //any data is available to the workflow
  await Durable.workflow.data('get', 'jimbo');
  await Durable.workflow.data('incr', 'counter', '10');
  const val1 = await Durable.workflow.data('incr', 'counter', '1');
  const val2 = await Durable.workflow.data('get', 'counter');
  const val3 = await Durable.workflow.data('mult', 'multer', '12');
  //val4 is 120.00000000009 (rounding error due to logarithmic math)
  const val4 = await Durable.workflow.data('mult', 'multer', '10');
  return `${hello} - ${goodbye}`;
}
