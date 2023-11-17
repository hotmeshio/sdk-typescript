import { Durable } from '../../../../services/durable';
import * as activities from './activities';

const { greet, bye } = Durable.workflow
  .proxyActivities<typeof activities>({ activities });

export async function example(name: string): Promise<string> {
  //set values in the durable workflow data store (these are indexed)
  const search = await Durable.workflow.search();
  await search.set('custom1', 'durable');
  await search.set('custom2', '55');
  //this is not indexed (but is still available to the workflow)
  await search.set('jimbo', 'jones');
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);
  //any data is available to the workflow
  await search.get('jimbo');
  await search.incr('counter', 10);
  const val1 = await search.incr('counter', 1);
  const val2 = await search.get('counter');
  const val3 = await search.mult('multer', 12);
  //val4 is 120.00000000009 (rounding error due to logarithmic math)
  const val4 = await search.mult('multer', 10);
  //console.log(val1, val2, val3, val4);
  return `${hello} - ${goodbye}`;
}
