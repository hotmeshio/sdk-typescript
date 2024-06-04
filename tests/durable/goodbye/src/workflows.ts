import { Durable } from '../../../../services/durable';

import * as activities from './activities';

const { greet, bye } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function example(name: string): Promise<string> {
  //set values (they're added to the workflow HASH AND are indexed)
  //(`custom1` and `custom2` were added to the 'bye-bye' index)
  const search = await Durable.workflow.search();
  await search.set('custom1', 'durable');
  await search.set('custom2', '55');

  //'jimbo' is not indexed (but it can be retrieved)
  await search.set('jimbo', 'jones');
  const [hello, goodbye] = await Promise.all([greet(name), bye(name)]);

  //all data is available to the workflow
  const j1 = await search.get('jimbo');
  await search.incr('counter', 10);
  const j2 = await search.get('jimbo');
  const j3 = await search.mget('jimbo');
  //console.log(j1, j2, j3[0]);
  const val1 = await search.incr('counter', 1);
  const val2 = await search.get('counter');
  const val3 = await search.mult('multer', 12);

  //val4 is 120.00000000009 (rounding error due to logarithmic math)
  const val4 = await search.mult('multer', 10);
  const signal1 = await Durable.workflow.waitFor<{ data: string }>('abcdefg');

  return `${hello} - ${goodbye}`;
}

export async function exampleHook(name: string): Promise<void> {
  const search = await Durable.workflow.search();
  await search.incr('counter', 100);
  await Durable.workflow.sleepFor('1 second');
  Durable.workflow.signal('abcdefg', { data: 'hello' });
}
