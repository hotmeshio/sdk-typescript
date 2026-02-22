import { Durable } from '../../../../services/durable';

import * as activities from './activities';

const { greet } = Durable.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function example(): Promise<[string, string, string, number]> {
  //send `trace` to the OpenTelemetry sink (happens just once)
  await Durable.workflow.trace({
    name: 'example',
    version: 1,
    complete: true,
  });

  //send `enrich` to the Database Backend (enriches record with name/value)
  await Durable.workflow.enrich({
    name: 'example',
    version: '1',
    complete: 'true',
  });

  //send 'emit' event to the Event Bus (emits events via the Pub/Sub Backend)
  await Durable.workflow.emit({
    'my.dog': { anything: 'goes' },
    'my.dog.cat': { anything: 'goes' },
  });

  return await Promise.all([
    greet('1'),
    greet('2'),
    greet('3'),
    Durable.workflow.sleepFor('5 seconds'),
  ]);
}
