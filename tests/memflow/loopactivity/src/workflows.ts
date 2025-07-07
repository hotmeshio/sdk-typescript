import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';

const { greet } = MemFlow.workflow.proxyActivities<typeof activities>({
  activities,
});

export async function example(): Promise<[string, string, string, number]> {
  //send `trace` to the OpenTelemetry sink (happens just once)
  await MemFlow.workflow.trace({
    name: 'example',
    version: 1,
    complete: true,
  });

  //send `enrich` to the Database Backend (enriches record with name/value)
  await MemFlow.workflow.enrich({
    name: 'example',
    version: '1',
    complete: 'true',
  });

  //send 'emit' event to the Event Bus (emits events via the Pub/Sub Backend)
  await MemFlow.workflow.emit({
    'my.dog': { anything: 'goes' },
    'my.dog.cat': { anything: 'goes' },
  });

  return await Promise.all([
    greet('1'),
    greet('2'),
    greet('3'),
    MemFlow.workflow.sleepFor('5 seconds'),
  ]);
}
