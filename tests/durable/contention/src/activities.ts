/**
 * Assembly-line activities — SDK-native, no HTTP dependencies.
 *
 * processStation: simulates station work (trivial — the point is
 * collation pressure, not activity duration).
 *
 * signalParent: signals the parent workflow via Durable Client,
 * reproducing the exact cross-workflow signal path that causes
 * the collation poison loop under parallel batch processing.
 */

import { Durable } from '../../../../services/durable';
import { Client as Postgres } from 'pg';
import { postgres_options } from '../../../$setup/postgres';

export async function processStation(input: {
  stationName: string;
  role: string;
}): Promise<{ stationName: string; completedAt: string }> {
  return {
    stationName: input.stationName,
    completedAt: new Date().toISOString(),
  };
}

export async function signalParent(input: {
  parentTaskQueue: string;
  parentWorkflowName: string;
  parentWorkflowId: string;
  signalId: string;
  data: any;
}): Promise<void> {
  const connection = { class: Postgres, options: postgres_options };
  const client = new Durable.Client({ connection });
  const handle = await client.workflow.getHandle(
    input.parentTaskQueue,
    input.parentWorkflowName,
    input.parentWorkflowId,
  );
  await handle.signal(input.signalId, input.data);
}
