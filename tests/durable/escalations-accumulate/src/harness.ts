import { Client as Postgres } from 'pg';

import { Durable } from '../../../../services/durable';
import { guid, sleepFor } from '../../../../modules/utils';
import { ProviderNativeClient } from '../../../../types/provider';
import { dropTables, postgres_options } from '../../../$setup/postgres';
import { PostgresConnection } from '../../../../services/connector/providers/postgres';

import * as workflows from './workflows';

const { Client, Worker } = Durable;

export type EscalationRow = Awaited<ReturnType<InstanceType<typeof Client>['escalations']['list']>>[number];

/** Shared boot for the accumulate suites: fresh tables, one worker per workflow. */
export async function bootHarness(taskQueue: string, names: Array<keyof typeof workflows>) {
  const postgresClient: ProviderNativeClient = (
    await PostgresConnection.connect(guid(), Postgres, postgres_options)
  ).getClient();
  await dropTables(postgresClient);
  const connection = { class: Postgres, options: postgres_options };
  const client = new Client({ connection });
  for (const name of names) {
    const worker = await Worker.create({ connection, taskQueue, workflow: workflows[name] });
    await worker.run();
  }
  return { client, connection, taskQueue };
}

/** Polls the role's pending list until the facet appears (Leg1 commit). */
export async function findPending(
  client: InstanceType<typeof Client>,
  role: string,
  facet: Record<string, unknown>,
): Promise<EscalationRow> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const rows = await client.escalations.list({ role, status: 'pending', metadata: facet });
    if (rows[0]) return rows[0];
    await sleepFor(25);
  }
  throw new Error(`no pending ${role} row for ${JSON.stringify(facet)}`);
}

export { workflows, guid, sleepFor };
