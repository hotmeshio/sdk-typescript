/**
 * A standalone process that holds one idle Postgres connection, then
 * answers `query` on stdin. Run with `plain` (a bare pg.Client, the way
 * connections were held before the resilient client) or `hotmesh` (a
 * connection created by PostgresConnection). The parent test terminates
 * the backend and observes whether this process survives.
 */
import { Client } from 'pg';

import { postgres_options } from '../../../$setup/postgres';
import { PostgresConnection } from '../../../../services/connector/providers/postgres';
import { guid } from '../../../../modules/utils';

const [mode, applicationName] = process.argv.slice(2);

async function main(): Promise<void> {
  const options = { ...postgres_options, application_name: applicationName };
  let client: any;
  if (mode === 'plain') {
    client = new Client(options);
    await client.connect();
  } else {
    client = (
      await PostgresConnection.connect(guid(), Client as any, options as any)
    ).getClient();
  }
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', async (line: string) => {
    if (line.trim() !== 'query') return;
    try {
      const result = await client.query('SELECT 1 AS ok');
      process.stdout.write(`result:${result.rows[0].ok}\n`);
    } catch (error) {
      process.stdout.write(`query-error:${(error as Error).message}\n`);
    }
  });
  process.stdout.write('ready\n');
}

main().catch((error) => {
  process.stdout.write(`boot-error:${error?.message}\n`);
  process.exit(2);
});
