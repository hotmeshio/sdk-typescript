import { Connection, ConnectionConfig } from "../../types/durable";

/*

Here is an example of how the methods in this file are used:

./worker.ts

import { Durable: { NativeConnection, Worker } } from '@hotmeshio/hotmesh';
import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`

import * as workflows from './workflows';

async function run() {
  const connection = await NativeConnection.connect({
    class: Redis,
    options: {
      host: 'localhost',
      port: 6379,
    },
  });
  const worker = await Worker.create({
    connection,
    taskQueue: 'hello-world',
    workflow: workflows.example,
    activities,
  });
  await worker.run();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

*/

export class NativeConnectionService {
  static async connect(config: ConnectionConfig): Promise<Connection> {
    return {
      class: config.class,
      options: { ...config.options },
     } as Connection;
  }
}
