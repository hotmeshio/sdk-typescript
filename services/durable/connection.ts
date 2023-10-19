import { Connection, ConnectionConfig } from "../../types/durable";

/*
Here is an example of how the methods in this file are used:

./client.ts

import { Durable } from '@hotmeshio/hotmesh';
import Redis from 'ioredis';
import { nanoid } from 'nanoid';

async function run() {
  const connection = await Durable.Connection.connect({
    class: Redis,
    options: {
      host: 'localhost',
      port: 6379,
    },
  });

  const client = new Durable.Client({
    connection,
  });

  const handle = await client.workflow.start(example, {
    taskQueue: 'hello-world',
    args: ['HotMesh'],
    workflowName: 'example',
    workflowId: nanoid(),
  });

  console.log(`Started workflow ${handle.workflowId}`);
  console.log(await handle.result());
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

*/

export class ConnectionService {
  static async connect(config: ConnectionConfig): Promise<Connection> {
    return {
      class: config.class,
      options: { ...config.options },
     } as Connection;
  }
}
