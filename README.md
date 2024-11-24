# HotMesh
![beta release](https://img.shields.io/badge/release-beta-blue.svg)

**HotMesh** offers the power of Temporal.io, but without a central app server. Just point to your database (**Postgres** or **Redis**).


<br/>

## Features

- **Serverless Orchestration**: Orchestrate your microservices without the need for a central app server.
- **Bring Your Own Database**: Mix and match technologies through a standard interface.
- **Decentralized Execution**: Centralized persistence (memory) with decentralized execution (cpu).
- **Linear Scalability**: Scale your database to scale your application.
- **Analytics**: Gain process insights with real-time analytics.


<br/>

## Install

```sh
npm install @hotmeshio/hotmesh
```

## Learn
[📄 Docs](https://hotmeshio.github.io/sdk-typescript/) | [💼 Sample Projects](https://github.com/hotmeshio/samples-typescript) | [🎥 Intro (3m)](https://www.loom.com/share/211bd4b4038d42f0ba34374ef5b6f961?sid=7b889a56-f60f-4ccc-84e7-8c2697e548a9) | [🎥 Transactional Workflow (9m)](https://www.loom.com/share/54ffd5266baf4ac6b287578abfd1d821?sid=0db2cef8-ef0d-4e02-a0b7-a1ee14f476ce)


<br/>

## MeshCall | Fast, Simple, Inter-Service Calls
[MeshCall](https://hotmeshio.github.io/sdk-typescript/classes/services_meshcall.MeshCall.html) connects any function to the mesh.

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Run an idempotent cron job <small>[more]</small></summary>

  ### Run a Cron
  This example demonstrates an *idempotent* cron that runs daily at midnight. The `id` makes each cron job unique and ensures that only one instance runs, despite repeated invocations. *The `cron` method returns `false` if a workflow is already running with the same `id`.*
  
  Optionally set a `delay` and/or set `maxCycles` to limit the number of cycles. The `interval` can be any human-readable time format (e.g., `1 day`, `2 hours`, `30 minutes`, etc) or a standard cron expression.

1. Define the cron function.
    ```typescript
    //cron.ts
    import { MeshCall } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    export const runMyCron = async (id: string, interval = '0 0 * * *'): Promise<boolean> => {
      return await MeshCall.cron({
        topic: 'my.cron.function',
        connection: {
          class: Redis,
          options: { url: 'redis://:key_admin@redis:6379' }
        },
        callback: async () => {
          //your code here...
        },
        options: { id, interval, maxCycles: 24 }
      });
    };
    ```

2. Call `runMyCron` at server startup (or call as needed to run multiple crons).
    ```typescript
    //server.ts
    import { runMyCron } from './cron';

    runMyCron('myNightlyCron123');
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Interrupt a cron job <small>[more]</small></summary>

  ### Interrupt a Cron
  This example demonstrates how to cancel a running cron job.

1. Use the same `id` and `topic` that were used to create the cron to cancel it.
    ```typescript
    import { MeshCall } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    MeshCall.interrupt({
      topic: 'my.cron.function',
      connection: {
        class: Redis,
        options: { url: 'redis://:key_admin@redis:6379' }
      },
      options: { id: 'myNightlyCron123' }
    });
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Call any function in any service <small>[more]</small></summary>

  ### Call a Function
  Make blazing fast interservice calls that behave like HTTP but without the setup and performance overhead. This example demonstrates how to connect a function to the mesh and call it from anywhere on the network.

1. Call `MeshCall.connect` and provide a `topic` to uniquely identify the function.

    ```typescript
    //myFunctionWrapper.ts
    import { MeshCall, Types } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    export const connectMyFunction = async () => {
      return await MeshCall.connect({
        topic: 'my.demo.function',
        connection: {
          class: Redis,
          options: { url: 'redis://:key_admin@redis:6379' }
        },
        callback: async (input: string) => {
          //your code goes here; response must be JSON serializable
          return { hello: input }
        },
      });
    };
      ```

2. Call `connectMyFunction` at server startup to connect your function to the mesh.

    ```typescript
    //server.ts
    import { connectMyFunction } from './myFunctionWrapper';
    connectMyFunction();
    ```

3. Call your function from anywhere on the network (or even from the same service). Send any payload as long as it's JSON serializable.

    ```typescript
    import { MeshCall } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    const result = await MeshCall.exec({
      topic: 'my.demo.function',
      args: ['something'],
      connection: {
        class: Redis,
        options: { url: 'redis://:key_admin@redis:6379' }
      },
    }); //returns `{ hello: 'something'}`
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Call and <b>cache</b> a function <small>[more]</small></summary>

  ### Cache a Function
  Redis is great for unburdening stressed services. This solution builds upon the previous example, caching the response. The linked function will only be re/called when the cached result expires. Everything remains the same, except the caller which specifies an `id` and `ttl`.

1. Make the call from another service (or even the same service). Include an `id` and `ttl` to cache the result for the specified duration.

    ```typescript
    import { MeshCall } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    const result = await MeshCall.exec({
      topic: 'my.demo.function',
      args: ['anything'],
      connection: {
        class: Redis,
        options: { url: 'redis://:key_admin@redis:6379' }
      },
      options: { id: 'myid123', ttl: '15 minutes' },
    }); //returns `{ hello: 'anything'}`
    ```

2. Flush the cache at any time, using the same `topic` and cache `id`.

    ```typescript
    import { MeshCall } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    await MeshCall.flush({
      topic: 'my.demo.function',
      connection: {
        class: Redis,
        options: { url: 'redis://:key_admin@redis:6379' }
      },
      options: { id: 'myid123' },
    });
    ```
</details>


<br/>

## MeshFlow | Transactional Workflow
[MeshFlow](https://hotmeshio.github.io/sdk-typescript/classes/services_meshflow.MeshFlow.html) is a drop-in replacement for [Temporal.io](https://temporal.io). MeshFlow emulates the Temporal SDK using a serverless mesh.

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Orchestrate unpredictable activities <small>[more]</small></summary>

### Proxy Activities
When an endpoint is unpredictable, use `proxyActivities`. HotMesh will retry as necessary until the call succeeds. This example demonstrates a workflow that greets a user in both English and Spanish. Even though both activities throw random errors, the workflow always returns a successful result.

1. Start by defining **activities**. Note how each throws an error 50% of the time.

    ```typescript
    //activities.ts
    export async function greet(name: string): Promise<string> {
      if (Math.random() > 0.5) throw new Error('Random error');
      return `Hello, ${name}!`;
    }

    export async function saludar(nombre: string): Promise<string> {
      if (Math.random() > 0.5) throw new Error('Random error');
      return `¡Hola, ${nombre}!`;
    }
    ```

2. Define the **workflow** logic. Include conditional branching, loops, etc to control activity execution. It's vanilla JavaScript written in your own coding style. The only requirement is to use `proxyActivities`, ensuring your activities are executed with HotMesh's durability wrapper.

    ```typescript
    //workflows.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import * as activities from './activities';

    const { greet, saludar } = MeshFlow.workflow
      .proxyActivities<typeof activities>({
        activities
      });

    export async function example(name: string): Promise<[string, string]> {
      return Promise.all([
        greet(name),
        saludar(name)
      ]);
    }
    ```

3. Instance a HotMesh **client** to invoke the workflow.

    ```typescript
    //client.ts
    import { MeshFlow, HotMesh } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';

    async function run(): Promise<string> {
      const client = new MeshFlow.Client({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 }
        }
      });

      const handle = await client.workflow.start<[string,string]>({
        args: ['HotMesh'],
        taskQueue: 'default',
        workflowName: 'example',
        workflowId: HotMesh.guid()
      });

      return await handle.result();
      //returns ['Hello HotMesh', '¡Hola, HotMesh!']
    }
    ```

4. Finally, create a **worker** and link the workflow function. Workers listen for tasks on their assigned Redis stream and invoke the workflow function each time they receive an event.

    ```typescript
    //worker.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';
    import * as workflows from './workflows';

    async function run() {
      const worker = await MeshFlow.Worker.create({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 },
        },
        taskQueue: 'default',
        workflow: workflows.example,
      });

      await worker.run();
    }
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Pause and wait for a signal <small>[more]</small></summary>

### Wait for Signal
Pause a function and only awaken when a matching signal is received from the outide.

1. Define the **workflow** logic. This one waits for the `my-sig-nal` signal, returning the signal payload (`{ hello: 'world' }`) when it eventually arrives. Interleave additional logic to meet your use case.

    ```typescript
    //waitForWorkflow.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';

    export async function waitForExample(): Promise<{hello: string}> {
      return await MeshFlow.workflow.waitFor<{hello: string}>('my-sig-nal');
      //continue processing, use the payload, etc...
    }
    ```

2. Instance a HotMesh **client** and start a workflow. Use a custom workflow ID (`myWorkflow123`).

    ```typescript
    //client.ts
    import { MeshFlow, HotMesh } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';

    async function run(): Promise<string> {
      const client = new MeshFlow.Client({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 }
        }
      });

      //start a workflow; it will immediately pause
      await client.workflow.start({
        args: ['HotMesh'],
        taskQueue: 'default',
        workflowName: 'waitForExample',
        workflowId: 'myWorkflow123',
        await: false,
      });
    }
    ```

3. Create a **worker** and link the `waitForExample` workflow function.

    ```typescript
    //worker.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';
    import * as workflows from './waitForWorkflow';

    async function run() {
      const worker = await MeshFlow.Worker.create({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 },
        },
        taskQueue: 'default',
        workflow: workflows.waitForExample,
      });

      await worker.run();
    }
    ```

4. Send a signal to awaken the paused function; await the function result.

    ```typescript
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import * as Redis from Redis;

    const client = new MeshFlow.Client({
      connection: {
        class: Redis,
        options: { host: 'redis', port: 6379 }
      }
    });

    //awaken the function by sending a signal
    await client.signal('my-sig-nal', { hello: 'world' });

    //get the workflow handle and await the result
    const handle = await client.getHandle({
      taskQueue: 'default',
      workflowId: 'myWorkflow123'
    });
    
    const result = await handle.result();
    //returns { hello: 'world' }
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Wait for multiple signals (collation) <small>[more]</small></summary>

### Collate Multiple Signals
Use a standard `Promise` to collate and cache multiple signals. HotMesh will only awaken once **all** signals have arrived. HotMesh will track up to 25 concurrent signals.

1. Update the **workflow** logic to await two signals using a promise: `my-sig-nal-1` and `my-sig-nal-2`. Add additional logic to meet your use case.

    ```typescript
    //waitForWorkflows.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';

    export async function waitForExample(): Promise<[boolean, number]> {
      const [s1, s2] = await Promise.all([
        Meshflow.workflow.waitFor<boolean>('my-sig-nal-1'),
        Meshflow.workflow.waitFor<number>('my-sig-nal-2')
      ]);
      //do something with the signal payloads (s1, s2)
      return [s1, s2];
    }
    ```

2. Send **two** signals to awaken the paused function.

    ```typescript
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import * as Redis from Redis;

    const client = new MeshFlow.Client({
      connection: {
        class: Redis,
        options: { host: 'redis', port: 6379 }
      }
    });

    //send 2 signals to awaken the function; order is unimportant
    await client.signal('my-sig-nal-2', 12345);
    await client.signal('my-sig-nal-1', true);

    //get the workflow handle and await the collated result
    const handle = await client.getHandle({
      taskQueue: 'default',
      workflowId: 'myWorkflow123'
    });
    
    const result = await handle.result();
    //returns [true, 12345]
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Create a recurring, cyclical workflow <small>[more]</small></summary>

### Cyclical Workflow
This example calls an activity and then sleeps for a week. It runs indefinitely until it's manually stopped. It takes advantage of durable execution and can safely sleep for months or years.

>Container restarts have no impact on actively executing workflows as all state is retained in Redis.

1. Define the **workflow** logic. This one calls a legacy `statusDiagnostic` function once a week.

    ```typescript
    //recurringWorkflow.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import * as activities from './activities';

    const { statusDiagnostic } = MeshFlow.workflow
      .proxyActivities<typeof activities>({
        activities
      });

    export async function recurringExample(someValue: number): Promise<void> {
      do {
        await statusDiagnostic(someValue);
      } while (await MeshFlow.workflow.sleepFor('1 week'));
    }
    ```

2. Instance a HotMesh **client** and start a workflow. Assign a custom workflow ID (e.g., `myRecurring123`) if the workflow should be idempotent.

    ```typescript
    //client.ts
    import { MeshFlow, HotMesh } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';

    async function run(): Promise<string> {
      const client = new MeshFlow.Client({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 }
        }
      });

      //start a workflow; it will immediately pause
      await client.workflow.start({
        args: [55],
        taskQueue: 'default',
        workflowName: 'recurringExample',
        workflowId: 'myRecurring123',
        await: false,
      });
    }
    ```

3. Create a **worker** and link the `recurringExample` workflow function.

    ```typescript
    //worker.ts
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';
    import * as workflows from './recurringWorkflow';

    async function run() {
      const worker = await MeshFlow.Worker.create({
        connection: {
          class: Redis,
          options: { host: 'redis', port: 6379 },
        },
        taskQueue: 'default',
        workflow: workflows.recurringExample,
      });

      await worker.run();
    }
    ```

4. Cancel the recurring workflow (`myRecurring123`) by calling `interrupt`.

    ```typescript
    import { MeshFlow } from '@hotmeshio/hotmesh';
    import * as Redis from Redis;

    const client = new MeshFlow.Client({
      connection: {
        class: Redis,
        options: { host: 'redis', port: 6379 }
      }
    });

    //get the workflow handle and interrupt it
    const handle = await client.getHandle({
      taskQueue: 'default',
      workflowId: 'myRecurring123'
    });
    
    const result = await handle.interrupt();
    ```
</details>

<br/>

## MeshData | Transactional Analytics
[MeshData](https://hotmeshio.github.io/sdk-typescript/classes/services_meshdata.MeshData.html) extends the **MeshFlow** service, surfacing transactional workflows as searchable data records.

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Create a search index <small>[more]</small></summary>

### Workflow Data Indexes

This example demonstrates how to define a schema and deploy an index for a 'user' entity type.

1. Define the **schema** for the `user` entity. This one includes the 3 formats supported by the FT.SEARCH module: `TEXT`, `TAG` and `NUMERIC`.

    ```typescript
    //schema.ts
    export const schema: Types.WorkflowSearchOptions = {
      schema: {
        id: { type: 'TAG', sortable: false },
        first: { type: 'TEXT', sortable: false, nostem: true },
        active: { type: 'TAG', sortable: false },
        created: { type: 'NUMERIC', sortable: true },
      },
      index: 'user',
      prefix: ['user'],
    };
    ```

2. Create the Redis index upon server startup. This one initializes the 'user' index in Redis, using the schema defined in the previous step. It's OK to call `createSearchIndex` multiple times; it will only create the index if it doesn't already exist.

    ```typescript
    //server.ts
    import { MeshData } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';
    import { schema } from './schema';

    const meshData = new MeshData(
      Redis,
      { url: 'redis://:key_admin@redis:6379' },
      schema,
    );
    await meshData.createSearchIndex('user', { namespace: 'meshdata' });
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Create an indexed, searchable record <small>[more]</small></summary>

### Workflow Record Data
This example demonstrates how to create a 'user' workflow backed by the searchable schema from the prior example.

1. Call MeshData `connect` to initialize a 'user' entity *worker*. It references a target worker function which will run the workflow. Data fields that are documented in the schema (like `active`) will be automatically indexed when set on the workflow record.

    ```typescript
    //connect.ts
    import { MeshData } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';
    import { schema } from './schema';

    export const connectUserWorker = async (): Promise<void> => {
      const meshData = new MeshData(
        Redis,
        { url: 'redis://:key_admin@redis:6379' },
        schema,
      );
    
      await meshData.connect({
        entity: 'user',
        target: async function(name: string): Promise<string> {
          //add custom, searchable data (`active`) and return
          const search = await MeshData.workflow.search();
          await search.set('active', 'yes');
          return `Welcome, ${name}.`;
        },
        options: { namespace: 'meshdata' },
      });
    }
    ```

2. Wire up the worker at server startup, so it's ready to process incoming requests.

    ```typescript
    //server.ts
    import { connectUserWorker } from './connect';
    await connectUserWorker();
    ```

3. Call MeshData `exec` to create a 'user' workflow. Searchable data can be set throughout the workflow's lifecycle. This one initializes the workflow with 3 data fields: `id`, `name` and `timestamp`. *An additional data field (`active`) is set within the workflow function in order to demonstrate both mechanisms for reading/writing data to a workflow.*
  
    ```typescript
    //exec.ts
    import { MeshData } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';

    const meshData = new MeshData(
      Redis,
      { url: 'redis://:key_admin@redis:6379' },
      schema,
    );

    export const newUser = async (id: string, name: string): Promise<string> => {
      const response = await meshData.exec({
        entity: 'user',
        args: [name],
        options: {
          ttl: 'infinity',
          id,
          search: {
            data: { id, name, timestamp: Date.now() }
          },
          namespace: 'meshdata',
        },
      });
      return response;
    };
    ```

4. Call the `newUser` function to create a searchable 'user' record.

    ```typescript
    import { newUser } from './exec';
    const response = await newUser('jim123', 'James');
    ```
</details>

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Fetch record data <small>[more]</small></summary>

### Read Record Data
This example demonstrates how to read data fields directly from a workflow.

1. Read data fields directly from the *jimbo123* 'user' record.

    ```typescript
    //read.ts
    import { MeshData } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';
    import { schema } from './schema';

    const meshData = new MeshData(
      Redis,
      { url: 'redis://:key_admin@redis:6379' },
      schema,
    );

    const data = await meshData.get(
      'user',
      'jimbo123',
      { 
        fields: ['id', 'name', 'timestamp', 'active'],
        namespace: 'meshdata'
      },
    );
    ```
</details> 

<details style="padding: .5em">
  <summary style="font-size:1.25em;">Search record data <small>[more]</small></summary>

### Query Record Data
This example demonstrates how to search for those workflows where a given condition exists in the data. This one searches for active users. *NOTE: The native Redis FT.SEARCH syntax is supported. The JSON abstraction shown here is a convenience method for straight-forward, one-dimensional queries.*

1. Search for active users (where the value of the `active` field is `yes`).

    ```typescript
    //read.ts
    import { MeshData } from '@hotmeshio/hotmesh';
    import * as Redis from 'redis';
    import { schema } from './schema';

    const meshData = new MeshData(
      Redis,
      { url: 'redis://:key_admin@redis:6379' },
      schema,
    );

    const results = await meshData.findWhere('user', {
      query: [{ field: 'active', is: '=', value: 'yes' }],
      limit: { start: 0, size: 100 },
      return: ['id', 'name', 'timestamp', 'active']
    });
    ```
</details> 

<br/>

## Visualize | OpenTelemetry
HotMesh's telemetry output provides unmatched insight into long-running, x-service transactions. Add your Honeycomb credentials to any project using HotMesh and HotMesh will emit the full *OpenTelemetry* execution tree organized as a DAG.

<br/>

## Visualize | HotMesh Dashboard
The HotMesh dashboard provides a detailed overview of all running workflows. An LLM is included to simplify querying and analyzing workflow data for those deployments that include the Redis `FT.SEARCH` module.

<br/>

## Visualize | RedisInsight
View commands, streams, data, CPU, load, etc using the RedisInsight data browser.

<br/>

## Samples
Refer to the [hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript) Git repo for *tutorials* and instructions on deploying the *HotMesh Dashboard* for visualizing workflows and managing network health.

Refer to the [hotmeshio/temporal-patterns-typescript](https://github.com/hotmeshio/temporal-patterns-typescript) Git repo for examples of common Temporal.io patterns implemented using HotMesh.

<br/>

## Advanced
The theory that underlies the architecture is applicable to any number of data storage and streaming backends: [A Message-Oriented Approach to Decentralized Process Orchestration](https://zenodo.org/records/12168558).
