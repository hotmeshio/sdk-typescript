# HotMesh
![alpha release](https://img.shields.io/badge/release-alpha-yellow)

HotMesh transforms Redis into a distributed orchestration engine.

*Write functions in your own preferred style, and let Redis govern their execution, reliably and durably.*

## Install
[![npm version](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh.svg)](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh)

```sh
npm install @hotmeshio/hotmesh
```

## Understanding HotMesh
HotMesh inverts the relationship to Redis: those functions that once used Redis as a cache, are instead *cached and governed* by Redis. Consider the following. It's a typical microservices network, with a tangled mess of services and functions. There's important business logic in there (functions *A*, *B* and *C* are critical!), but they're hard to find and access.

<img src="./docs/img/operational_data_layer.png" alt="A Tangled Microservices Network with 3 valuable functions buried within" style="max-width:100%;width:600px;">

HotMesh creates an *ad hoc*, Redis-backed network of functions and organizes them into a unified service mesh. *Any service with access to Redis can join in the network, bypassing the legacy clutter.*

<img src="./docs/img/hotmesh_emergent_properties.png" alt="HotMesh elevates Redis to a Service Mesh, Message Router, Integration Bus, and Business Process Engine." style="max-width:100%;width:600px;">


## Design
HotMesh uses your existing Redis installation. If you already have an instance available, you have all the infrastructure you'll ever need.

### Design | Pluck
The simplest way to get started is to use the [HotMesh Pluck](https://github.com/hotmeshio/pluck-typescript) package. It's designed for usability, and is backed by the HotMesh system if you need advanced features. It includes a detailed [SDK](https://hotmeshio.github.io/pluck-typescript/) and a wide variety of examples.

### Design | Durable
HotMesh's *Durable* module is a TypeScript Library modeled after Temporal.io. If you're familiar with their SDK, the principles are the same.

1. Start by defining **activities**. Activities can be written in any style, using any framework, and can even be legacy functions you've already written. The only requirement is that they return a Promise. *Note how the `saludar` example throws an error 50% of the time. It doesn't matter how unpredictable your functions are, HotMesh will retry as necessary until they succeed.*
    ```javascript
    //activities.ts
    export async function greet(name: string): Promise<string> {
      return `Hello, ${name}!`;
    }

    export async function saludar(nombre: string): Promise<string> {
      if (Math.random() > 0.5) throw new Error('Random error');
      return `¡Hola, ${nombre}!`;
    }
    ```
2. Define your **workflow** logic. Include conditional branching, loops, etc to control activity execution. It's vanilla code written in your own coding style. The only requirement is to use `proxyActivities`, ensuring your activities are executed with HotMesh's durability guarantee.
    ```javascript
    //workflows.ts
    import { Durable } from '@hotmeshio/hotmesh';
    import * as activities from './activities';

    const { greet, saludar } = Durable.workflow
      .proxyActivities<typeof activities>({
        activities
      });

    export async function example(name: string, lang: string): Promise<string> {
      if (lang === 'es') {
        return await saludar(name);
      } else {
        return await greet(name);
      }
    }
    ```
3. Instance a HotMesh **client** to invoke the workflow.
    ```javascript
    //client.ts
    import { Durable, HotMesh } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`

    async function run(): Promise<string> {
      const client = new Durable.Client({
        connection: {
          class: Redis,
          options: { host: 'localhost', port: 6379 }
        }
      });

      const handle = await client.workflow.start({
        args: ['HotMesh', 'es'],
        taskQueue: 'default',
        workflowName: 'example',
        workflowId: HotMesh.guid()
      });

      return await handle.result();
      //returns '¡Hola, HotMesh!'
    }
    ```
4. Finally, create a **worker** and link your workflow function. Workers listen for tasks on their assigned Redis stream and invoke your workflow function each time they receive an event.
    ```javascript
    //worker.ts
    import { Durable } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis';
    import * as workflows from './workflows';

    async function run() {
      const worker = await Durable.Worker.create({
        connection: {
          class: Redis,
          options: { host: 'localhost', port: 6379 },
        },
        taskQueue: 'default',
        workflow: workflows.example,
      });

      await worker.run();
    }
    ```

#### Workflow Extensions
Externalizing state fundamentally changes the execution profile for your functions, allowing you to design long-running, durable workflows. The `Durable` base class (shown in the examples above) provides additional methods for solving the most common state management challenges.

 - `waitFor` Pause your function using your chosen signal key, and only awaken when the signal is received from the outide. Use a standard `Promise` to collate and cache the signals and only awaken your function once all signals have arrived.
    ```javascript
    const { waitFor } = Durable.workflow;
    const [a, b] = await Promise.all([
      waitFor<{payload: string}>('sig1'),
      waitFor<number>('sig2')
    ]);
    ```
 - `signal` Send a signal (and payload) to a paused function awaiting the signal. Signals may also be sent from the outside to awaken a paused function.
    ```javascript
    await Durable.workflow.signal('sig1', {payload: 'hi!'});
    ```
 - `hook` Redis governance converts your functions into 're-entrant processes'. Optionally use the *hook* method to spawn parallel execution threads to augment a running workflow.
    ```javascript
    await Durable.workflow.hook({
      workflowName: 'newsletter',
      taskQueue: 'default',
      args: []
    });
    ```
 - `sleepFor` Pause function execution for a ridiculous amount of time (months, years, etc). There's no risk of information loss, as Redis governs function state. When your function awakens, function state is efficiently (and automatically) restored and your function will resume right where it left off.
    ```javascript
    await Durable.workflow.sleepFor('1 month');
    ```
 - `random` Generate a deterministic random number that can be used in a reentrant process workflow (replaces `Math.random()`).
    ```javascript
    const random = await Durable.workflow.random();
    ```
 - `execChild` Call another durable function and await the response. *Design sophisticated, multi-process solutions by leveraging this command.*
    ```javascript
    const jobResponse = await Durable.workflow.execChild({
      workflowName: 'newsletter',
      taskQueue: 'default',
      args: [{ id, user_id, etc }],
    });
    ```
 - `startChild` Call another durable function, but do not await the response.
    ```javascript
    const jobId = await Durable.workflow.startChild({
      workflowName: 'newsletter',
      taskQueue: 'default',
      args: [{ id, user_id, etc }],
    });
    ```
 - `getContext` Get the current workflow context (workflowId, replay history, replay index, etc).
    ```javascript
    const context = await Durable.workflow.getContext();
    ```
 - `search` Instance a search session
    ```javascript
    const search = await Durable.workflow.search();
    ```
    - `set` Set one or more name/value pairs
      ```javascript
      await search.set('name1', 'value1', 'name2', 'value2');
      ```
    - `get` Get a single value by name
      ```javascript
      const value = await search.get('name');
      ```
    - `mget` Get multiple values by name
      ```javascript
      const [val1, val2] = await search.mget('name1', 'name2');
      ```
    - `del` Delete one or more entries by name and return the number deleted
      ```javascript
      const count = await search.del('name1', 'name2');
      ```
    - `incr` Increment (or decrement) a number
      ```javascript
      const value = await search.incr('name', 12);
      ```
    - `mult` Multiply a number
      ```javascript
      const value = await search.mult('name', 12);
      ```

Refer to the [hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript) repo for usage examples. 

### Design | Advanced
The *Pluck* and *Durable* modules are the easiest way to use HotMesh. But if you need full control over your function lifecycles (including high-volume, high-speed use cases), you can use HotMesh's underlying YAML models to optimize your durable workflows. The following model depicts a sequence of activities orchestrated by HotMesh. Any function you associate with a `topic` in your YAML definition is guaranteed to be durable.

```yaml
app:
  id: sandbox
  version: '1'
  graphs:
    - subscribes: sandbox.work.do
      publishes: sandbox.work.done

      activities:
        gateway:
          type: trigger
        servicec:
          type: worker
          topic: sandbox.work.do.servicec
        serviced:
          type: worker
          topic: sandbox.work.do.serviced
        sforcecloud:
          type: worker
          topic: sandbox.work.do.sforcecloud

      transitions:
        gateway:
          - to: servicec
        servicec:
          - to: serviced
        serviced:
          - to: sforcecloud
```

### Initialize
Provide your chosen Redis instance and configuration options to start a HotMesh Client. *HotMesh supports both `ioredis` and `redis` clients interchangeably.*

```javascript
import { HotMesh } from '@hotmeshio/hotmesh';
import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`

const hotMesh = await HotMesh.init({
  appId: 'sandbox',
  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db } //per your chosen Redis client
    }
  }
});
```

A HotMesh Client can be used to trigger worfkows and subscribe to results.

### Trigger a Workflow
Call `pub` to initiate a workflow. This function returns a job ID that allows you to monitor the progress of the workflow.

```javascript
const topic = 'sandbox.work.do';
const payload = { };
const jobId = await hotMesh.pub(topic, payload);
```

### Subscribe to Events
Call `psub` (patterned subscription) to subscribe to all workflow results for a given topic.

```javascript
await hotMesh.psub('sandbox.work.done.*', (topic, jobOutput) => {
  // use jobOutput.data
});
```

### Trigger and Wait
Call `pubsub` to start a workflow and *wait for the response*. HotMesh establishes a one-time subscription and delivers the job result once the workflow concludes.

```javascript
const jobOutput = await hotMesh.pubsub(topic, payload);
```

>The `pubsub` method is a convenience function that merges pub and sub into a single call. Opt for HotMesh's queue-driven engine over fragile HTTP requests to develop resilient solutions.

### Link Worker Functions
Link worker functions to a topic of your choice. When a workflow activity in the YAML definition with a corresponding topic runs, HotMesh will invoke your function, retrying as configured until it succeeds.

```javascript
import { HotMesh } from '@hotmeshio/hotmesh';
import Redis from 'ioredis';

const hotMesh = await HotMesh.init({
  appId: 'sandbox',
  workers: [
    { 
      topic: 'sandbox.work.do.servicec',
      redis: {
        class: Redis,
        options: { host, port, password, db }
      }
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: { }
        };
      }
    }
  ]
};
```

### Observability
Workflows and activities are run according to the rules you define, offering [Graph-Oriented](./docs/system_lifecycle.md#telemetry) telemetry insights into your legacy function executions.

## FAQ
Refer to the [FAQ](./docs/faq.md) for terminology, definitions, and an exploration of how HotMesh facilitates orchestration use cases.

## Quick Start
Refer to the [Quick Start](./docs/quickstart.md) for sample flows you can easily copy, paste, and modify to get started.

## Developer Guide
For more details on the complete development process, including information about schemas, APIs, and deployment, consult the [Developer Guide](./docs/developer_guide.md).

## Model Driven Development
[Model Driven Development](./docs/model_driven_development.md) is an established strategy for managing process-oriented tasks. Check out this guide to understand its foundational principles.

## Data Mapping
Exchanging data between activities is central to HotMesh. For detailed information on supported functions and the functional mapping syntax (@pipes), see the [Data Mapping Overview](./docs/data_mapping.md).

## Composition
While the simplest graphs are linear, detailing a consistent sequence of non-cyclical activities, graphs can be layered to represent intricate business scenarios. Some can even be designed to accommodate long-lasting workflows that span months. For more details, check out the [Composable Workflow Guide](./docs/composable_workflow.md).

## Distributed Orchestration
HotMesh is a distributed orchestration engine. Refer to the [Distributed Orchestration Guide](./docs/distributed_orchestration.md) for a detailed breakdown of the approach.

## System Lifecycle
Gain insight into HotMesh's monitoring, exception handling, and alarm configurations via the [System Lifecycle Guide](./docs/system_lifecycle.md).
