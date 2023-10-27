# HotMesh
![alpha release](https://img.shields.io/badge/release-alpha-yellow)

Elevate Redis from an in-memory data store to a game-changing [service mesh](./docs/faq.md#what-is-hotmesh). Turn your unpredictable functions into unbreakable workflows.

## Install
[![npm version](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh.svg)](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh)

```sh
npm install @hotmeshio/hotmesh
```

## Design
The HotMesh SDK is designed to keep your code front-and-center. Write functions as you normally would, then use HotMesh to make them durable.

1. Start by defining **activities**. Activities are those functions that will be invoked by your workflow. They are commonly used to read and write to databases and invoke external services. They can be written in any style, using any framework, and can even be legacy functions you've already written. The only requirement is that they return a Promise. *Note how the `saludar` example throws an error 50% of the time. It doesn't matter how unpredictable your functions are, HotMesh will retry as necessary until they succeed.*
    ```javascript
    //activities.ts

    export async function greet(name: string): Promise<string> {
      return `Hello, ${name}!`;
    }

    export async function saludar(nombre: string): Promise<string> {
      Math.random() > 0.5 && throw new Error('Random error');
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

3. Although you could call your workflow directly (it's just a vanilla function), it's only durable when invoked and orchestrated via HotMesh. By using a HotMesh **client** to trigger the function, it's guaranteed to return a result.
    ```javascript
    //client.ts

    import { Durable } from '@hotmeshio/hotmesh';
    import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`
    import { nanoid } from 'nanoid';

    async function run(): Promise<string> {
      const client = new Durable.Client({
        connection: {
          class: Redis,
          options: { host: 'localhost', port: 6379 }
        }
      });

      const handle = await client.workflow.start({
        args: ['HotMesh', 'es'],
        taskQueue: 'hello-world',
        workflowName: 'example',
        workflowId: nanoid()
      });

      return await handle.result();
      //returns '¡Hola, HotMesh!'
    }
    ```

4. The last step is to create a **worker** and link it to your workflow function. Workers listen for tasks on their assigned channel, executing the targeted workflow function until it succeeds.
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
        taskQueue: 'hello-world',
        workflow: workflows.example,
      });
      await worker.run();
    }
    ```

>HotMesh delivers durable workflows without the cost and complexity of a centralized service mesh. Refer to the [samples-typescript](https://github.com/hotmeshio/samples-typescript) Git Repo for a range of examples, including compositional workflows (where one workflow calls another) and remote execution (where calls are brokered across microservices).

## Advanced Design
HotMesh's TypeScript SDK is the easiest way to make your functions durable. But if you need full control over your function lifecycles (including high-volume, high-speed use cases), you can use HotMesh's underlying YAML models to optimize your durable workflows. The following model depicts a sequence of activities orchestrated by HotMesh. Any function you associate with a `topic` in your YAML definition is guaranteed to be durable.

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

## Architectural First Principles
For a deep dive into HotMesh's distributed orchestration philosophy, refer to the [Architectural First Principles Overview](./docs/architecture.md).

## Distributed Orchestration
HotMesh is a distributed orchestration engine. Refer to the [Distributed Orchestration Guide](./docs/distributed_orchestration.md) for a detailed breakdown of the approach.

## System Lifecycle
Gain insight into HotMesh's monitoring, exception handling, and alarm configurations via the [System Lifecycle Guide](./docs/system_lifecycle.md).

## Alpha Release
So what exacty is an [alpha release](./docs/alpha.md)?!
