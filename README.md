# HotMesh
![alpha release](https://img.shields.io/badge/release-alpha-yellow)

Elevate Redis from an in-memory data store to a game-changing **service mesh**, delivering *durable* workflows without the overhead of a dedicated control plane. With HotMesh, you can keep your code at the forefront, utilizing [Redis infrastructure](./docs/faq.md#what-is-hotmesh) you already trust and own.

## Install
[![npm version](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh.svg)](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh)

```sh
npm install @hotmeshio/hotmesh
```

## Design
HotMesh's TypeScript SDK is modeled after Temporal IO's developer-friendly approach. Design and deploy durable workflows using your preferred coding style. Write your functions as you normally would, then use the HotMesh to make them durable. Temporal's [hello-world tutorial](https://github.com/temporalio/samples-typescript/tree/main/hello-world/src), for example, requires few changes beyond importing the HotMesh SDK.

>Start by defining activities. These are the functions that will be invoked by your workflow. They can be written in any style, using any framework, and can even be legacy functions you've already written. The only requirement is that they return a Promise.

**./activities.ts**
```javascript
export async function greet(name: string): Promise<string> {
  return `Hello, ${name}!`;
}
```

>Next, define your workflow. This function will invoke your activities. Include conditional logic, loops, etc. There's nothing special here--it's still just vanilla code written in your own coding style.

**./workflows.ts**
```javascript
import { Durable } from '@hotmeshio/hotmesh';
import type * as activities from './activities';

const { greet } = Durable.workflow.proxyActivities<typeof activities>();

export async function example(name: string): Promise<string> {
  return await greet(name);
}
```

>Finally, create a worker and client. The *client* triggers workflows, while the *worker* runs them, retrying as necessary until the workflow succeeds--all without the need for complicated retry logic.

**./worker.ts**
```javascript
import { Durable } from '@hotmeshio/hotmesh';
import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`
import * as activities from './activities';

async function run() {
  const connection = await Durable.NativeConnection.connect({
    class: Redis,
    options: { host: 'localhost', port: 6379 },
  });
  const worker = await Durable.Worker.create({
    connection,
    namespace: 'default',
    taskQueue: 'hello-world',
    workflowsPath: require.resolve('./workflows'),
    activities,
  });
  await worker.run();
}
```

**./client.ts**
```javascript
import { Durable } from '@hotmeshio/hotmesh';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

async function run() {
  const connection = await Durable.Connection.connect({
    class: Redis,
    options: { host: 'localhost', port: 6379 },
  });

  const client = new Durable.Client({
    connection,
  });

  const handle = await client.workflow.start({
    args: ['HotMesh'],
    taskQueue: 'hello-world',
    workflowName: 'example',
    workflowId: 'workflow-' + uuidv4(),
  });

  console.log(await handle.result());
}
```

>HotMesh delivers durable function execution using a [distributed service mesh](./docs/distributed_orchestration.md). The design delivers durable workflows without the cost and complexity of a centralized service mesh/control plane. Refer to the [hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript) Git Repo for a range of examples, including nested workflows.

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
Call `sub` to subscribe to all workflow results for a given topic.

```javascript
await hotMesh.sub('sandbox.work.done', (topic, jobOutput) => {
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
Gain insight into the HotMesh's monitoring, exception handling, and alarm configurations via the [System Lifecycle Guide](./docs/system_lifecycle.md).

## Alpha Release
So what exacty is an [alpha release](./docs/alpha.md)?!
