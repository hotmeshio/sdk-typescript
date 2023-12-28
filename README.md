# HotMesh
![alpha release](https://img.shields.io/badge/release-alpha-yellow)

Elevate Redis from an in-memory data cache, and turn your unpredictable functions into unbreakable workflows.

**HotMesh** is a wrapper for Redis that exposes concepts like ‘activities’, ‘workflows’, and 'jobs'. Behind the scenes, it uses *Redis Data* (Hash, ZSet, List); *Redis Streams* (XReadGroup, XAdd, XLen, etc); and *Redis Publish/Subscribe*.

It's still Redis in the background, but your functions are run as *reentrant processes* and are executed in a distributed environment, with all the benefits of a distributed system, including fault tolerance, scalability, and high availability.

Write functions in your own preferred style, and let Redis govern their execution with its unmatched performance.

## Install
[![npm version](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh.svg)](https://badge.fury.io/js/%40hotmeshio%2Fhotmesh)

```sh
npm install @hotmeshio/hotmesh
```

## Design
The HotMesh SDK is designed to keep your code front-and-center. Write code as you normally would, then use HotMesh to make it durable.

1. Start with any ordinary class. Pay attention to unpredictable functions: those that execute slowly, cause problems at scale, or simply fail to return. *Note how the `flaky` function throws an error 50% of the time. This is exactly the type of function that can be fixed using HotMesh.*
    ```javascript
    //myworkflow.ts

    export class MyWorkflow {

      async run(name: string): Promise<string> {
        const hi = await this.flaky(name);
        const hello = await this.greet(name);
        return `${hi} ${hello}`;
      }

      //this function is unpredictable and will fail 50% of the time
      async flaky(name: string): Promise<string> {
        if (Math.random() < 0.5) {
          throw new Error('Ooops!');
        }
        return `Hi, ${name}!`;
      }

      async greet(name: string): Promise<string> {
        return `Hello, ${name}!`;
      }
    }
    ```
2. Import `Redis` and `MeshOS` and configure host, port, etc. List those functions that Redis should govern as durable workflows (like `run` and `flaky`). And that's it! *Your functions don't actually change; rather, their governance does.*
    ```javascript
    //myworkflow.ts

    import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`
    import { MeshOS } from '@hotmeshio/hotmesh';

    export class MyWorkflow extends MeshOS {

      redisClass = Redis;
      redisOptions = { host: 'localhost', port: 6379 };

      //list functions to run as durable workflows
      workflowFunctions = ['run'];

      //list functions to retry and cache
      proxyFunctions = ['flaky'];

      async run(name: string): Promise<string> {
        const hi = await this.flaky(name);
        const hello = await this.greet(name);
        return `${hi} ${hello}`;
      }

      //this function is now durable and will be retried until it succeeds!
      async flaky(name: string): Promise<string> {
        if (Math.random() < 0.5) {
          throw new Error('Ooops!');
        }
        return `Hi, ${name}!`;
      }

      async greet(name: string): Promise<string> {
        return `Hello, ${name}!`;
      }
    }
    ```
3. Invoke your class, providing a unique id (it's now an idempotent workflow and needs a GUID). Nothing changes from the outside, *but Redis now governs the end-to-end execution.* It's guaranteed to succeed, even if it takes a while. 
    ```javascript
    //mycaller.ts

    const workflow = new MyWorkflow({ id: 'my123', await: true });
    const response = await workflow.run('World');
    //Hi, World! Hello, World!
    ```

Redis governance delivers more than just reliability. Externalizing state fundamentally changes the execution profile for your functions, allowing you to design long-running, durable workflows. Use the following methods to solve the most common state management challenges.

 - `waitForSignal` | Pause and wait for external event(s) before continuing. The *waitForSignal* method will collate and cache the signals and only awaken your function once they've all arrived.
 - `signal` | Send a signal (and optional payload) to any paused function.
 - `hook` | Redis governance supercharges your functions, transforming them into 're-entrant processes'. Optionally use the *hook* method to spawn parallel execution threads within any running function.
 - `sleep` | Pause function execution for a ridiculous amount of time (months, years, etc). There's no risk of information loss, as Redis governs function state. When your function awakens, function state is efficiently (and automatically) restored.
 - `random` | Generate a deterministic random number that can be used in a reentrant process workflow (replaces `Math.random()`).
 - `executeChild` | Call another durable function and await the response. *Design sophisticated, multi-process solutions by leveraging this command.*
 - `startChild` | Call another durable function, but do not await the response.
 - `set` | Set one or more name/value pairs (e.g, `set('name1', 'value1', 'name1', 'value2')`)
 - `get` | Get a single value by name(e.g, `get('name')`)
 - `mget` | Get multiple values by name (e.g, `get('name1', 'name2')`)
 - `del` | Delete one or more entries by name and return the number deleted (e.g, `del('name1', 'name1')`)
 - `incr` | Increment (or decrement) a number (e.g, `incr('name', -99)`)
 - `mult` | Multiply a number (e.g, `mult('name', 12)`)

Refer to the [hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript) repo for usage examples. 

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

## Distributed Orchestration
HotMesh is a distributed orchestration engine. Refer to the [Distributed Orchestration Guide](./docs/distributed_orchestration.md) for a detailed breakdown of the approach.

## System Lifecycle
Gain insight into HotMesh's monitoring, exception handling, and alarm configurations via the [System Lifecycle Guide](./docs/system_lifecycle.md).

## Alpha Release
So what exacty is an [alpha release](./docs/alpha.md)?
