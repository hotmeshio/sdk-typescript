# Quick Start

The examples provided in this guide are the simplest possible flows that can be defined in HotMesh. They are intended to be used as a starting point as you modify portions to fit your use case. Examples are organized as a story, with each example building upon the prior one for better context. But any single example can be used as a starting point if you find it relevant.

**Table of Contents**
- [Setup](#setup)
  - [Install Packages](#install-packages)
  - [Configure and Initialize Redis](#configure-and-initialize-redis)
  - [Configure and Initialize HotMesh](#configure-and-initialize-hotmesh)
  - [Define the Application](#define-the-application)
  - [Deploy the Application](#deploy-the-application)
  - [Activate the Application](#activate-the-application)
  - [End-to-End Example](#end-to-end-example)
- [The Simplest Flow](#the-simplest-flow)
- [The Simplest Compositional Flow](#the-simplest-compositional-flow)
- [The Simplest Executable Flow](#the-simplest-executable-flow)
- [The Simplest Executable Data Flow](#the-simplest-executable-data-flow)
- [The Simplest Parallel Data Flow](#the-simplest-parallel-data-flow)
- [The Simplest Sequential Data Flow](#the-simplest-sequential-data-flow)
- [The Simplest Compositional Executable Flow](#the-simplest-compositional-executable-flow)
- [The Simplest Conditional Executable Flow](#the-simplest-conditional-executable-flow)

## Setup
### Install Packages
Install the HotMesh NPM package.

```bash
npm install @hotmeshio/hotmesh
```

Install the `Redis` or `IORedis` NPM package.

```bash
npm install redis
```

**OR**

```bash
npm install ioredis
```

### Configure and Initialize Redis
Configure and initialize 3 Redis clients.

>The examples in this guide will use the `ioredis` package, but you can use the `redis` package if you prefer. Configure and connect to Redis as is standard for your chosen package.

```javascript
import Redis from 'ioredis'; //OR `import * as Redis from 'redis';`
```

### Configure and Initialize HotMesh
Configure and initialize HotMesh.

```javascript
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',
  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db }
    }
  }
});
```

Before running workflows, the application must be *defined*, *deployed*, and *activated*. This is a *one-time activity* that must be called before calling `pub`, `pubsub` and similar application endpoints.

### Define the Application
This first example isn't technically "workflow", but it does represent the simplest app possible: a single flow with a single activity. It's valid YAML, but the app it defines will terminate as soon as it starts. 

Begin by saving the following YAML descriptor using a file name of your choosing (e.g., `abc.1.yaml`). This file will be referenced during the *deploy* step, so make sure you know where it's located.

```yaml
# abc.1.yaml
app:
  id: abc
  version: '1'
  graphs:
    - subscribes: abc.test
      activities:
        t1:
          type: trigger
```

### Deploy the Application
Now that you have a YAML descriptor, you can deploy it to HotMesh using the `deploy` method. This step compiles and saves the YAML descriptor to Redis where any connected engine or worker can access the rules it defines.

```javascript
await hotMesh.deploy('./abc.1.yaml');
```

### Activate the Application
Once the YAML descriptor is *deployed* to Redis, you can *activate* it using the `activate` method. This final step leverages the `sub` Redis channel in the background to coordinate quorum behavior across all running instances. This ensures that the entire fleet will simultaneously reference and execute the targeted YAML version.

```javascript
await hotMesh.activate('1');
```

>The flow is now active and available for invocation from any microservice with a connection to HotMesh.

Since there are no subsequent activities (this flow only has a single trigger), the flow will terminate immediately and the `response` will only contain `metadata` about the call (e.g., `jid` (job id), `js` (job status), `jc` (job created), etc).

>Notice how the `abc.test` topic is used to trigger the flow. This is the same topic that was defined in the YAML descriptor.

```javascript
const response = await hotMesh.pubsub('abc.test', {});
console.log(response.metadata);
```

### End-to-End Example
Here is the entire end-to-end example, including the one-time setup steps to *deploy* and *activate* version 1 of app `abc`.

```javascript
import Redis from 'ioredis';
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',
  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db }
    }
  }
});

await hotMesh.deploy(`
app:
  id: abc
  version: '1'
  graphs:
    - subscribes: abc.test
      activities:
        t1:
          type: trigger
`);
await hotMesh.activate('1');
await hotMesh.pubsub('abc.test');
```

## The Simplest Flow
Graphs need at least one *transition* to be classified as a "workflow". Notice how the graph now includes a `transitions` section to define the activity transition from `t1` to `a1`. Save this YAML descriptor as `abc.2.yaml`.

```yaml
# abc.2.yaml
app:
  id: abc
  version: '2'
  graphs:
    - subscribes: abc.test
      activities:
        t1:
          type: trigger
        a1:
          type: hook
      transitions:
        t1:
          - to: a1
```

Use HotMesh's **hot deployment** capability to upgrade the `abc` app from version `1` to version `2`.

```javascript
// continued from prior example

await hotMesh.deploy('./abc.1.yaml');
await hotMesh.activate('1');

const response1 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');

const response2 = await hotMesh.pubsub('abc.test', {});
```

From the outside (from the caller's perspective), the flow behavior doesn't appear much different. But behind the scenes, two activities will now run: the `trigger` (t1) and the `activity` (a1). The `trigger` will transition to the `activity`, and the `activity` will transition to the end of the flow.

## The Simplest Compositional Flow
This example shows the simplest *compositional flow* possible (where one flow triggers another). Composition allows for standardization and component re-use. Save this YAML descriptor as `abc.3.yaml`:

```yaml
# abc.3.yaml
app:
  id: abc
  version: '3'
  graphs:

    - subscribes: abc.test
      activities:
        t1:
          type: trigger
        a1:
          type: await
          topic: some.other.topic
      transitions:
        t1:
          - to: a1

    - subscribes: some.other.topic
      activities:
        t2:
          type: trigger
```

Upgrade the `abc` app from version `2` to version `3` and run another test.

```javascript
// continued from prior example

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');

const response2 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.3.yaml');
await hotMesh.activate('3');

const response3 = await hotMesh.pubsub('abc.test', {});
```

From the outside (from the caller's perspective), this flow doesn't appear much different from the prior 2 example flows as it doesn't define any input or output data. But behind the scenes, two flows will run: the first flow will transition from the `trigger` to the `await` activity which will then call the second flow, using the `some.other.topic` topic as the link. The second flow only defines a single `trigger`, so it will terminate immediately after it starts. After flow 2 terminates and returns, the `await` activity will conclude and flow 1 will terminate as well.

## The Simplest Executable Flow
This example shows the simplest *executable flow* possible (where actual work is performed by coordinating and executing functions on your network). Notice how activity, `a1` has been defined as a `worker` type. Save this YAML descriptor as `abc.4.yaml`.

>The `work.do` topic identifies the worker function to execute. This name is arbitrary and should match the semantics of your use case and the topic space you define for your organization.

```yaml
# abc.4.yaml
app:
  id: abc
  version: '4'
  graphs:
    - subscribes: abc.test
      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
      transitions:
        t1:
          - to: a1
```

When a `worker` activity is defined in the YAML, you must likewise register a `worker` function that will perform the work. Here is the updated end-to-end example with the entire evolution of the application from version `1` to version `4`. Notice how the `HotMesh.init` call now registers the worker function, using 3 additional redis connections. Points of presence like this (instances of HotMesh) can declare a HotMesh engine and/or one or more workers. The engine is used to coordinate the flow, while the workers are used to perform the work.

```javascript
import Redis from 'ioredis';
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',

  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db }
    }
  },

  workers: [
    { 
      topic: 'work.do',
      redis: {
        class: Redis,
        options: { host, port, password, db }
      }
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: {} // optional
        };
      }
    }
  ]
});

await hotMesh.deploy('./abc.1.yaml');
await hotMesh.activate('1');

const response1 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');

const response2 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.3.yaml');
await hotMesh.activate('3');

const response3 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.4.yaml');
await hotMesh.activate('4');

const response4 = await hotMesh.pubsub('abc.test', {});
```

## The Simplest Executable Data Flow
This example shows the simplest *executable data flow* possible (where data described using JSON Schema is exchanged between flows, activities, and worker functions). Notice how input and output schemas have been added to the flow along with mapping statements to bind the data. Save this YAML descriptor as `abc.5.yaml`.

When executed, this flow will expect a payload with a field named 'a' (the input) and will return a payload with a field named 'b' (the output). The input will be provided to the worker function which will modify it and return the output. This output will be surfaced and returned to the caller as the job data (the job response/output).

```yaml
# abc.5.yaml
app:
  id: abc
  version: '5'
  graphs:
    - subscribes: abc.test

      input:
        schema:
          type: object
          properties:
            a:
              type: string

      output:
        schema:
          type: object
          properties:
            b:
              type: string

      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
          input:
            schema:
              type: object
              properties:
                x:
                  type: string
            maps:
              x: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                y:
                  type: string
          job:
            maps:
              b: '{$self.output.data.y}'
      transitions:
        t1:
          - to: a1
```

Here is the updated end-to-end example with the entire evolution of the application from version `1` to version `5`. Note how the final response for the workflow now includes the output data from the worker function (`hello world`). All variable names are arbitrary (a, b, x, y). Choose names and structures that reflect your use case.

```javascript
import Redis from 'ioredis';
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',

  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db }
    }
  },

  workers: [
    { 
      topic: 'work.do',
      redis: {
        class: Redis,
        options: { host, port, password, db }
      }
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: { y: `${data?.data?.x} world` }
        };
      }
    }
  ]
});

await hotMesh.deploy('./abc.1.yaml');
await hotMesh.activate('1');
const response1 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');
const response2 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.3.yaml');
await hotMesh.activate('3');
const response3 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.4.yaml');
await hotMesh.activate('4');
const response4 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.5.yaml');
await hotMesh.activate('5');
const response5 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response5.data.b); // hello world
```

## The Simplest Parallel Data Flow
This example shows the simplest *parallel data flow* possible (where data is produced by two parallel worker function). This flow is relativey unchanged from the prior example with the exception of the addition of a second worker function (`a2`). Save this YAML descriptor as `abc.6.yaml`.

When executed, this flow will expect a payload with a field named 'a' (the input) and will return a payload with fields 'b' and 'c' (the output). Both workers will receive the same input data and operate in parallel, each producing a field in the output. The output will be surfaced and returned to the caller as the job data (the job response/output).

```yaml
# abc.6.yaml
app:
  id: abc
  version: '6'
  graphs:
    - subscribes: abc.test

      input:
        schema:
          type: object
          properties:
            a:
              type: string

      output:
        schema:
          type: object
          properties:
            b:
              type: string
            c:
              type: string

      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
          input:
            schema:
              type: object
              properties:
                x:
                  type: string
            maps:
              x: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                y:
                  type: string
          job:
            maps:
              b: '{$self.output.data.y}'
        a2:
          type: worker
          topic: work.do.more
          input:
            schema:
              type: object
              properties:
                i:
                  type: string
            maps:
              i: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                o:
                  type: string
          job:
            maps:
              c: '{$self.output.data.o}'
      transitions:
        t1:
          - to: a1
          - to: a2
```

## The Simplest Sequential Data Flow
This example shows how information produced by one worker function can be provided as input to another. This flow is relativey unchanged from the prior example but does modify the `transitions` section, so that both a1 and a2 execute sequentially so that the output from 'a1' is guaranteed to be available as input to 'a2'.

When executed, this flow will expect a payload with a field named 'a' (the input) and will return a payload with fields 'b' and 'c' (the output). The input will be provided to the first worker function which will transform the input. The transformed input will then be passed to the second worker function where it will be further modified. Finally, the output of the second worker function will be returned to the caller as the job data (the job response/output).

```yaml
# abc.7.yaml
app:
  id: abc
  version: '7'
  graphs:
    - subscribes: abc.test

      input:
        schema:
          type: object
          properties:
            a:
              type: string

      output:
        schema:
          type: object
          properties:
            b:
              type: string
            c:
              type: string

      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
          input:
            schema:
              type: object
              properties:
                x:
                  type: string
            maps:
              x: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                y:
                  type: string
          job:
            maps:
              b: '{$self.output.data.y}'
        a2:
          type: worker
          topic: work.do.more
          input:
            schema:
              type: object
              properties:
                i:
                  type: string
            maps:
              i: '{a1.output.data.y}'
          output:
            schema:
              type: object
              properties:
                o:
                  type: string
          job:
            maps:
              c: '{$self.output.data.o}'
      transitions:
        t1:
          - to: a1
        a1:
          - to: a2
```

Here is the updated end-to-end example with the entire evolution of the application from version `1` to version `7`. Note how the final response for workflow 6 will be `{ b: 'hello world', c: 'hello world'}` while the final response for workflow 7 will be `{ b: 'hello world', c: 'hello world world'}`. This is expected as the output of the first worker function is passed to the second worker function in workflow 7, revealing the additive nature of sequential execution.

```javascript
import Redis from 'ioredis';
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',

  engine: {
    redis: {
      class: Redis,
      options: { host, port, password, db }
    }
  },

  workers: [
    { 
      topic: 'work.do',
      redis: {
        class: Redis,
        options: { host, port, password, db }
      }
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: { y: `${data?.data?.x} world` }
        };
      }
    },

    { 
      topic: 'work.do.more',
      redis: {
        class: Redis,
        options: { host, port, password, db }
      }
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: { o: `${data?.data?.i} world` }
        };
      }
    }

  ]
});

await hotMesh.deploy('./abc.1.yaml');
await hotMesh.activate('1');
const response1 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');
const response2 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.3.yaml');
await hotMesh.activate('3');
const response3 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.4.yaml');
await hotMesh.activate('4');
const response4 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.5.yaml');
await hotMesh.activate('5');
const response5 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response5.data.b); // hello world

await hotMesh.deploy('./abc.6.yaml');
await hotMesh.activate('6');
const response6 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response6.data.b); // hello world
console.log(response6.data.c); // hello world

await hotMesh.deploy('./abc.7.yaml');
await hotMesh.activate('7');
const response7 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response7.data.b); // hello world
console.log(response7.data.c); // hello world world
```

## The Simplest Compositional Executable Flow
This example depicts a composed set of 2 flows, passing data from the first flow to the second flow. The second flow calls the same worker function used in prior examples (The function identified by the `work.do` topic) to transform the input before returning it to the first flow.

```yaml
# abc.8.yaml
app:
  id: abc
  version: '8'
  graphs:
    - subscribes: abc.test
      input:
        schema:
          type: object
          properties:
            a:
              type: string

      output:
        schema:
          type: object
          properties:
            b:
              type: string

      activities:
        t1:
          type: trigger
        a1:
          type: await
          topic: some.other.topic
          input:
            schema:
              type: object
              properties:
                awaitInput1:
                  type: string
            maps:
              awaitInput1: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                awaitOutput1:
                  type: string
          job:
            maps:
              b: '{$self.output.data.awaitOutput1}'

      transitions:
        t1:
          - to: a1

    - subscribes: some.other.topic
      input:
        schema:
          type: object
          properties:
            awaitInput1:
              type: string

      output:
        schema:
          type: object
          properties:
            awaitOutput1:
              type: string

      activities:
        t2:
          type: trigger
        a2:
          type: worker
          topic: work.do
          input:
            schema:
              type: object
              properties:
                x:
                  type: string
            maps:
              x: '{t2.output.data.awaitInput1}'
          output:
            schema:
              type: object
              properties:
                y:
                  type: string
          job:
            maps:
              awaitOutput1: '{$self.output.data.y}'

      transitions:
        t2:
          - to: a2
```

Test the output of the composed flow:

```javascript
// continued from prior example
await hotMesh.deploy('./abc.7.yaml');
await hotMesh.activate('7');
const response7 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response7.data.b); // hello world
console.log(response7.data.c); // hello world world

await hotMesh.deploy('./abc.8.yaml');
await hotMesh.activate('8');
const response8 = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response8.data.b); // hello world
```

## The Simplest Conditional Executable Flow
This example depicts two sequential workers. The first worker will conditionally run if the input (a) is not equal to `goodbye` or `bye`. The second worker will only run if the output produced by worker `a1` (`a1.output.data.y`) is equal to `hello world`. Refer to the `transitions` section in the following flow for details.

>Note how the [@pipes](./data_mapping.md) syntax can be used to design robust comparison expressions for fine-grained control over the execution flow.

```yaml
# abc.9.yaml
app:
  id: abc
  version: '9'
  graphs:
    - subscribes: abc.test

      input:
        schema:
          type: object
          properties:
            a:
              type: string

      output:
        schema:
          type: object
          properties:
            b:
              type: string
            c:
              type: string

      activities:
        t1:
          type: trigger
        a1:
          type: worker
          topic: work.do
          input:
            schema:
              type: object
              properties:
                x:
                  type: string
            maps:
              x: '{t1.output.data.a}'
          output:
            schema:
              type: object
              properties:
                y:
                  type: string
          job:
            maps:
              b: '{$self.output.data.y}'
        a2:
          type: worker
          topic: work.do.more
          input:
            schema:
              type: object
              properties:
                i:
                  type: string
            maps:
              i: '{a1.output.data.y}'
          output:
            schema:
              type: object
              properties:
                o:
                  type: string
          job:
            maps:
              c: '{$self.output.data.o}'
      transitions:
        t1:
          - to: a1
            conditions:
              gate: and
              match:
                - expected: false
                  actual: 
                    '@pipe':
                      - ['{t1.output.data.a}', 'goodbye']
                      - ['{@conditional.equality}']
                - expected: false
                  actual: 
                    '@pipe':
                      - ['{t1.output.data.a}', 'bye']
                      - ['{@conditional.equality}']
        a1:
          - to: a2
            conditions:
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - ['{a1.output.data.y}', 'hello world']
                      - ['{@conditional.equality}']
```

Note how the workflow will output values for fields b and/or c depending upon what is input into field a. If the input is `goodbye` or `bye`, the workflow will only execute the trigger (t1) and then immediately end. If the input is `hello` (and the output produced by `a1` is 'hello world'), then the workflow will also execute activity `a2`.

```javascript
// continued from prior example
await hotMesh.deploy('./abc.8.yaml');
await hotMesh.activate('8');
const response8 = await hotMesh.pubsub('abc.test', { a : 'hello' });

console.log(response8.data.b); // hello world
console.log(response8.data.c); // hello world world

await hotMesh.deploy('./abc.9.yaml');
await hotMesh.activate('9');

const response9a = await hotMesh.pubsub('abc.test', { a : 'goodbye' });
console.log(response9a.data); // undefined

const response9b = await hotMesh.pubsub('abc.test', { a : 'help' });
console.log(response9b.data.b); // help world
console.log(response9b.data.c); // undefined

const response9c = await hotMesh.pubsub('abc.test', { a : 'hello' });
console.log(response9c.data.b); // hello world
console.log(response9c.data.c); // hello world world
```
