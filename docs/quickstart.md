# Quick Start

This guide demonstrates how to build sophisticated workflows using HotMesh, progressing from basic flows to complex, conditional, and compositional patterns. Each example builds upon the previous one, showing how to evolve a simple application into a production-ready workflow system.

**Table of Contents**
- [Setup](#setup)
  - [Install Packages](#install-packages)
  - [Configure and Initialize Postgres](#configure-and-initialize-postgres)
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

### Configure and Initialize Postgres
Set up your Postgres connection. These examples use the `pg` package, but you can adapt the connection configuration for your database setup.

```javascript
import { Client as Postgres } from 'pg';
```

### Configure and Initialize HotMesh
Initialize HotMesh with your database connection:

```javascript
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',
  engine: {
    connection: {
      class: Postgres,
      options: {
        connectionString: 'postgresql://user:pass@localhost:5432/db'
      }
    }
  }
});
```

Before running workflows, applications must be *defined*, *deployed*, and *activated*. This three-step process is performed once before calling workflow endpoints like `pub` and `pubsub`.

### Define the Application
Start with the simplest possible application: a single flow containing one activity. While this isn't technically a workflow (it terminates immediately), it establishes the foundation for more complex flows.

Save this YAML descriptor as `abc.1.yaml`:

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
Deploy compiles and stores the YAML descriptor in the database, making it accessible to all connected engines and workers:

```javascript
await hotMesh.deploy('./abc.1.yaml');
```

### Activate the Application
Activation coordinates the entire fleet to simultaneously reference and execute the specified version using the database's notification system:

```javascript
await hotMesh.activate('1');
```

The flow is now active and ready for invocation. Since this flow contains only a trigger activity, it terminates immediately. The response contains metadata about the execution (job id, status, creation time, etc.) but no data.

The `abc.test` topic triggers the flow, matching the topic defined in the YAML descriptor:

```javascript
const response = await hotMesh.pubsub('abc.test', {});
console.log(response.metadata);
```

### End-to-End Example
Complete setup example showing deployment and activation of version 1:

```javascript
import { Client as Postgres } from 'pg';
import { HotMesh } from '@hotmeshio/hotmesh';

const hotMesh = await HotMesh.init({
  appId: 'abc',
  engine: {
    connection: {
      class: Postgres,
      options: {
        connectionString: 'postgresql://user:pass@localhost:5432/db'
      }
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
A workflow requires at least one transition between activities. This example adds a `transitions` section to define the flow from trigger `t1` to activity `a1`. Save this YAML descriptor as `abc.2.yaml`:

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

Use HotMesh's hot deployment capability to upgrade from version 1 to version 2:

```javascript
// continued from prior example

await hotMesh.deploy('./abc.1.yaml');
await hotMesh.activate('1');

const response1 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');

const response2 = await hotMesh.pubsub('abc.test', {});
```

The caller experience remains unchanged, but internally two activities now execute: the trigger transitions to the hook activity, which then completes the flow.

## The Simplest Compositional Flow
Composition allows one flow to trigger another, enabling standardization and component reuse. This example demonstrates the simplest compositional pattern where flow 1 calls flow 2. Save this YAML descriptor as `abc.3.yaml`:

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

Upgrade to version 3 and test:

```javascript
// continued from prior example

await hotMesh.deploy('./abc.2.yaml');
await hotMesh.activate('2');

const response2 = await hotMesh.pubsub('abc.test', {});

await hotMesh.deploy('./abc.3.yaml');
await hotMesh.activate('3');

const response3 = await hotMesh.pubsub('abc.test', {});
```

Two flows now execute sequentially: flow 1 transitions from trigger to await activity, which calls flow 2 using the `some.other.topic` topic. Flow 2 executes its trigger and terminates, then flow 1 completes.

## The Simplest Executable Flow
This example introduces actual work execution by defining a `worker` activity. Workers perform computational tasks on your network. Save this YAML descriptor as `abc.4.yaml`:

The `work.do` topic identifies the worker function to execute. Choose topic names that match your use case and organizational conventions.

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

When defining worker activities, register corresponding worker functions that perform the actual work. The updated initialization now includes worker registration with additional database connections. HotMesh instances can declare engines (for coordination) and/or workers (for execution):

```javascript
import { Client as Postgres } from 'pg';
import { HotMesh } from '@hotmeshio/hotmesh';

const connection = {
  class: Postgres,
  options: {
    connectionString: 'postgresql://user:pass@localhost:5432/db'
  }
};

const hotMesh = await HotMesh.init({
  appId: 'abc',
  engine: { connection },
  workers: [
    { 
      topic: 'work.do',
      connection,
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
This example demonstrates data exchange using JSON Schema validation and mapping. Input and output schemas define data structure, while mapping statements bind data between activities. Save this YAML descriptor as `abc.5.yaml`:

This flow expects input with field 'a' and returns output with field 'b'. The worker function receives the input, transforms it, and returns the output that becomes the job response.

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

The complete evolution from version 1 to version 5. The final workflow response now includes output data from the worker function (`hello world`). Variable names (a, b, x, y) are arbitrary - choose names that reflect your use case:

```javascript
import { Client as Postgres } from 'pg';
import { HotMesh } from '@hotmeshio/hotmesh';

const connection = {
  class: Postgres,
  options: {
    connectionString: 'postgresql://user:pass@localhost:5432/db'
  }
};

const hotMesh = await HotMesh.init({
  appId: 'abc',

  engine: { connection },

  workers: [
    { 
      topic: 'work.do',
      connection,
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
This example demonstrates parallel execution where two worker functions process the same input simultaneously. The flow adds a second worker (`a2`) that runs in parallel with the first. Save this YAML descriptor as `abc.6.yaml`:

This flow expects input with field 'a' and returns output with fields 'b' and 'c'. Both workers receive identical input data and execute in parallel, each contributing a field to the output.

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
This example demonstrates sequential execution where the output of one worker becomes the input to the next. The transitions section is modified so that a1 and a2 execute sequentially, ensuring a1's output is available as input to a2.

This flow expects input with field 'a' and returns output with fields 'b' and 'c'. The input flows through the first worker, gets transformed, then flows through the second worker for additional transformation.

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

Complete evolution from version 1 to version 7. Note the different outputs: workflow 6 produces `{ b: 'hello world', c: 'hello world'}` while workflow 7 produces `{ b: 'hello world', c: 'hello world world'}`. The sequential execution in workflow 7 demonstrates the additive nature of chained transformations:

```javascript
import { Client as Postgres } from 'pg';
import { HotMesh } from '@hotmeshio/hotmesh';

const connection = {
  class: Postgres,
  options: {
    connectionString: 'postgresql://user:pass@localhost:5432/db'
  }
};

const hotMesh = await HotMesh.init({
  appId: 'abc',

  engine: { connection },

  workers: [
    { 
      topic: 'work.do',
      connection,
      callback: async (data: StreamData) => {
        return {
          metadata: { ...data.metadata },
          data: { y: `${data?.data?.x} world` }
        };
      }
    },

    { 
      topic: 'work.do.more',
      connection,
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
This example demonstrates composition with data flow - passing data from one flow to another. The first flow sends data to a second flow, which executes a worker function to transform the data before returning it.

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

Testing the composed flow:

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
This example demonstrates conditional execution using transition conditions. The first worker runs conditionally based on input validation, and the second worker runs conditionally based on the first worker's output.

The first worker executes only if the input (a) is not equal to `goodbye` or `bye`. The second worker executes only if the first worker's output (`a1.output.data.y`) equals `hello world`. The transitions section uses [@pipes](./data_mapping.md) syntax for robust comparison expressions:

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

The workflow produces different outputs based on the input:
- Input `goodbye` or `bye`: only trigger executes, workflow ends immediately
- Input `hello`: both workers execute (a1's output is 'hello world', triggering a2)
- Other input: only a1 executes (a1's output doesn't match 'hello world', so a2 doesn't run)

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
