# My First App: Network Calculator

The tutorial uses the creation of a networked calculator application to illustrate important concepts about designing and running distributed applications at scale. Whether you deploy one worker of dozens, nothing changes in the deployment and configuration. HotMesh will calcuate values as fast as Redis can distribute the work streams.

## Table of Contents

1. [Setting up Your Development Environment](#setting-up-your-development-environment)
2. [Foundational Concepts](#foundational-concepts)
    - [Activities](#activities)
    - [Subscriptions and Publications](#subscriptions-and-publications)
    - [Using YAML Files](#using-yaml-files)
3. [Flow 1: Calculate](#flow-1-calculate)
    - [Transitions](#transitions)
    - [Schemas](#schemas)
    - [Maps](#maps)
4. [Flow 2: Calculation Executor](#flow-2-calculation-executor)
    - [Transitions](#transitions-1)
    - [Schemas](#schemas-1)
    - [Maps](#maps-1)
5. [Deploying and Activating Your Application](#deploying-and-activating-your-application)
6. [Invoking Your Application's Endpoints](#invoking-your-applications-endpoints)
    - [Pub](#pub)
    - [Sub](#sub)
    - [PubSub](#pubsub)

## Setting up Your Development Environment
To get started with HotMesh, you need to set up your development environment. This involves installing required packages and setting up the database. Follow the steps below:

1. **Install the HotMesh Package**: Open your terminal and run the following command to install HotMesh:

    ```bash
    npm install @hotmeshio/hotmesh
    ```

2. **Install Redis/IORedis Packages**: HotMesh uses Redis, and both `redis` and `ioredis` NPM packages can be used. Choose whichever package you are familiar with.

    ```bash
    npm install redis
    npm install ioredis
    ```

3. **Set Up Redis**: Ensure that your Redis instance is up and running. If you are running Redis locally, the default host is `localhost` and the default port is `6379`. For more information on how to set up Redis, refer to the official Redis [documentation](https://redis.io/topics/quickstart).

4. **Initialize HotMesh**: You need to initialize HotMesh with a configuration object that includes your application ID, namespace, store (Redis in this case), and any adapters for handling specific topics.

5. **Deploy and Activate Your App**: The final step involves deploying your application configuration using the `deploy` method, and then activating your application using the `activate` method.

## Foundational Concepts
Before diving into the worflows, let's cover some foundational aspects in HotMesh.

### Activities
In HotMesh, activities are the basic building blocks of a workflow. They represent a single task or operation that needs to be performed in a workflow. They'll be referenced throughout, so it's good to understand the basic types.

There are four primary types of activities in HotMesh:

1. `activity`: A vanilla `activity` is a pass-through activity that is useful for precalculating expensive mappings. It doesn't make external calls but can be combined with a WebHook for modeling aggregate-and-release use cases.

2. `trigger`: A `trigger` activity initiates the execution of a workflow. It is the single entry point for kicking off a workflow.

3. `await`: An `await` activity waits for a response from another workflow before proceeding. All other activities in the workflow running in parallel will continue to run as expected; however, the await activity will pause until the response is returned from the spawned workflow job.

4. `worker`: A `worker` activity identifies a function on the network where HotMesh has a presence. If you have a microservice running an instance of HotMesh, you can register a function with an arbitrary topic of your choice and any time that HotMesh runs a workflow with a `worker` activity with this topic, it will call the function in Job context, passing all job data described by the schema. In the following example, HotMesh has been initialized to run the function when the `calculation.execute` topic is encountered in a flow. The function will execute in buffered job context, isolated from network back-pressure risk.

    ```typescript
    import {
      HotMesh,
      HotMeshConfig,
      RedisStore
      RedisStream,
      RedisSub } from '@hotmeshio/hotmesh';

    //init 3 Redis clients
    const redisClient1 = getMyRedisClient();
    const redisClient2 = getMyRedisClient();
    const redisClient3 = getMyRedisClient();

    const config: HotMeshConfig = {
      appId: "my-app",
      workers: [
        { 
          topic: 'calculation.execute',
          store: redisClient1,
          stream: redisClient2,
          sub: redisClient3,
          callback: async (data: StreamData) => {
            //do something with data and return
          }
        }
      ]
    };

    hotMesh = await HotMesh.init(config);
    ```

### Subscriptions and Publications
In HotMesh, activities subscribe to topics and publish to them. The "subscribes" key in the YAML configuration defines the topic an activity is subscribing to. Similarly, the "publishes" key defines the topic to which the activity is publishing.

```yaml
subscribes: calculate
publishes: calculated
```

In the above snippet, the activity subscribes to the 'calculate' topic and publishes to the 'calculated' topic.

### YAML Configuration Files
The YAML files in HotMesh should be organized for maintainability and efficiency. It's often easiest to name them according to their subscription topic and place them in appropriate directories, namely `/graphs`, `/schemas`, and `/maps`. Here's an example of how the app config files are organized for this tutorial. (*The source files for this tutorial are located [here](../tests/%24setup/apps/calc/v1/hotmesh.yaml).*).

```plaintext
/src
  ├── /hotmesh.yaml
  |
  ├── /graphs
  │   ├── calculate.yaml
  │   └── calculation.execute.yaml
  |
  ├── /schemas
  │   ├── calculate.yaml
  │   └── calculation.execute.yaml
  |
  └── /maps
      ├── calculate.yaml
```

The `hotmesh.yaml` file serves as the application manifest and should include a reference to every workflow that should be compiled and deployed. Removing a workflow from deployment is as simple as removing its reference from the `hotmesh.yaml` file and redeploying the app.

The files in the `/graphs`, `/schemas`, and `/maps` directories correspond to the workflows, schemas, and maps of your application, respectively.

These YAML files are crucial in defining your application's behavior, its communication with other services, and how data flows between different components.

In the next section, we will finally put all these pieces together and look at how to deploy a HotMesh application.

Now let's see how these principles come together in the workflows: 'Calculate' and 'Calculation Executor'. 

## Flow 1: Calculate
The Calculate flow consists of the 'calculate' and 'operate' activities. Here is the YAML configuration of these activities:

```yaml
input:
  schema:
    $ref: '../schemas/calculate.yaml#/input'
output:
  schema:
    $ref: '../schemas/calculate.yaml#/output'

activities:

  calculate:
    title: Calculate
    type: trigger

  operate:
    title: Operate
    type: await
    topic: calculation.execute
    input:
      schema:
        $ref: '../schemas/calculate.yaml#/input'
      maps:
        $ref: '../maps/calculate.yaml#/operate/input'
    job:
      maps:
        $ref: '../maps/calculate.yaml#/operate/job'
```

In the above example, `calculate` is a `trigger` activity and `operate` is an `await` activity.

### Transitions
Transitions define how control moves from one activity to another. They are defined in the Graphs and specify the sequence of activities.

For example:

```yaml
transitions:
  calculate:
    - to: operate
```

The above example indicates that after `calculate` activity, control moves to the `operate` activity.

### Schemas
Schemas are used to define the incoming and outgoing message format for each activity. They are defined in separate YAML files in the `/schemas` directory. Here's an example:

```yaml
input:
  type: object
  properties:
    operation:
      type: string
      enum:
        - add
        - subtract
        - multiply
        - divide
    values:
      type: array
        items: number
output:
  type: object
  properties:
    result:
      type: number
```

### Maps
Maps define how upstream activity data is copied, transformed, and used as input to downstream activities. They are defined in separate YAML files in the `/maps` directory.

```yaml
calculate:
  output:
    operation: '{calculate.input.data.operation}'
    values: '{calculate.input.data.values}'
operate:
  input:
    operation: '{calculate.output.data.operation}'
    values: '{calculate.output.data.values}'
  job:
    result: '{operate.output.data.result}'
```

This map shows how data from the `calculate` activity is used as input for the `operate` activity.

In the Calculate flow, the `calculate` activity initiates the workflow and the `operate` activity awaits the calculation execution. The transition dictates the control flow from `calculate` to `operate`. The schema and map ensure that the output from one activity is correctly transferred as the input to the next.

## Flow 2: Calculation Executor
The Calculation Executor flow consists of the 'receiver' and 'executor' activities. This flow is responsible for executing the calculations as per the input received. Let's break down its YAML configuration:

```yaml
input:
  schema:
    $ref: '../schemas/calculate.yaml#/input'
output:
  schema:
    $ref: '../schemas/calculate.yaml#/output'

activities:

  receiver:
    title: Receive Values
    type: trigger

  executor:
    title: Execute Calculation
    type: worker
    topic: calculation.execute
    input:
      schema:
        $ref: '../schemas/calculate.yaml#/input'
      maps:
        $ref: '../maps/calculation.execute.yaml#/executor/input'
    job:
      maps:
        $ref: '../maps/calculation.execute.yaml#/executor/job'
```

'Receiver' is a `trigger` type activity, which is the entry point for this flow. It receives the calculation details and passes it to the 'executor' activity.

'Executor' is a `worker` type activity, which takes the calculation details from the 'receiver' activity and executes the calculation.

### Transitions
The flow of control in this workflow is straightforward - from 'receiver' to 'executor'. This is represented by the following transition in the YAML:

```yaml
transitions:
  receiver:
    - to: executor
```

### Schemas
The schemas used in these flows are defined in the `calculate.yaml` file in the `schemas` directory. These schemas are utilized by both flows, which makes for easier maintenance and promotes consistency across different parts of the system. The choice to use JSON Schema offers a high degree of flexibility and reusability.

Here is the shared schema once again for reference:

```yaml
input:
  type: object
  properties:
    operation:
      type: string
      enum:
        - add
        - subtract
        - multiply
        - divide
    values:
      type: array
        items: number
output:
  type: object
  properties:
    result:
      type: number
```

### Maps
Maps for the `calculation.execute.yaml` workflow are located in the `maps` directory. They are similar to the other workflow but couldn't be reused, because they are mapping to a differently-named upstream activity.

```yaml
receiver:
  output:
    operation: '{receiver.input.data.operation}'
    values: '{receiver.input.data.values}'
executor:
  input:
    operation: '{receiver.output.data.operation}'
    values: '{receiver.output.data.values}'
  job:
    result: '{executor.output.data.result}'
```

## Deploying and Activating Your Application
After setting up your HotMesh application, the next step is to deploy and activate it. Here's how to do it:

1. **Initialize:** The HotMesh configuration is initialized with the application's ID, store, and a list of adapters. (NOTE: You can choose to initialize HotMesh to serve as an `engine`, `worker` or both. In this example, HotMesh is serving in both capacities, both orchestrating the workflow described by the YAML source file as well as performing the calculation in the worker context. However, it's possible to deploy as many engines and workers as you need for your use case. HotMesh will coordinate the entire fleet without any additional configuration beyond what is shown here.)

    ```javascript
    const hotMesh = HotMesh.init({
      appId: 'myapp',
      engine: {
        store: redisClient1,
        stream: redisClient2,
        sub: redisClient3,
      }
      workers: [
        {
          topic: 'calculation.execute',
          store: redisClient1,
          stream: redisClient4,
          sub: redisClient3,
          callback: async (streamData: StreamData) => {
            return {
              status: 'success',
              metadata: { ...streamData.metadata },
              data: { /* `streamData.data` is incoming */ },
            }
          }
        }
      ]
    });
    ```

2. **Deploy:** Once HotMesh is initialized, deploy to alert the networked quorum of clients.
    ```javascript
    await hotMesh.deploy('/app/v1/hotmesh.yaml');
    ```

3. **Activate:** Finally, call activate to set the active version simultaneously for all connnected clients.
    ```javascript
    await hotMesh.activate('1');
    ```

After the deployment and activation steps, your HotMesh application is ready to process events.

## Invoking Your Application's Endpoints
Once your application is deployed and activated, you'll be able to use its endpoints to perform calculations. HotMesh provides two methods for this: *pub* for one-way (fire-and-forget) messaging, and *pubsub* for stateful request/response.

### Pub
Here is an example of how to send a one-way message. You can optionally await the response (the job ID) to confirm that the request was received, but this is a simple fire-and-forget call. This is useful for tasks that don't require immediate feedback.

```javascript
const payload = { operation: 'add', values: [1, 2, 3] };
const jobId = await hotMesh.pub('calculate', payload);
//jobId is system-assigned in this context (e.g., `987656789.235`)
```

### PSub
Suppose you need to listen in on the results of all computations on a particular topic, not just the ones you initiated. In that case, you can use the patterned subscription `psub` method. It allows you to subscribe to a specific topic and define a callback function that will be executed every time a new result is published on that topic. 

This is useful in scenarios where you're interested in monitoring global computation results, performing some action based on them, or even just logging them for auditing purposes.

Here's how it might look:

```javascript
await hotMesh.psub('calculated.*', (topic: string, message: JobOutput) => {
  // `message.data.result` will be `5`
});

const payload = { operation: 'divide', values: [100, 4, 5] };
const jobId = await hotMesh.pub('calculate', payload);
```

### PubSub
If you need to send a message and wait for a response, you can use the `pubsub` method. HotMesh will subscribe to the response on your behalf, making it simple to model the request using a standard async/await. The benefit, of course, is that this is a fully duplexed call that adheres to the principles of CQRS.

```javascript
const payload = { operation: 'add', values: [1, 2, 3] };
const { data, metadata } = await hotMesh.pubsub('calculate', payload);
//data.result is `6`
```

No matter where in the network the calculation is performed (no matter the microservice that is subscribed as the official "handler" to perform the calculation), the answer will always be published back to the originating caller the moment it's ready. It's a one-time subscription handled automatically by the engine, enabling traditional read/write use cases but without network back-pressure risk.
