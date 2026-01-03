# HotMesh

![beta release](https://img.shields.io/badge/release-beta-blue.svg)

HotMesh converts any Postgres database into a workflow orchestration system—no servers, no infrastructure, just intelligent coordination.


**Table of Contents**
- [Common Use Cases](#common-use-cases)
- [Installation](#installation)
- [Quick Example: If/Else Workflow](#quick-example-ifelse-workflow)
  - [MemFlow Approach (Temporal-Compatible)](#memflow-approach-temporal-compatible)
  - [HotMesh Approach (Functional YAML)](#hotmesh-approach-functional-yaml)
- [The Power of Transpilation](#the-power-of-transpilation)
- [Key Features](#key-features)
- [Progressive Orchestration](#progressive-orchestration)
- [Advanced Capabilities](#advanced-capabilities)
- [License](#license)

## Common Use Cases

### 1. Pipeline Database
Transform Postgres into a durable pipeline processor. Orchestrate long-running, multi-step pipelines transactionally and durably.

### 2. Temporal You Own
Get the power of Temporal without the infrastructure. HotMesh includes MemFlow, a Temporal-compatible API that runs directly on your Postgres database. No app server required.

### 3. Distributed State Machine
Build resilient, stateful applications where every component can fail and recover. HotMesh manages state transitions, retries, and coordination.

### 4. Workflow-as-Code Platform
Choose your style: procedural workflows with MemFlow's Temporal API, or functional workflows with HotMesh's YAML syntax.

## Installation

```bash
npm install @hotmeshio/hotmesh
```

## Quick Example: If/Else Workflow

### MemFlow Approach (Temporal-Compatible)

First, define your activities in a separate file (standard TypeScript functions):

```typescript
// activities.ts
export async function checkInventory(itemId: string): Promise<number> {
  // Query inventory database
  return getInventoryCount(itemId);
}

export async function reserveItem(itemId: string, quantity: number): Promise<string> {
  // Reserve item in inventory
  return createReservation(itemId, quantity);
}

export async function notifyBackorder(itemId: string): Promise<void> {
  // Send backorder notification
  await sendBackorderEmail(itemId);
}
```

Define your workflow (it should orchestrate your activities according to your business logic):

```typescript
// workflows.ts
import { MemFlow } from '@hotmeshio/hotmesh';
import * as activities from './activities';

export async function orderWorkflow(itemId: string, requestedQty: number) {
  const { checkInventory, reserveItem, notifyBackorder } = MemFlow.workflow.proxyActivities<typeof activities>({
    taskQueue: 'inventory-tasks',
    retryPolicy: {
      maximumAttempts: 3,
      backoffCoefficient: 2,
      maximumInterval: '300s'
    }
  });
  
  const availableQty = await checkInventory(itemId);
  
  if (availableQty >= requestedQty) {
    return await reserveItem(itemId, requestedQty);
  } else {
    await notifyBackorder(itemId);
    return 'backordered';
  }
}
```

Then register your workers and execute:

```typescript
// main.ts
import { MemFlow } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';
import * as activities from './activities';
import { orderWorkflow } from './workflows';

const connection = {
  class: Postgres,
  options: { connectionString: 'postgresql://localhost:5432/mydb' }
};

// Register activity worker
await MemFlow.registerActivityWorker({
  connection,
  taskQueue: 'inventory-tasks'
}, activities, 'inventory-activities');

// Create workflow worker
await MemFlow.Worker.create({
  connection,
  taskQueue: 'orders',
  workflow: orderWorkflow
});

// Execute workflow
const client = new MemFlow.Client({ connection });

const handle = await client.workflow.start({
  args: ['item-123', 5],
  taskQueue: 'orders',
  workflowName: 'orderWorkflow',
  workflowId: 'order-456'
});

const result = await handle.result();
console.log(result); // 'reservation-789' or 'backordered'
```

### HotMesh Approach (Functional YAML)

The same workflow and activities can be expressed using HotMesh's declarative YAML syntax. First, define the workflow graph:

```yaml
# order.yaml
app:
  id: orders
  version: '1'
  graphs:
    - subscribes: order.requested
      
      input:
        schema:
          type: object
          properties:
            itemId:
              type: string
            requestedQty:
              type: number
      
      output:
        schema:
          type: object
          properties:
            result:
              type: string
      
      activities:
        trigger:
          type: trigger
        
        checkInventory:
          type: worker
          topic: inventory.check
          input:
            maps:
              itemId: '{trigger.output.data.itemId}'
          output:
            schema:
              type: object
              properties:
                availableQty:
                  type: number
        
        reserveItem:
          type: worker
          topic: inventory.reserve
          input:
            maps:
              itemId: '{trigger.output.data.itemId}'
              quantity: '{trigger.output.data.requestedQty}'
          output:
            schema:
              type: object
              properties:
                reservationId:
                  type: string
          job:
            maps:
              result: '{$self.output.data.reservationId}'
        
        notifyBackorder:
          type: worker
          topic: inventory.backorder.notify
          input:
            maps:
              itemId: '{trigger.output.data.itemId}'
          job:
            maps:
              result: 'backordered'
      
      transitions:
        trigger:
          - to: checkInventory
        
        checkInventory:
          - to: reserveItem
            conditions:
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - ['{checkInventory.output.data.availableQty}', '{trigger.output.data.requestedQty}']
                      - ['{@conditional.gte}']
          
          - to: notifyBackorder
            conditions:
              match:
                - expected: false
                  actual:
                    '@pipe':
                      - ['{checkInventory.output.data.availableQty}', '{trigger.output.data.requestedQty}']
                      - ['{@conditional.gte}']
```

Then bind the same activities to worker topics and deploy:

```typescript
// main.ts (uses same activities.ts)
import { HotMesh } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';
import * as activities from './activities';

const connection = {
  class: Postgres,
  options: { connectionString: 'postgresql://localhost:5432/mydb' }
};

const retryPolicy = {
  maximumAttempts: 3,
  backoffCoefficient: 2,
  maximumInterval: '300s'
};

const hotMesh = await HotMesh.init({
  appId: 'orders',
  engine: { connection },
  workers: [
    {
      topic: 'inventory.check',
      connection,
      retryPolicy,
      callback: async (data) => {
        const availableQty = await activities.checkInventory(data.data.itemId);
        return {
          metadata: { ...data.metadata },
          data: { availableQty }
        };
      }
    },
    {
      topic: 'inventory.reserve',
      connection,
      retryPolicy,
      callback: async (data) => {
        const reservationId = await activities.reserveItem(
          data.data.itemId, 
          data.data.quantity
        );
        return {
          metadata: { ...data.metadata },
          data: { reservationId }
        };
      }
    },
    {
      topic: 'inventory.backorder.notify',
      connection,
      retryPolicy,
      callback: async (data) => {
        await activities.notifyBackorder(data.data.itemId);
        return { metadata: { ...data.metadata } };
      }
    }
  ]
});

// Deploy and activate
await hotMesh.deploy('./order.yaml');
await hotMesh.activate('1');

// Execute workflow
const result = await hotMesh.pubsub('order.requested', {
  itemId: 'item-123',
  requestedQty: 5
});

console.log(result.data.result); // 'reservation-789' or 'backordered'
```

## The Power of Transpilation

Notice how both approaches implement the same logic:
- Check inventory availability
- If available: reserve the item
- If not: send backorder notification

MemFlow's procedural code can transpile to HotMesh's functional YAML. The mesh of engines executes either representation identically.

## Key Features

### Zero Infrastructure
- No workflow servers to manage
- No separate state stores
- No additional databases
- Just Postgres and your application code

### Built-in Resilience
- Automatic retries with exponential backoff
- Durable execution through database streams
- Crash recovery without data loss
- Hot deployments with zero downtime

### Complete Flexibility
- Choose procedural (MemFlow) or functional (HotMesh) style
- Mix and match approaches in the same system
- Seamless interoperability between styles
- Full Temporal API compatibility with MemFlow

### Distributed by Design
- Every database client is part of the mesh
- Automatic load distribution
- No single points of failure
- Scale by adding database connections


## Progressive Orchestration

Workflow systems tend to force a choice between two models:

* **Choreography**: Distributed by design, but difficult to reason about, observe, and debug
* **Orchestration**: Easier to model and visualize, but dependent on centralized infrastructure

HotMesh removes the need to choose. It preserves the explicit structure of orchestration while operating in a fully distributed way. Workflow state lives in the database, and participating clients act as peers in the execution mesh. 

## Advanced Capabilities

- **Long-running workflows**: Durable sleep, wait conditions
- **Workflow composition**: Parent/child workflows, sub-workflows
- **Entity management**: Built-in JSONB state management
- **Interceptors**: Cross-cutting concerns as durable functions
- **Observability**: OpenTelemetry integration
- **Time travel**: Replay workflows from any point
- **Targeted throttling**: Control message flow rate for workers
- **Hot deployments**: Update workflows without downtime

---

**Retain your process data. Learn from your process data.**

## License

HotMesh is licensed under the Apache License, Version 2.0.

You may use, modify, and distribute HotMesh in accordance with the license, including as part of your own applications and services. However, offering HotMesh itself as a standalone, hosted commercial orchestration service (or a substantially similar service) requires prior written permission from the authors.
