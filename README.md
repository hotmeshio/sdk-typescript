# HotMesh

![beta release](https://img.shields.io/badge/release-beta-blue.svg)

Run durable workflows on Postgres. No servers, no queues, just your database.


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

## Two ways to write workflows

Both approaches reuse your activity functions:

```typescript
// activities.ts (shared between both approaches)
export async function checkInventory(itemId: string): Promise<number> {
  return getInventoryCount(itemId);
}

export async function reserveItem(itemId: string, quantity: number): Promise<string> {
  return createReservation(itemId, quantity);
}

export async function notifyBackorder(itemId: string): Promise<void> {
  await sendBackorderEmail(itemId);
}
```

### Option 1: Code (Temporal-compatible API)

```typescript
// workflows.ts
import { MemFlow } from '@hotmeshio/hotmesh';
import * as activities from './activities';

export async function orderWorkflow(itemId: string, qty: number) {
  const { checkInventory, reserveItem, notifyBackorder } = 
    MemFlow.workflow.proxyActivities<typeof activities>({
      taskQueue: 'inventory-tasks'
    });
  
  const available = await checkInventory(itemId);
  
  if (available >= qty) {
    return await reserveItem(itemId, qty);
  } else {
    await notifyBackorder(itemId);
    return 'backordered';
  }
}

// main.ts
const connection = {
  class: Postgres,
  options: { connectionString: 'postgresql://localhost:5432/mydb' }
};

await MemFlow.registerActivityWorker({
  connection,
  taskQueue: 'inventory-tasks'
}, activities, 'inventory-activities');

await MemFlow.Worker.create({
  connection,
  taskQueue: 'orders',
  workflow: orderWorkflow
});

const client = new MemFlow.Client({ connection });
const handle = await client.workflow.start({
  args: ['item-123', 5],
  taskQueue: 'orders',
  workflowName: 'orderWorkflow',
  workflowId: 'order-456'
});

const result = await handle.result();
```

### Option 2: YAML (functional approach)

```yaml
# order.yaml
activities:
  trigger:
    type: trigger
    
  checkInventory:
    type: worker
    topic: inventory.check
    
  reserveItem:
    type: worker
    topic: inventory.reserve
    
  notifyBackorder:
    type: worker
    topic: inventory.backorder.notify
    
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

```typescript
// main.ts (reuses same activities.ts)
import * as activities from './activities';

const hotMesh = await HotMesh.init({
  appId: 'orders',
  engine: { connection },
  workers: [
    {
      topic: 'inventory.check',
      connection,
      callback: async (data) => {
        const availableQty = await activities.checkInventory(data.data.itemId);
        return { metadata: { ...data.metadata }, data: { availableQty } };
      }
    },
    {
      topic: 'inventory.reserve',
      connection,
      callback: async (data) => {
        const reservationId = await activities.reserveItem(data.data.itemId, data.data.quantity);
        return { metadata: { ...data.metadata }, data: { reservationId } };
      }
    },
    {
      topic: 'inventory.backorder.notify',
      connection,
      callback: async (data) => {
        await activities.notifyBackorder(data.data.itemId);
        return { metadata: { ...data.metadata } };
      }
    }
  ]
});

await hotMesh.deploy('./order.yaml');
await hotMesh.activate('1');

const result = await hotMesh.pubsub('order.requested', {
  itemId: 'item-123',
  requestedQty: 5
});
```

Both compile to the same distributed execution model.

## Core features

- **Durable execution** - Survives crashes, retries automatically
- **No infrastructure** - Runs on your existing Postgres
- **Temporal compatible** - Drop-in replacement for many use cases
- **Distributed** - Every client participates in execution
- **Observable** - Full execution history in your database

## Common patterns

**Long-running workflows**

```typescript
await sleep('30 days');
await sendFollowUp();
```

**Parallel execution**

```typescript
const results = await Promise.all([
  processPayment(),
  updateInventory(),
  notifyWarehouse()
]);
```

**Child workflows**

```typescript
const childHandle = await startChild(validateOrder, { args: [orderId] });
const validation = await childHandle.result();
```

## License

HotMesh is licensed under the Apache License, Version 2.0.

You may use, modify, and distribute HotMesh in accordance with the license, including as part of your own applications and services. However, offering HotMesh itself as a standalone, hosted commercial orchestration service (or a substantially similar service) requires prior written permission from the author.
