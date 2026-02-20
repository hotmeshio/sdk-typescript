# HotMesh

![beta release](https://img.shields.io/badge/release-beta-blue.svg)

Run durable workflows on Postgres. No servers, no queues, just your database.

```bash
npm install @hotmeshio/hotmesh
```

## Use HotMesh for...

- **Durable pipelines** — Orchestrate long-running, multi-step pipelines transactionally.
- **Temporal replacement** — MemFlow provides a Temporal-compatible API that runs directly on Postgres. No app server required.
- **Distributed state machines** — Build stateful applications where every component can [fail and recover](https://github.com/hotmeshio/sdk-typescript/blob/main/services/collator/README.md).
- **AI and training pipelines** — Multi-step AI workloads where each stage is expensive and must not be repeated on failure. A crashed pipeline resumes from the last committed step, not from the beginning.

## How it works in 30 seconds

1. **You write workflow functions.** Plain TypeScript — branching, loops, error handling. HotMesh also supports a YAML syntax for declarative, functional workflows.
2. **HotMesh compiles them into a transactional execution plan.** Each step becomes a committed database row. If the process crashes mid-workflow, it resumes from the last committed step.
3. **Your Postgres database is the engine.** It stores state, coordinates retries, and delivers messages. Every connected client participates in execution — there is no central server.

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

## It's just data

There is nothing between you and your data. Workflow state lives in your database as ordinary rows — `jobs` and `jobs_attributes`. Query it directly, back it up with pg_dump, replicate it, join it against your application tables.

```sql
SELECT
  j.key          AS job_key,
  j.status       AS semaphore,
  j.entity       AS workflow,
  a.field        AS attribute,
  a.value        AS value,
  j.created_at,
  j.updated_at
FROM
  jobs j
  JOIN jobs_attributes a ON a.job_id = j.id
WHERE
  j.key = 'order-456'
ORDER BY
  a.field;
```

What happened? Consult the database. What's still running? Query the semaphore. What failed? Read the row. The execution state isn't reconstructed from a log — it was committed transactionally as each step ran.

You can also use the Temporal-compatible API:

```typescript
const handle = client.workflow.getHandle('orders', 'orderWorkflow', 'order-456');

const result = await handle.result();           // final output
const status = await handle.status();           // semaphore (0 = complete)
const state  = await handle.state(true);        // full state with metadata
const exported = await handle.export({          // selective export
  allow: ['data', 'state', 'status', 'timeline']
});
```

## Architecture

For a deep dive into the transactional execution model — how every step is crash-safe, how the monotonic collation ledger guarantees exactly-once delivery, and how cycles and retries remain correct under arbitrary failure — see the [Collation Design Document](https://github.com/hotmeshio/sdk-typescript/blob/main/services/collator/README.md). The symbolic system (how to design workflows) and lifecycle details (how to deploy workflows) are covered in the [Architectural Overview](https://zenodo.org/records/12168558).

## License

HotMesh is source-available under the [HotMesh Source Available License](./LICENSE).
