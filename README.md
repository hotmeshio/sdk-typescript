# HotMesh

![beta release](https://img.shields.io/badge/release-beta-blue.svg)

Run durable workflows on Postgres. No servers, no queues, just your database.

```bash
npm install @hotmeshio/hotmesh
```

## Use HotMesh for

- **Durable pipelines** — Orchestrate long-running, multi-step pipelines transactionally.
- **Crash-safe execution** — Every step is a committed row. If the process dies, it picks up where it left off.
- **Distributed state machines** — Build stateful applications where every component can [fail and recover](https://github.com/hotmeshio/sdk-typescript/blob/main/services/collator/README.md).
- **AI and training pipelines** — Multi-step AI workloads where each stage is expensive and must not be repeated on failure. A crashed pipeline resumes from the last committed step, not from the beginning.
- **Human-in-the-loop workflows** — Workflow pauses that require external input are searchable, claimable rows in your database. Build approval queues, review flows, and AI handoffs without a separate task service.

## How it works in 30 seconds

1. **You write workflow functions.** Plain TypeScript — branching, loops, error handling. HotMesh also supports a YAML syntax for declarative, functional workflows.
2. **HotMesh compiles them into a transactional execution plan.** Each step becomes a committed database row backed by Postgres ACID guarantees and monotonic integers for ordering. If the process crashes mid-workflow, it resumes from the last committed step.
3. **Your Postgres database is the engine.** It stores state, coordinates retries, and delivers messages. Every connected client participates in execution — there is no central server.

## Quickstart

Install the package:

```bash
npm install @hotmeshio/hotmesh
```

The repo includes a `docker-compose.yml` that starts Postgres and a development container:

```bash
docker compose up -d
```

See the [Durable API reference](https://docs.hotmesh.io/classes/services_durable.Durable.html) for the full API surface — workflows, activities, signals, child workflows, and more.

## Writing workflows

**Define the workflow** — plain TypeScript with branching, loops, and error handling. Activities are proxied so their results are checkpointed and replayed on restart.

```typescript
// workflows.ts
import { Durable } from '@hotmeshio/hotmesh';
import type * as activities from './activities';

export async function orderWorkflow(itemId: string, qty: number) {
  const { checkInventory, reserveItem, notifyBackorder } =
    Durable.workflow.proxyActivities<typeof activities>();

  const available = await checkInventory(itemId);

  if (available >= qty) {
    return await reserveItem(itemId, qty);
  } else {
    await notifyBackorder(itemId);
    return 'backordered';
  }
}
```

**Start a worker** — connects to Postgres and begins processing workflows on the given task queue.

```typescript
// worker.ts
import { Durable } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';
import { orderWorkflow } from './workflows';

const connection = {
  class: Postgres,
  options: { connectionString: 'postgresql://localhost:5432/mydb' }
};

const worker = await Durable.Worker.create({
  connection,
  taskQueue: 'orders',
  workflow: orderWorkflow,
});

await worker.run();
```

**Run a workflow** — start an execution and await its result. The client can run in a different process, container, or server.

```typescript
// client.ts
import { Durable } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

const connection = {
  class: Postgres,
  options: { connectionString: 'postgresql://localhost:5432/mydb' }
};

const client = new Durable.Client({ connection });
const handle = await client.workflow.start({
  args: ['item-123', 5],
  taskQueue: 'orders',
  workflowName: 'orderWorkflow',
  workflowId: 'order-456',
});

const result = await handle.result();
```

### Activities

Activities are your side-effectful functions — database calls, API requests, anything non-deterministic. HotMesh checkpoints their results so they're never re-executed on replay.

```typescript
// activities.ts
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

## Common patterns

All snippets below run inside a workflow function (like `orderWorkflow` above). Durable methods are available as static imports:

```typescript
import { Durable } from '@hotmeshio/hotmesh';
```

**Long-running workflows** — `sleep` is durable. The process can restart; the timer survives.

```typescript
// sendFollowUp is a proxied activity from proxyActivities()
await Durable.workflow.sleep('30 days');
await sendFollowUp();
```

**Parallel execution** — fan out to multiple activities and wait for all results.

```typescript
// proxied activities run as durable, retryable steps
const [payment, inventory, shipment] = await Promise.all([
  processPayment(orderId),
  updateInventory(orderId),
  notifyWarehouse(orderId)
]);
```

**Child workflows** — compose workflows from other workflows.

```typescript
const result = await Durable.workflow.executeChild({
  args: [orderId],
  taskQueue: 'validation',
  workflowName: 'validateOrder',
  workflowId: `validate-${orderId}`,
});
```

**Signals** — pause a workflow until an external event arrives.

```typescript
const approval = await Durable.workflow.condition<{ approved: boolean }>('manager-approval');
if (!approval.approved) return 'rejected';
```

**Human-in-the-loop** — When a workflow pauses waiting for input, it can write a searchable, claimable row to the database. External systems — dashboards, AI agents, APIs — query by role and metadata, claim ownership, and resolve to resume the workflow. The pause lives in `public.hmsh_escalations` alongside the rest of your workflow state.

```typescript
// workflow: pause and write a claimable row
const decision = await Durable.workflow.condition<{ approved: boolean }>(
  'approval',
  { role: 'manager', type: 'order-review', metadata: { orderId } },
);

// elsewhere: find pending approvals, claim one, resolve it
const [pending] = await client.escalations.list({ role: 'manager', status: 'pending' });
await client.escalations.claim({ id: pending.id, assignee: 'alice@company.com' });
await client.escalations.resolve({ id: pending.id, resolverPayload: { approved: true } });
// the workflow resumes with { approved: true }
```

## Retries and error handling

Activities retry automatically on failure. Configure the policy per activity or per worker:

```typescript
// Durable: per-activity retry policy (activities registered at Worker.create)
const { reserveItem } = Durable.workflow.proxyActivities<typeof activities>({
  retry: {
    maximumAttempts: 5,
    backoffCoefficient: 2,
    maximumInterval: '60s'
  }
});
```

```typescript
// HotMesh: worker-level retry policy
const hotMesh = await HotMesh.init({
  appId: 'orders',
  engine: { connection },
  workers: [{
    topic: 'inventory.reserve',
    connection,
    retry: {
      maximumAttempts: 5,
      backoffCoefficient: 2,
      maximumInterval: '60s'
    },
    callback: async (data) => { /* ... */ }
  }]
});
```

Defaults: 3 attempts, coefficient 10, 120s cap. Delay formula: `min(coefficient ^ attempt, maximumInterval)`. Duration strings like `'5 seconds'`, `'2 minutes'`, and `'1 hour'` are supported.

If all retries are exhausted, the activity fails and the error propagates to the workflow function — handle it with a standard `try/catch`.

## Workflow state is queryable data

Workflow state lives in your database as ordinary rows — `jobs` and `jobs_attributes`. Query it directly, back it up with pg_dump, replicate it, join it against your application tables.

```sql
SELECT
  j.key          AS job_key,
  j.status       AS semaphore,
  j.entity       AS workflow,
  a.symbol       AS attribute,
  a.dimension    AS dimension,
  a.value        AS value,
  j.created_at,
  j.updated_at
FROM
  jobs j
  JOIN jobs_attributes a ON a.job_id = j.id
WHERE
  j.key = 'order-456'
ORDER BY
  a.symbol, a.dimension;
```

What happened? Consult the database. What's still running? Query the semaphore. What failed? Read the row. The execution state isn't reconstructed from a log — it was committed transactionally as each step ran.

```typescript
const handle = client.workflow.getHandle('orders', 'orderWorkflow', 'order-456');

const result = await handle.result();           // final output
const status = await handle.status();           // semaphore (0 = complete)
const state  = await handle.state(true);        // full state with metadata
const exported = await handle.export({          // selective export
  allow: ['data', 'state', 'status', 'timeline']
});
```

## Observability

There is no proprietary dashboard. Workflow state lives in Postgres, so use whatever tools you already have:

- **Direct SQL** — query `jobs` and `jobs_attributes` to inspect state, as shown above.
- **Handle API** — `handle.status()`, `handle.state(true)`, and `handle.export()` give programmatic access to any running or completed workflow.
- **Logging** — set `HMSH_LOGLEVEL` (`debug`, `info`, `warn`, `error`, `silent`) to control log verbosity.
- **OpenTelemetry** — set `HMSH_TELEMETRY=true` to emit spans and metrics. Plug in any OTel-compatible collector.

## YAML workflows

HotMesh also supports a declarative YAML syntax. The same activities run in both modes — the difference is compilation speed. YAML workflows compile ~10x faster because the execution graph is declared upfront rather than discovered through replay. The tradeoff is expressiveness: YAML uses a functional pipe syntax for conditions and transformations instead of native TypeScript control flow.

See the [Quick Start guide](https://github.com/hotmeshio/sdk-typescript/blob/main/docs/quickstart.md) for YAML examples and the `tests/functional/` directory for working implementations.

## Architecture

For a deep dive into the transactional execution model — how every step is crash-safe, how the monotonic collation ledger guarantees exactly-once delivery, and how cycles and retries remain correct under arbitrary failure — see the [Collation Design Document](https://github.com/hotmeshio/sdk-typescript/blob/main/services/collator/README.md). The symbolic system (how to design workflows) and lifecycle details (how to deploy workflows) are covered in the [Architectural Overview](https://zenodo.org/records/12168558).

## Running tests

Tests run inside Docker. Start the services and run the full suite:

```bash
docker compose up -d
docker compose exec hotmesh npm test
```

Run a specific test group:

```bash
docker compose exec hotmesh npm run test:durable         # all Durable tests
docker compose exec hotmesh npm run test:durable:hello   # single Durable test (hello world)
docker compose exec hotmesh npm run test:virtual         # all Virtual network function (VNF) tests
```

## License

HotMesh is source-available under the [HotMesh Source Available License](./LICENSE).
