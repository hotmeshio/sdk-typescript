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
- **Human-in-the-loop queues** — Suspend workflows until an operator claims and resolves the task. Pending suspensions become discoverable, claimable rows in Postgres. Concurrent workers receive exact reason codes (`conflict`, `already-resolved`, `signal-failed`) instead of opaque nulls.

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
// Basic: pause until any caller sends the matching signal
const approval = await Durable.workflow.condition<{ approved: boolean }>('manager-approval');
if (!approval.approved) return 'rejected';
```

Pass a `ConditionQueueConfig` as the second argument to also create a claimable task record in Postgres (see [Signal queue](#signal-queue-postgres-only) below):

```typescript
// With queue config: atomically suspend the workflow AND enqueue a claimable task record
const approval = await Durable.workflow.condition<{ approved: boolean }>('manager-approval', {
  role: 'manager',
  description: `Approve order ${orderId}`,
  metadata: { orderId, region: 'us-west' }, // GIN-indexed; used for claimByMetadata queries
  envelope: { formSchema: [...] },           // unindexed display context; safe for large blobs
});
// approval is false if a timeout was set and no signal arrived
if (approval === false) return 'timed-out';
if (!approval.approved) return 'rejected';
```

The optional first string argument is a timeout; the queue config can appear as the second or third argument:

```typescript
// With timeout AND queue config
const approval = await Durable.workflow.condition<{ approved: boolean }>(
  'manager-approval',
  '72 hours',                  // workflow times out if nobody acts
  { role: 'manager', metadata: { orderId } },
);
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

## Signal queue (Postgres only)

When `condition()` is called with a `ConditionQueueConfig`, HotMesh atomically writes a row to `hotmesh_signals` inside the same Postgres transaction that suspends the workflow. The result is a durable, queryable task queue backed by the same database — no separate service, no double-write, no gap between "task created" and "workflow suspended".

**Workflow side** — declare what kind of task this suspension represents:

```typescript
// workflows/approval.ts
export async function orderApproval(orderId: string) {
  const signalId = `approve-${orderId}`;

  const result = await Durable.workflow.condition<{ approved: boolean; notes: string }>(
    signalId,
    {
      role: 'pharmacist',
      type: 'approval',
      priority: 3,
      description: `Review order ${orderId}`,
      taskQueue: 'rx-approvals',
      workflowType: 'orderApproval',
      metadata: { orderId },               // GIN-indexed; query with claimByMetadata
      envelope: {                          // not indexed; pass form schemas, display context
        formSchema: [{ name: 'notes', type: 'textarea', required: true }],
      },
    },
  );

  if (result === false) return { status: 'timed-out' };
  return result;
}
```

**Resolver side** — claim and resolve from any process (API handler, worker, dashboard):

```typescript
// resolver.ts
import { Durable } from '@hotmeshio/hotmesh';

const client = new Durable.Client({ connection });

// List all pending tasks for a role
const pending = await client.signalQueue.list({ role: 'pharmacist', status: 'pending' });

// Claim by metadata — atomic; concurrent workers get 'conflict', not a silent null
const claim = await client.signalQueue.claimByMetadata({
  key: 'orderId',
  value: orderId,
  assignee: 'jane@example.com',
  durationMinutes: 30,
});

if (!claim.ok) {
  // claim.reason: 'not-found' (nothing queued) | 'conflict' (another worker beat you)
  return;
}

// Resolve — marks the DB record resolved AND delivers the signal to the paused workflow
const resolution = await client.signalQueue.resolve({
  id: claim.entry.id,
  resolverPayload: { approved: true, notes: 'LGTM' },
});

if (!resolution.ok) {
  if (resolution.reason === 'signal-failed') {
    // DB updated but workflow signal not delivered — retry with resolution.signalKey
  }
}
```

**All `signalQueue` methods:**

| Method | Description |
|---|---|
| `list(params)` | List signals filtered by `role`, `status`, `taskQueue` |
| `get(id)` | Fetch a single signal record by UUID |
| `claim({ id, assignee?, durationMinutes? })` | Claim by record ID |
| `claimByMetadata({ key, value, assignee?, durationMinutes? })` | Claim by GIN-indexed metadata field |
| `release({ id })` | Return a claimed signal to `pending` |
| `releaseExpired()` | Sweep all past-expiry claims back to `pending` |
| `resolve({ id, resolverPayload? })` | Resolve by ID and deliver signal to the workflow |
| `resolveByMetadata({ key, value, resolverPayload? })` | Resolve by metadata field and deliver signal |

**Result types** — all mutating methods return a discriminated union so callers can handle every outcome without guessing what `null` meant:

```typescript
// Claim
type ClaimSignalResult =
  | { ok: true; entry: SignalQueueEntry }
  | { ok: false; reason: 'not-found' | 'conflict' };

// Release
type ReleaseSignalResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'wrong-status' };

// Resolve
type ResolveSignalResult =
  | { ok: true }
  | { ok: false; reason: 'not-found' | 'already-resolved' }
  | { ok: false; reason: 'signal-failed'; signalKey: string };
```

`signal-failed` means the database record was updated but `workflow.signal()` threw (network blip, workflow already complete). The `signalKey` is returned so callers can retry signal delivery independently without re-resolving the record.

> **Postgres only.** The `signalQueue.*` methods require the Postgres provider. Workflows using `condition()` without a queue config are unaffected on any provider.

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
