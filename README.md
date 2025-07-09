# HotMesh

**Permanent-Memory Workflows & AI Agents**

![beta release](https://img.shields.io/badge/release-beta-blue.svg)  ![made with typescript](https://img.shields.io/badge/built%20with-typescript-lightblue.svg)

**HotMesh** is a Temporal-style workflow engine that runs natively on PostgreSQL — with a powerful twist: every workflow maintains permanent, state that persists independently of the workflow itself.

This means:

* Any number of lightweight, thread-safe **hook workers** can attach to the same workflow record at any time.
* These hooks can safely **read and write** to shared state.
* The result is a **durable execution model** with **evolving memory**, ideal for **human-in-the-loop processes** and **AI agents that learn over time**.

---

## Table of Contents

1. 🚀 Quick Start
2. 🧠 How Permanent Memory Works
3. 🔌 Hooks & Entity API
4. 🤖 Building Durable AI Agents
5. 🔬 Advanced Patterns & Recipes
6. 📚 Documentation & Links

---

## 🚀 Quick Start

### Install
```bash
npm install @hotmeshio/hotmesh
```

### Start a workflow
```typescript
// index.ts
import { MemFlow } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

async function main() {
  const mf = await MemFlow.init({
    appId: 'my-app',
    engine: {
      connection: {
        class: Postgres,
        options: { connectionString: process.env.DATABASE_URL }
      }
    }
  });

  // Kick off a workflow
  const handle = await mf.workflow.start({
    entity: 'user',
    workflowName: 'userExample',
    workflowId: 'jane@hotmesh.com',
    args: ['Jane'],
    taskQueue: 'entityqueue'
  });

  console.log('Result:', await handle.result());
}

main().catch(console.error);
```

---

## 🧠 How Permanent Memory Works

* **Entity = persistent JSON record** – each workflow's memory is stored as a JSONB row in your Postgres database
* **Atomic operations** (`set`, `merge`, `append`, `increment`, `toggle`, `delete`, …)
* **Transactional** – every update participates in the workflow/DB transaction
* **Time-travel-safe** – full replay compatibility; side-effect detector guarantees determinism
* **Hook-friendly** – any worker with the record ID can attach and mutate its slice of the JSON
* **Index-friendly** - entity data is stored as JSONB; add partial indexes for improved query analysis.

**Example: Adding a Partial Index for Specific Entity Types**
```sql
-- Create a partial index for 'user' entities with specific entity values
CREATE INDEX idx_user_premium ON your_app.jobs (id)
WHERE entity = 'user' AND (context->>'isPremium')::boolean = true;
```
This index will only be used for queries that match both conditions, making lookups for premium users much faster.

---

## 🔌 Hooks & Entity API – Full Example

HotMesh hooks are powerful because they can be called both internally (from within a workflow) and externally (from outside, even after the workflow completes). This means you can:

* Start a workflow that sets up initial state
* Have the workflow call some hooks internally
* Let the workflow complete
* Continue to update the workflow's entity state from the outside via hooks
* Build long-running processes that evolve over time

Here's a complete example showing both internal and external hook usage:

```typescript
import { MemFlow } from '@hotmeshio/hotmesh';

/* ------------ Main workflow ------------ */
export async function userExample(name: string): Promise<any> {
  //the entity method provides transactional, replayable access to shared job state 
  const entity = await MemFlow.workflow.entity();

  //create the initial entity (even arrays are supported)
  await entity.set({
    user: { name },
    hooks: {},
    metrics: { count: 0 }
  });

  // Call one hook internally
  const result1 = await MemFlow.workflow.execHook({
    taskQueue: 'entityqueue',
    workflowName: 'hook1',
    args: [name, 'hook1'],
    signalId: 'hook1-complete'
  });

  // merge the result
  await entity.merge({ hooks: { r1: result1 } });
  await entity.increment('metrics.count', 1);

  return "The main has completed; the db record persists and can be hydrated; hook in from the outside!";
}

/* ------------ Hook 1 (hooks have access to methods like sleepFor) ------------ */
export async function hook1(name: string, kind: string): Promise<any> {
  await MemFlow.workflow.sleepFor('2 seconds');
  const res = { kind, processed: true, at: Date.now() };
  await MemFlow.workflow.signal('hook1-complete', res);
}

/* ------------ Hook 2 (hooks can access shared job entity) ------------ */
export async function hook2(name: string, kind: string): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  await entity.merge({ user: { lastSeen: new Date().toISOString() } });
  await MemFlow.workflow.signal('hook2-complete', { ok: true });
}

/* ------------ Worker/Hook Registration ------------ */
async function startWorker() {
  const mf = await MemFlow.init({
    appId: 'my-app',
    engine: {
      connection: {
        class: Postgres,
        options: { connectionString: process.env.DATABASE_URL }
      }
    }
  });

  const worker = await mf.worker.create({
    taskQueue: 'entityqueue',
    workflow: example
  });

  await mf.worker.create({
    taskQueue: 'entityqueue',
    workflow: hook1
  });

  await mf.worker.create({
    taskQueue: 'entityqueue',
    workflow: hook2
  });

  console.log('Workers and hooks started and listening...');
}
```

### The Power of External Hooks

One of HotMesh's most powerful features is that workflow entities remain accessible even after the main workflow completes. By providing the original workflow ID, any authorized client can:

* Hook into existing workflow entities
* Update state and trigger new processing
* Build evolving, long-running processes
* Enable human-in-the-loop workflows
* Create AI agents that learn over time

Here's how to hook into an existing workflow from the outside:

```typescript
/* ------------ External Hook Example ------------ */
async function externalHookExample() {
  const client = new MemFlow.Client({
    appId: 'my-app',
    engine: {
      connection: {
        class: Postgres,
        options: { connectionString: process.env.DATABASE_URL }
      }
    }
  });

  // Start hook2 externally by providing the original workflow ID
  await client.workflow.hook({
    workflowId: 'jane@hotmesh.com', //id of the target workflow
    taskQueue: 'entityqueue',
    workflowName: 'hook2',
    args: [name, 'external-hook']
  });
}
```

---

## 🤖 Building Durable AI Agents

Permanent memory unlocks a straightforward pattern for agentic systems:

1. **Planner workflow** – sketches a task list, seeds entity state.
2. **Tool hooks** – execute individual tasks, feeding intermediate results back into state.
3. **Reflector hook** – periodically summarizes state into long-term memory embeddings.
4. **Supervisor workflow** – monitors metrics stored in state and decides when to finish.

Because every step is durable *and* shares the same knowledge object, agents can pause,
restart, scale horizontally, and keep evolving their world-model indefinitely.

---

## 📚 Documentation & Links

* SDK API – [https://hotmeshio.github.io/sdk-typescript](https://hotmeshio.github.io/sdk-typescript)
* Examples – [https://github.com/hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript)

---

## License

Apache 2.0 – see `LICENSE` for details.
