# HotMesh MemFlow

**Permanent-Memory Workflows & AI Agents**

![beta release](https://img.shields.io/badge/release-beta-blue.svg)  ![made with typescript](https://img.shields.io/badge/built%20with-typescript-lightblue.svg)

MemFlow is a drop-in Temporal-style engine that runs natively on Postgres — but with a twist:
every workflow owns a *permanent*, JSON-backed context that lives beyond the main workflow.
Any number of *hooks* (lightweight, thread-safe workers) can attach to that record at any
time, read it, and safely write back incremental knowledge.
Think **durable execution** + **shared, evolving memory** → perfect for human-in-the-loop
processes and AI agents that learn over time.

---

## Table of Contents

1. 🚀 Quick Start
2. 🧠 How Permanent Memory Works
3. 🔌 Hooks & Context API
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
    workflowName: 'example',
    args: ['Jane'],
    taskQueue: 'contextual'
  });

  console.log('Result:', await handle.result());
}

main().catch(console.error);
```


## 🧠 How Permanent Memory Works

* **Context = JSONB row** in `<yourappname>.jobs` table
* **Atomic operations** (`set`, `merge`, `append`, `increment`, `toggle`, `delete`, …)
* **Transactional** – every update participates in the workflow/DB transaction
* **Time-travel-safe** – full replay compatibility; side-effect detector guarantees determinism
* **Hook-friendly** – any worker with the record ID can attach and mutate its slice of the JSON

* Context data is stored as JSONB; add partial indexes for improved query analysis.

**Example: Adding a Partial Index for Specific Entity Types**
```sql
-- Create a partial index for 'user' entities with specific context values
CREATE INDEX idx_user_premium ON your_app.jobs (id)
WHERE entity = 'user' AND (context->>'isPremium')::boolean = true;
```
This index will only be used for queries that match both conditions, making lookups for premium users much faster.

---

## 🔌 Hooks & Context API – Full Example

```typescript
import { MemFlow } from '@hotmeshio/hotmesh';

/* ------------ Main workflow ------------ */
export async function example(name: string): Promise<any> {
  //the context method provides transactional, replayable access to shared job state 
  const ctx = await MemFlow.workflow.context();

  //create the initial context (even arrays are supported)
  await ctx.set({
    user: { name },
    hooks: {},
    metrics: { count: 0 }
  });

  // Call two hooks in parallel to updaet the same shared context
  const [r1, r2] = await Promise.all([
    MemFlow.workflow.execHook({
      taskQueue: 'contextual',
      workflowName: 'hook1',
      args: [name, 'hook1'],
      signalId: 'hook1-complete',
    }),
    MemFlow.workflow.execHook({
      taskQueue: 'contextual',
      workflowName: 'hook2',
      args: [name, 'hook2'],
      signalId: 'hook2-complete',
    })
  ]);

  // merge here (or have the hooks merge in...everyone can access context)
  await ctx.merge({ hooks: { r1, r2 } });
  await ctx.increment('metrics.count', 2);

  return "The main has completed; the db record persists and can be hydrated; hook in from the outside!";
}

/* ------------ Hook 1 (hooks have access to methods like sleepFor) ------------ */
export async function hook1(name: string, kind: string): Promise<any> {
  await MemFlow.workflow.sleepFor('2 seconds');
  const res = { kind, processed: true, at: Date.now() };
  await MemFlow.workflow.signal('hook1-complete', res);
}

/* ------------ Hook 2 (hooks can access shared job context) ------------ */
export async function hook2(name: string, kind: string): Promise<void> {
  const ctx = await MemFlow.workflow.context();
  await ctx.merge({ user: { lastSeen: new Date().toISOString() } });
  await MemFlow.workflow.signal('hook2-complete', { ok: true });
}
```

**Highlights**

* Hook functions are replay-safe.
* Hook functions can safely read and write to the the *same* JSON context.
* All context operations (`set`, `merge`, `append`, etc.) execute transactionally.
* Context data is stored as JSONB; add partial indexes for improved query analysis.

---

## 🤖 Building Durable AI Agents

Permanent memory unlocks a straightforward pattern for agentic systems:

1. **Planner workflow** – sketches a task list, seeds context.
2. **Tool hooks** – execute individual tasks, feeding intermediate results back into context.
3. **Reflector hook** – periodically summarises context into long-term memory embeddings.
4. **Supervisor workflow** – monitors metrics stored in context and decides when to finish.

Because every step is durable *and* shares the same knowledge object, agents can pause,
restart, scale horizontally, and keep evolving their world-model indefinitely.

---

## 📚 Documentation & Links

* SDK API – [https://hotmeshio.github.io/sdk-typescript](https://hotmeshio.github.io/sdk-typescript)
* Examples – [https://github.com/hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript)

---

## License

Apache 2.0 – see `LICENSE` for details.
