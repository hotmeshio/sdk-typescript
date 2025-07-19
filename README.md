# HotMesh

**Durable Memory + Coordinated Execution**

![beta release](https://img.shields.io/badge/release-beta-blue.svg) ![made with typescript](https://img.shields.io/badge/built%20with-typescript-lightblue.svg)

HotMesh removes the repetitive glue of building durable agents, pipelines, and long‑running workflows. Instead of you designing queues, schedulers, cache layers, and ad‑hoc state stores for each agent or pipeline, HotMesh standardizes the pattern:

* **Entity (Core Memory)**: The authoritative JSONB document + its indexable “surface” fields.
* **Hooks (Durable Units of Work)**: Re‑entrant, idempotent functions that *maintain* the entity over time.
* **Workflow (Coordinator)**: The thin orchestration entry that seeds state, spawns hooks, and optionally synthesizes results.
* **Commands (State Mutation API)**: Atomic `set / merge / append / increment / tag / signal` updates with optimistic invariants handled by Postgres transactions.

You focus on *what should change in memory*; HotMesh handles *how it changes safely and durably.*

---

## Why It’s Easier with HotMesh

| Problem You Usually Solve Manually                  | HotMesh Built‑In                          | Impact                                         |
| --------------------------------------------------- | ----------------------------------------- | ---------------------------------------------- |
| Designing per‑agent persistence and caches          | Unified JSONB entity + typed accessors    | One memory model across agents/pipelines       |
| Preventing race conditions on shared state          | Transactional hook writes                 | Safe parallel maintenance                      |
| Coordinating multi-perspective / multi-step work    | Hook spawning + signals                   | Decomposed work without orchestration glue     |
| Schema evolution / optional fields                  | Flexible JSONB + selective indexes        | Add / adapt state incrementally                |
| Querying live pipeline / agent status               | SQL over materialized surfaces            | Operational observability using standard tools |
| Avoiding duplicate side-effects during retry/replay | Deterministic re‑entry + idempotent hooks | Simplifies error handling                      |
| Per‑tenant isolation                                | Schema (or prefix) scoping                | Clean multi‑tenant boundary                    |
| Background progression / fan‑out                    | `execHook` + signals                      | Natural concurrency without queue plumbing     |

---

## Core Abstractions

### 1. Entities

Durable JSONB documents representing *process memory*. Each entity:

* Has a stable identity (`workflowId` / logical key).
* Evolves via atomic commands.
* Is versioned implicitly by transactional history.
* Can be partially indexed for targeted query performance.

> **Design Note:** Treat entity shape as *contractual surface* + *freeform interior*. Index only the minimal surface required for lookups or dashboards.

### 2. Hooks

Re‑entrant, idempotent, interruptible units of work that *maintain* an entity. Hooks can:

* Start, stop, or be re‑invoked without corrupting state.
* Run concurrently (Postgres ensures isolation on write).
* Emit signals to let coordinators or sibling hooks know a perspective / phase completed.

### 3. Workflow Coordinators

Thin entrypoints that:

* Seed initial entity state.
* Fan out perspective / phase hooks.
* Optionally synthesize or finalize.
* Return a snapshot (often the final entity state) — *the workflow result is just memory*.

### 4. Commands (Entity Mutation Primitives)

| Command     | Purpose                                   | Example                                          |
| ----------- | ----------------------------------------- | ------------------------------------------------ |
| `set`       | Replace full value (first write or reset) | `await e.set({ user: { id: 123, name: "John" } })` |
| `merge`     | Deep JSON merge                           | `await e.merge({ user: { email: "john@example.com" } })` |
| `append`    | Append to an array field                  | `await e.append('items', { id: 1, name: "New Item" })` |
| `prepend`   | Add to start of array field              | `await e.prepend('items', { id: 0, name: "First Item" })` |
| `remove`    | Remove item from array by index          | `await e.remove('items', 0)` |
| `increment` | Numeric counters / progress               | `await e.increment('counter', 5)` |
| `toggle`    | Toggle boolean value                      | `await e.toggle('settings.enabled')` |
| `setIfNotExists` | Set value only if path doesn't exist | `await e.setIfNotExists('user.id', 123)` |
| `delete`    | Remove field at specified path           | `await e.delete('user.email')` |
| `get`       | Read value at path (or full entity)      | `await e.get('user.email')` |
| `signal`    | Mark hook milestone / unlock waiters      | `await MemFlow.workflow.signal('phase-x', data)` |

The Entity module also provides static methods for cross-entity querying:

```typescript
// Find entities matching conditions
const activeUsers = await Entity.find('user', { 
  status: 'active',
  country: 'US' 
});

// Find by specific field condition
const highValueOrders = await Entity.findByCondition(
  'order',
  'total_amount',
  1000,
  '>=',
  hotMeshClient
);

// Find single entity by ID
const user = await Entity.findById('user', 'user123', hotMeshClient);

// Create optimized index for queries
await Entity.createIndex('user', 'email', hotMeshClient);
```

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Memory Architecture](#memory-architecture)
3. [Durable AI Agents](#durable-ai-agents)
4. [Stateful Pipelines](#stateful-pipelines)
5. [Indexing Strategy](#indexing-strategy)
6. [Operational Notes](#operational-notes)
7. [Documentation & Links](#documentation--links)

---

## Quick Start

### Install

```bash
npm install @hotmeshio/hotmesh
```

### Minimal Setup
```ts
import { MemFlow } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

async function main() {
  // Auto-provisions required tables/index scaffolding on first run
  const mf = await MemFlow.init({
    appId: 'my-app',
    engine: {
      connection: {
        class: Postgres,
        options: { connectionString: process.env.DATABASE_URL }
      }
    }
  });

  // Start a durable research agent (entity-backed workflow)
  const handle = await mf.workflow.start({
    entity: 'research-agent',
    workflowName: 'researchAgent',
    workflowId: 'agent-session-jane-001',
    args: ['Long-term impacts of renewable energy subsidies'],
    taskQueue: 'agents'
  });

  console.log('Final Memory Snapshot:', await handle.result());
}

main().catch(console.error);
```

### Value Checklist (What You Did *Not* Have To Do)
- Create tables / migrations
- Define per-agent caches
- Implement optimistic locking
- Build a queue fan‑out mechanism
- Hand-roll replay protection

---

## Memory Architecture
Each workflow = **1 durable entity**. Hooks are stateless functions *shaped by* that entity's evolving JSON. You can inspect or modify it at any time using ordinary SQL or the provided API.

### Programmatic Indexing
```ts
// Create index for premium research agents
await MemFlow.Entity.createIndex('research-agent', 'isPremium', hotMeshClient);

// Find premium agents needing verification
const agents = await MemFlow.Entity.find('research-agent', {
  isPremium: true,
  needsVerification: true
}, hotMeshClient);
```

### Direct SQL Access
```sql
-- Same index via SQL (more control over index type/conditions)
CREATE INDEX idx_research_agents_premium ON my_app.jobs (id)
WHERE entity = 'research-agent' AND (context->>'isPremium')::boolean = true;

-- Ad hoc query example
SELECT id, context->>'status' as status, context->>'confidence' as confidence
FROM my_app.jobs
WHERE entity = 'research-agent'
  AND (context->>'isPremium')::boolean = true
  AND (context->>'confidence')::numeric > 0.8;
```

**Guidelines:**
1. *Model intent, not mechanics.* Keep ephemeral calculation artifacts minimal; store derived values only if reused.
2. *Index sparingly.* Each index is a write amplification cost. Start with 1–2 selective partial indexes.
3. *Keep arrays append‑only where possible.* Supports audit and replay semantics cheaply.
4. *Choose your tool:* Use Entity methods for standard queries, raw SQL for complex analytics or custom indexes.

---

## Durable AI Agents
Agents become simpler: the *agent* is the memory record; hooks supply perspectives, verification, enrichment, or lifecycle progression.

### Coordinator (Research Agent)
```ts
export async function researchAgent(query: string) {
  const entity = await MemFlow.workflow.entity();

  const initial = {
    query,
    findings: [],
    perspectives: {},
    confidence: 0,
    verification: {},
    status: 'researching',
    startTime: new Date().toISOString()
  };
  await entity.set<typeof initial>(initial);

  // Fan-out perspectives
  await MemFlow.workflow.execHook({ taskQueue: 'agents', workflowName: 'optimisticPerspective', args: [query], signalId: 'optimistic-complete' });
  await MemFlow.workflow.execHook({ taskQueue: 'agents', workflowName: 'skepticalPerspective', args: [query], signalId: 'skeptical-complete' });
  await MemFlow.workflow.execHook({ taskQueue: 'agents', workflowName: 'verificationHook', args: [query], signalId: 'verification-complete' });
  await MemFlow.workflow.execHook({ taskQueue: 'agents', workflowName: 'synthesizePerspectives', args: [], signalId: 'synthesis-complete' });

  return await entity.get();
}
```

### Synthesis Hook
```ts
export async function synthesizePerspectives({ signal }: { signal: string }) {
  const e = await MemFlow.workflow.entity();
  const ctx = await e.get();

  const synthesized = await analyzePerspectives(ctx.perspectives);
  await e.merge({
    perspectives: {
      synthesis: {
        finalAssessment: synthesized,
        confidence: calculateConfidence(ctx.perspectives)
      }
    },
    status: 'completed'
  });
  await MemFlow.workflow.signal(signal, {});
}
```

> **Pattern:** Fan-out hooks that write *adjacent* subtrees (e.g., `perspectives.optimistic`, `perspectives.skeptical`). A final hook merges a compact synthesis object. Avoid cross-hook mutation of the same nested branch.

---

## Stateful Pipelines
Pipelines are identical in structure to agents: a coordinator seeds memory; phase hooks advance state; the entity is the audit trail.

### Document Processing Pipeline (Coordinator)
```ts
export async function documentProcessingPipeline() {
  const pipeline = await MemFlow.workflow.entity();

  const initial = {
    documentId: `doc-${Date.now()}`,
    status: 'started',
    startTime: new Date().toISOString(),
    imageRefs: [],
    extractedInfo: [],
    validationResults: [],
    finalResult: null,
    processingSteps: [],
    errors: [],
    pageSignals: {}
  };
  await pipeline.set<typeof initial>(initial);

  await pipeline.merge({ status: 'loading-images' });
  await pipeline.append('processingSteps', 'image-load-started');
  const imageRefs = await activities.loadImagePages();
  if (!imageRefs?.length) throw new Error('No image references found');
  await pipeline.merge({ imageRefs });
  await pipeline.append('processingSteps', 'image-load-completed');

  // Page hooks
  for (const [i, ref] of imageRefs.entries()) {
    const page = i + 1;
    await MemFlow.workflow.execHook({
      taskQueue: 'pipeline',
      workflowName: 'pageProcessingHook',
      args: [ref, page, initial.documentId],
      signalId: `page-${page}-complete`
    });
  }

  // Validation
  await MemFlow.workflow.execHook({ taskQueue: 'pipeline', workflowName: 'validationHook', args: [initial.documentId], signalId: 'validation-complete' });
  // Approval
  await MemFlow.workflow.execHook({ taskQueue: 'pipeline', workflowName: 'approvalHook', args: [initial.documentId], signalId: 'approval-complete' });
  // Notification
  await MemFlow.workflow.execHook({ taskQueue: 'pipeline', workflowName: 'notificationHook', args: [initial.documentId], signalId: 'processing-complete' });

  await pipeline.merge({ status: 'completed', completedAt: new Date().toISOString() });
  await pipeline.append('processingSteps', 'pipeline-completed');
  return await pipeline.get();
}
```

**Operational Characteristics:**
- *Replay Friendly*: Each hook can be retried; pipeline memory records invariant progress markers (`processingSteps`).
- *Parallelizable*: Pages fan out naturally without manual queue wiring.
- *Auditable*: Entire lifecycle captured in a single evolving JSON record.

---

## Documentation & Links
* **SDK Reference** – https://hotmeshio.github.io/sdk-typescript
* **Agent Example Tests** – https://github.com/hotmeshio/sdk-typescript/tree/main/tests/memflow/agent
* **Pipeline Example Tests** – https://github.com/hotmeshio/sdk-typescript/tree/main/tests/memflow/pipeline
* **Sample Projects** – https://github.com/hotmeshio/samples-typescript

---

## License
Apache 2.0 with commercial restrictions* – see `LICENSE`.
>*NOTE: It's open source with one commercial exception: Build, sell, and share solutions made with HotMesh. But don't white-label the orchestration core and repackage it as your own workflow-as-a-service.
