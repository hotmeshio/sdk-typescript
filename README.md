# HotMesh

**Integrate AI automation into your current stack — without breaking it**

![beta release](https://img.shields.io/badge/release-beta-blue.svg)

HotMesh modernizes existing business systems by introducing a durable workflow layer that connects AI, automation, and human-in-the-loop steps — **without replacing your current stack**.
Each process runs with persistent memory in Postgres, surviving retries, crashes, and human delays.

```bash
npm install @hotmeshio/hotmesh
```

---

## What It Solves

Modernization often stalls where systems meet people and AI.
HotMesh builds a **durable execution bridge** across those seams — linking your database, APIs, RPA, and AI agents into one recoverable process.

* **AI that can fail safely** — retries, resumable state, and confidence tracking
* **Human steps that don’t block** — pause for days, resume instantly
* **Legacy systems that stay connected** — SQL and RPA coexist seamlessly
* **Full visibility** — query workflows and outcomes directly in SQL

---

## Core Model

### Entity — the Business Process Record

Every workflow writes to a durable JSON document in Postgres called an **Entity**.
It becomes the shared memory between APIs, RPA jobs, LLM agents, and human operators.

```ts
const e = await MemFlow.workflow.entity();

// initialize from a source event
await e.set({
  caseId: "A42",
  stage: "verification",
  retries: 0,
  notes: []
});

// AI step adds structured output
await e.merge({
  aiSummary: { result: "Verified coverage", confidence: 0.93 },
  stage: "approval",
});

// human operator review
await e.append("notes", { reviewer: "ops1", comment: "ok to proceed" });

// maintain counters
await e.increment("retries", 1);

// retrieve current process state
const data = await e.get();
```

**Minimal surface contract**

| Command       | Purpose                            |
| ------------- | ---------------------------------- |
| `set()`       | Initialize workflow state          |
| `merge()`     | Update any JSON path               |
| `append()`    | Add entries to lists (logs, notes) |
| `increment()` | Maintain counters or metrics       |
| `get()`       | Retrieve current state             |

Entities are stored in plain SQL tables, directly queryable:

```sql
SELECT id, context->>'stage', context->'aiSummary'->>'result'
FROM my_app.jobs
WHERE entity = 'claims-review'
  AND context->>'stage' != 'complete';
```

---

### Hook — Parallel Work Units

Hooks are stateless functions that operate on the shared Entity.
Each hook executes independently (API, RPA, or AI), retrying automatically until success.

```ts
await MemFlow.workflow.execHook({
  workflowName: "verifyCoverage",
  args: ["A42"]
});
```

To run independent work in parallel, use a **batch execution** pattern:

```ts
// Run independent research perspectives in parallel using batch execution
await MemFlow.workflow.execHookBatch([
  {
    key: 'optimistic',
    options: {
      taskQueue: 'agents',
      workflowName: 'optimisticPerspective',
      args: [query],
      signalId: 'optimistic-complete'
    }
  },
  {
    key: 'skeptical',
    options: {
      taskQueue: 'agents',
      workflowName: 'skepticalPerspective',
      args: [query],
      signalId: 'skeptical-complete'
    }
  }
]);
```

Each hook runs in its own recoverable context, allowing AI, API, and RPA agents to operate independently while writing to the same durable Entity.

---

## Example — AI-Assisted Claims Review

```ts
export async function claimsWorkflow(caseId: string) {
  const e = await MemFlow.workflow.entity();
  await e.set({ caseId, stage: "intake", approved: false });

  // Run verification and summarization in parallel
  await MemFlow.workflow.execHookBatch([
    {
      key: 'verifyCoverage',
      options: {
        taskQueue: 'agents',
        workflowName: 'verifyCoverage',
        args: [caseId],
        signalId: 'verify-complete'
      }
    },
    {
      key: 'generateSummary',
      options: {
        taskQueue: 'agents',
        workflowName: 'generateSummary',
        args: [caseId],
        signalId: 'summary-complete'
      }
    }
  ]);

  // Wait for human sign-off
  const approval = await MemFlow.workflow.waitFor("human-approval");
  await e.merge({ approved: approval === true, stage: "complete" });

  return await e.get();
}
```

This bridges:

* an existing insurance or EHR system (status + audit trail)
* LLM agents for data validation and summarization
* a human reviewer for final sign-off

—all within one recoverable workflow record.

---

## Why It Fits Integration Work

HotMesh is purpose-built for **incremental modernization**.

| Need                          | What HotMesh Provides                    |
| ----------------------------- | ---------------------------------------- |
| Tie AI into legacy apps       | Durable SQL bridge with full visibility  |
| Keep human review steps       | Wait-for-signal workflows                |
| Handle unstable APIs          | Built-in retries and exponential backoff |
| Trace process across systems  | Unified JSON entity per workflow         |
| Store long-running AI results | Durable state for agents and automations |

---

## License

Apache 2.0 — free to build, integrate, and deploy.
Do not resell the core engine as a hosted service.
