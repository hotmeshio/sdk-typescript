# HotMesh

**🧠 Workflow That Remembers**

![beta release](https://img.shields.io/badge/release-beta-blue.svg)  ![made with typescript](https://img.shields.io/badge/built%20with-typescript-lightblue.svg)

HotMesh brings a **memory model** to your automation: durable entities that hold context, support concurrency, and evolve across runs. Built on PostgreSQL, it treats your database not just as storage—but as the runtime hub for agents, pipelines, and long-lived processes.

Use HotMesh to:

* **Store Evolving State** – Retain memory/state between executions
* **Coordinate Distributed Work** – Safely allow multiple workers to act on shared state
* **Track and Replay** – Full audit history and replay support by default

---

## Table of Contents

1. [Quick Start](#-quick-start)
2. [Permanent Memory Architecture](#-permanent-memory-architecture)
3. [Durable AI Agents](#-durable-ai-agents)
4. [Building Pipelines with State](#-building-pipelines-with-state)
5. [Documentation & Links](#-documentation--links)

---

## Quick Start

### Prerequisites

* PostgreSQL (or Supabase)
* Node.js 16+

### Install

```bash
npm install @hotmeshio/hotmesh
```

### Connect to a Database

HotMesh leverages Temporal.io's developer-friendly syntax for authoring workers, workflows, and clients. The `init` and `start` methods should look familiar.

```ts
import { MemFlow } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

async function main() {
  // MemFlow will auto-provision the database upon init
  const mf = await MemFlow.init({
    appId: 'my-app',
    engine: {
      connection: {
        class: Postgres,
        options: { connectionString: process.env.DATABASE_URL }
      }
    }
  });

  // Start a workflow with an assigned ID and arguments
  const handle = await mf.workflow.start({
    entity: 'research-agent',
    workflowName: 'researchAgent',
    workflowId: 'agent-session-jane-001',
    args: ['What are the long-term impacts of renewable energy subsidies?'],
    taskQueue: 'agents'
  });

  console.log('Result:', await handle.result());
}

main().catch(console.error);
```

### System Benefits

* **No Setup Required** – Tables and indexes are provisioned automatically
* **Shared State** – Every worker shares access to the same entity memory
* **Coordination by Design** – PostgreSQL handles consistency and isolation
* **Tenant Isolation** – Each app maintains its own schema
* **Scalable Defaults** – Partitioned tables and index support included

---

## Permanent Memory Architecture

Every workflow in HotMesh is backed by an "entity": a versioned, JSONB record that tracks its memory and state transitions.

* **Entities** – Represent long-lived state for a workflow or agent
* **Commands** – Modify state with methods like `set`, `merge`, `append`, `increment`
* **Consistency** – All updates are transactional with Postgres
* **Replay Safety** – Protects against duplicated side effects during re-execution
* **Partial Indexing** – Optimized querying of fields within large JSON structures

### Example: Partial Index for Premium Users

```sql
-- Index only those user entities that are marked as premium
CREATE INDEX idx_user_premium ON your_app.jobs (id)
WHERE entity = 'user' AND (context->>'isPremium')::boolean = true;
```

This index improves performance for filtered queries while reducing index size.

---

## Durable AI Agents

Agents often require memory—context that persists between invocations, spans multiple perspectives, or outlives a single process. HotMesh supports that natively.

The following example builds a "research agent" that runs sub-flows and self-reflects on the results.

### Research Agent Example

#### Main Coordinator Agent

```ts
export async function researchAgent(query: string): Promise<any> {
  const entity = await MemFlow.workflow.entity();

  // Set up shared memory for this agent session
  await entity.set({
    query,
    findings: [],
    perspectives: {},
    confidence: 0,
    status: 'researching'
  });

  // Launch perspective hooks in parallel (no need to await here)
  const optimistic = MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'optimisticPerspective',
    args: [query]
  });

  const skeptical = MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'skepticalPerspective',
    args: [query]
  });

  // Launch a child workflow with its own isolated entity/state
  const factChecker = await MemFlow.workflow.execChild({
    entity: 'fact-checker',
    workflowName: 'factCheckAgent',
    workflowId: `fact-check-${Date.now()}`,
    args: [query],
    taskQueue: 'agents'
  });

  // Wait for all views to complete before analyzing
  await Promise.all([optimistic, skeptical, factChecker]);

  // Final synthesis: aggregate and compare all perspectives
  await MemFlow.workflow.execHook({
    taskQueue: 'perspectives',
    workflowName: 'synthesizePerspectives',
    args: []
  });

  return await entity.get();
}
```

#### Hooks: Perspectives

```ts
// Optimistic hook looks for affirming evidence
export async function optimisticPerspective(query: string, config: {signal: string}): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  const findings = await searchForSupportingEvidence(query);
  await entity.merge({ perspectives: { optimistic: { findings, confidence: 0.8 }}});
  //signal the caller to notify all done
  await MemFlow.workflow.signal(config.signal, {});
}

// Skeptical hook seeks out contradictions and counterpoints
export async function skepticalPerspective(query: string, config: {signal: string}): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  const counterEvidence = await searchForCounterEvidence(query);
  await entity.merge({ perspectives: { skeptical: { counterEvidence, confidence: 0.6 }}});
  await MemFlow.workflow.signal(config.signal, {});
}
```

#### Child Agent: Fact Checker

```ts
// A dedicated child agent with its own entity type and context
export async function factCheckAgent(query: string): Promise<any> {
  const entity = await MemFlow.workflow.entity();
  await entity.set({ query, sources: [], verifications: [] });

  await MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'verifySourceCredibility',
    args: [query]
  });

  return await entity.get();
}
```

#### Synthesis

```ts
// Synthesis hook aggregates different viewpoints
export async function synthesizePerspectives(config: {signal: string}): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  const context = await entity.get();

  const result = await analyzePerspectives(context.perspectives);

  await entity.merge({
    perspectives: {
      synthesis: {
        finalAssessment: result,
        confidence: calculateConfidence(context.perspectives)
      }
    },
    status: 'completed'
  });
  await MemFlow.workflow.signal(config.signal, {});
}
```

---

## Building Pipelines with State

HotMesh treats pipelines as long-lived records, not ephemeral jobs. Every pipeline run is stateful, resumable, and traceable.

### Setup a Data Pipeline

```ts
export async function dataPipeline(source: string): Promise<void> {
  const entity = await MemFlow.workflow.entity();

  // Initial policy and tracking setup
  await entity.set({
    source,
    pipeline: { version: 1, policy: { refreshInterval: '24 hours' } },
    changeLog: []
  });

  // Trigger the recurring orchestration pipeline
  await MemFlow.workflow.execHook({
    taskQueue: 'pipeline',
    workflowName: 'runPipeline',
    args: [true]
  });
}
```

### Orchestration Hook

```ts
export async function runPipeline(repeat = false): Promise<void> {
  do {
    // Perform transformation step
    await MemFlow.workflow.execHook({
      taskQueue: 'transform',
      workflowName: 'cleanData',
      args: []
    });

    if (repeat) {
      // Schedule next execution
      await MemFlow.workflow.execHook({
        taskQueue: 'scheduler',
        workflowName: 'scheduleRefresh',
        args: []
      });
    }
  } while (repeat)
}

/**
 * Hook to clean and transform data
 */
export async function cleanData(signalInfo?: { signal: string }): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  
  // Simulate data cleaning
  await entity.merge({
    status: 'cleaning',
    lastCleanedAt: new Date().toISOString()
  });

  // Add to changelog
  await entity.append('changeLog', {
    action: 'clean',
    timestamp: new Date().toISOString()
  });

  // Signal completion if called via execHook
  if (signalInfo?.signal) {
    await MemFlow.workflow.signal(signalInfo.signal, { 
      status: 'cleaned',
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Hook to schedule the next refresh based on policy
 */
export async function scheduleRefresh(signalInfo?: { signal: string }): Promise<void> {
  const entity = await MemFlow.workflow.entity();
  
  // Get refresh interval from policy
  const currentEntity = await entity.get();
  const refreshInterval = currentEntity.pipeline.policy.refreshInterval;
  
  // Sleep for the configured interval
  await MemFlow.workflow.sleepFor(refreshInterval);
  
  // Update status after sleep
  await entity.merge({
    status: 'ready_for_refresh',
    nextRefreshAt: new Date().toISOString()
  });

  // Add to changelog
  await entity.append('changeLog', {
    action: 'schedule_refresh',
    timestamp: new Date().toISOString(),
    nextRefresh: new Date().toISOString()
  });

  // Signal completion if called via execHook
  if (signalInfo?.signal) {
    await MemFlow.workflow.signal(signalInfo.signal, {
      status: 'scheduled',
      nextRefresh: new Date().toISOString()
    });
  }
}
```

### Trigger from Outside

```ts
// External systems can trigger a single pipeline run
export async function triggerRefresh() {
  const client = new MemFlow.Client({/*...*/});

  await client.workflow.hook({
    workflowId: 'pipeline-123',
    taskQueue: 'pipeline',
    workflowName: 'runPipeline',
    args: []
  });
}
```

---

## Documentation & Links

* SDK Reference – [hotmeshio.github.io/sdk-typescript](https://hotmeshio.github.io/sdk-typescript)
* Examples – [github.com/hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript)

---

## License

Apache 2.0 – See `LICENSE` for details.
