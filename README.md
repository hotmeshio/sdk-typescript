# HotMesh

**Workflow That Remembers**

![beta release](https://img.shields.io/badge/release-beta-blue.svg)  ![made with typescript](https://img.shields.io/badge/built%20with-typescript-lightblue.svg)

HotMesh brings a **memory model** to durable functions. Built on PostgreSQL, it treats your database as the runtime hub for agents, pipelines, and long-lived processes.

Use HotMesh to:

* **Store Evolving State** – Retain memory/state between executions
* **Coordinate Distributed Work** – Safely allow multiple workers to act on shared state
* **Track and Replay** – Full audit history and replay support by default

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Permanent Memory Architecture](#permanent-memory-architecture)
3. [Durable AI Agents](#durable-ai-agents)
4. [Building Pipelines with State](#building-pipelines-with-state)
5. [Documentation & Links](#documentation--links)

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

Agents often require memory—context that persists between invocations, spans multiple perspectives, or outlives a single process.

The following example builds a "research agent" that executes hooks with different perspectives and then synthesizes. The data-first approach sets up initial state and then uses temporary hook functions to augment over the lifecycle of the entity record.

### Research Agent Example

#### Main Coordinator Agent

```ts
export async function researchAgent(query: string): Promise<any> {
  const agent = await MemFlow.workflow.entity();

  // Set up shared memory for this agent session
  const initialState = {
    query,
    findings: [],
    perspectives: {},
    confidence: 0,
    verification: {},
    status: 'researching',
    startTime: new Date().toISOString(),
  }
  await agent.set<typeof initialState>(initialState);

  // Launch perspective hooks
  await MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'optimisticPerspective',
    args: [query],
    signalId: 'optimistic-complete'
  });

  await MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'skepticalPerspective',
    args: [query],
    signalId: 'skeptical-complete'
  });

  await MemFlow.workflow.execHook({
    taskQueue: 'agents',
    workflowName: 'verificationHook',
    args: [query],
    signalId: 'verification-complete'
  });

  await MemFlow.workflow.execHook({
    taskQueue: 'perspectives',
    workflowName: 'synthesizePerspectives',
    args: [],
    signalId: 'synthesis-complete',
  });

  // return analysis, verification, and synthesis
  return await agent.get();
}
```


Let's look at one of these hooks in detail - the synthesis hook that combines all perspectives into a final assessment:

#### Synthesis Hook

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

//other hooks...
```

> 💡 A complete implementation of this Research Agent example with tests, OpenAI integration, and multi-perspective analysis can be found in the [agent test suite](https://github.com/hotmeshio/sdk-typescript/tree/main/tests/memflow/agent).

---

## Building Pipelines with State

HotMesh treats pipelines as long-lived records. Every pipeline run is stateful, resumable, and traceable. Hooks can be re-run at any time, and can be invoked by external callers. Sleep and run on a cadence to keep the pipeline up to date.

### Setup a Data Pipeline

```ts
export async function documentProcessingPipeline(): Promise<any> {
  const pipeline = await MemFlow.workflow.entity();

  // Initialize pipeline state with empty arrays
  const initialState = {
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
  
  await pipeline.set<typeof initialState>(initialState);

  // Step 1: Get list of image file references
  await pipeline.merge({status: 'loading-images'});
  await pipeline.append('processingSteps', 'image-load-started');
  const imageRefs = await activities.loadImagePages();
  if (!imageRefs || imageRefs.length === 0) {
    throw new Error('No image references found');
  }
  await pipeline.merge({imageRefs});
  await pipeline.append('processingSteps', 'image-load-completed');

  // Step 2: Launch processing hooks for each page
  for (const [index, imageRef] of imageRefs.entries()) {
    const pageNumber = index + 1;

    await MemFlow.workflow.execHook({
      taskQueue: 'pipeline',
      workflowName: 'pageProcessingHook',
      args: [imageRef, pageNumber, initialState.documentId],
      signalId: `page-${pageNumber}-complete`
    });
  };

  // Step 3: Launch validation hook
  await MemFlow.workflow.execHook({
    taskQueue: 'pipeline',
    workflowName: 'validationHook',
    args: [initialState.documentId],
    signalId: 'validation-complete'
  });

  // Step 4: Launch approval hook
  await MemFlow.workflow.execHook({
    taskQueue: 'pipeline',
    workflowName: 'approvalHook',
    args: [initialState.documentId],
    signalId: 'approval-complete',
  });

  // Step 5: Launch notification hook
  await MemFlow.workflow.execHook({
    taskQueue: 'pipeline',
    workflowName: 'notificationHook',
    args: [initialState.documentId],
    signalId: 'processing-complete',
  });

  // Step 6: Return final state
  await pipeline.merge({status: 'completed', completedAt: new Date().toISOString()});
  await pipeline.append('processingSteps', 'pipeline-completed');
  return await pipeline.get();
}
```

> 💡 A complete implementation of this Pipeline example with OpenAI Vision integration, processing hooks, and document workflow automation can be found in the [pipeline test suite](https://github.com/hotmeshio/sdk-typescript/tree/main/tests/memflow/pipeline).

---

## Documentation & Links

* SDK Reference – [hotmeshio.github.io/sdk-typescript](https://hotmeshio.github.io/sdk-typescript)
* Examples – [github.com/hotmeshio/samples-typescript](https://github.com/hotmeshio/samples-typescript)

---

## License

Apache 2.0 with commercial restrictions – See `LICENSE` for details.
