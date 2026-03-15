# Exporter Input Enrichment

HotMesh's native exporter now supports **input enrichment** and **direct database queries** for expired jobs, eliminating the need for custom export layers.

## Overview

The exporter provides two binding modes:

### Late-binding (default)
- Returns timeline_key references in events
- Minimal I/O: only the main job export
- Clients can query for specific inputs as needed

### Early-binding (opt-in)
- Enriches events with full activity/child workflow inputs
- Single query batch-fetches all required data
- Ready for immediate dashboard rendering

## Configuration Options

```typescript
interface ExecutionExportOptions {
  mode?: 'sparse' | 'verbose';      // Fetch child workflows?
  exclude_system?: boolean;          // Filter system activities?
  omit_results?: boolean;            // Omit activity results?
  max_depth?: number;                // Max child recursion depth
  enrich_inputs?: boolean;           // NEW: Fetch activity inputs?
  allow_direct_query?: boolean;      // NEW: Query expired jobs?
}
```

## Usage Examples

### Basic Export (late-binding)
```typescript
const handle = await Durable.workflow.getHandle(taskQueue, workflowName, workflowId);
const execution = await handle.exportExecution();

// Events have timeline_key but no inputs
const activity = execution.events.find(e => e.event_type === 'activity_task_completed');
console.log(activity.attributes.timeline_key); // "-workflowId-$activityName-5"
console.log(activity.attributes.input);        // undefined
```

### Enriched Export (early-binding)
```typescript
const execution = await handle.exportExecution({
  enrich_inputs: true,
});

// Events now include full inputs
const activity = execution.events.find(e => e.event_type === 'activity_task_completed');
console.log(activity.attributes.input);  // ["arg1", "arg2", ...]
console.log(activity.attributes.result); // { ... }
```

### Expired Job Export
```typescript
// For jobs that have expired (is_live=false) but are still in the database
const execution = await handle.exportExecution({
  allow_direct_query: true,
  enrich_inputs: true,
});

// Fallback to direct DB query if handle export fails
```

## Enriched Event Types

When `enrich_inputs: true`, the following event types gain an `input` field:

### Activity Events
```typescript
interface ActivityTaskScheduledAttributes {
  kind: 'activity_task_scheduled';
  activity_type: string;
  timeline_key: string;
  execution_index: number;
  input?: any;  // <-- NEW
}

interface ActivityTaskCompletedAttributes {
  kind: 'activity_task_completed';
  activity_type: string;
  result?: any;
  scheduled_event_id?: number;
  timeline_key: string;
  execution_index: number;
  input?: any;  // <-- NEW
}

interface ActivityTaskFailedAttributes {
  kind: 'activity_task_failed';
  activity_type: string;
  failure?: any;
  scheduled_event_id?: number;
  timeline_key: string;
  execution_index: number;
  input?: any;  // <-- NEW
}
```

### Child Workflow Events
```typescript
interface ChildWorkflowExecutionStartedAttributes {
  kind: 'child_workflow_execution_started';
  child_workflow_id: string;
  awaited: boolean;
  timeline_key: string;
  execution_index: number;
  input?: any;  // <-- NEW
}
```

## Migration from Custom Exporters

If your project currently uses a custom export layer (like long-tail's `services/export`), you can migrate to native HotMesh enrichment:

### Before (custom exporter)
```typescript
import { exportWorkflowExecution } from './services/export';

const execution = await exportWorkflowExecution(
  workflowId,
  taskQueue,
  workflowName,
  options,
);
```

### After (native HotMesh)
```typescript
import { Durable } from '@hotmeshio/hotmesh';

const handle = await Durable.workflow.getHandle(taskQueue, workflowName, workflowId);
const execution = await handle.exportExecution({
  enrich_inputs: true,       // Equivalent to enrichEventInputs()
  allow_direct_query: true,  // Equivalent to exportExecutionDirect() fallback
});
```

## Performance Characteristics

### Late-binding (enrich_inputs: false)
- **Queries**: 1 (job hash only)
- **Latency**: ~10ms
- **Use case**: List views, summary dashboards

### Early-binding (enrich_inputs: true)
- **Queries**: 3 (job hash + activity inputs + child workflow inputs)
- **Latency**: ~30-50ms
- **Use case**: Detailed execution timeline, debugging

### Direct Query (allow_direct_query: true)
- **Queries**: 2 (job row + attributes)
- **Latency**: ~20ms
- **Use case**: Expired/archived jobs

## Architecture

The enrichment feature maintains HotMesh's layered architecture:

### Type Layer
- `types/exporter.ts` - Export types and options

### Service Layer
- `services/durable/exporter.ts` - Business logic, no SQL
- `services/durable/handle.ts` - Public API

### Store Layer
- `services/store/index.ts` - Abstract interface
- `services/store/providers/postgres/postgres.ts` - Implementation
- `services/store/providers/postgres/exporter-sql.ts` - SQL queries

## Provider Support

Input enrichment requires provider support for three optional methods:

```typescript
interface StoreService {
  // ...existing methods

  getActivityInputs?(workflowId: string, symbolField: string): Promise<{
    byJobId: Map<string, any>;
    byNameIndex: Map<string, any>;
  }>;

  getChildWorkflowInputs?(
    childJobKeys: string[],
    symbolField: string,
  ): Promise<Map<string, any>>;

  getJobByKeyDirect?(jobKey: string): Promise<{
    job: JobRow;
    attributes: Record<string, string>;
  }>;
}
```

**Currently supported**: Postgres

## Symbol Resolution

The exporter uses HotMesh's native symbol registry to resolve compressed field codes:

```typescript
// Symbols are resolved from the stable path
'activity_trigger/output/data/arguments' → 'aag,0'
'trigger/output/data/arguments' → 'aBa,0'

// These codes are used to query the jobs_attributes table
SELECT value FROM jobs_attributes WHERE field = 'aag,0';
```

No hardcoded symbol strings are used - everything is resolved dynamically from the symbol registry.

## Testing

See `tests/durable/exporter/enrichment.test.ts` for comprehensive integration tests demonstrating:

- Late-binding exports (sparse)
- Early-binding exports (enriched)
- Sequential late→early binding calls
- Summary validation
- Input data verification
