# Dimensional Addressing

This document describes the dimensional addressing system used by HotMesh to isolate cycle iterations and parallel branches into independent state namespaces. For the core collation ledger mechanics, see the [Collation Design Document](./README.md).

## Dimensional Address (DAD)

Every activity executes in a dimensional plane identified by its **DAD** (Dimensional Address). The DAD is a comma-separated path that encodes the activity's position in the execution tree:

```
,<trigger_index>,<child1_index>,<child2_index>,...
```

All activities start at index `0` in their dimension. Cycle re-entry increments the index to create a new plane.

## Examples

| Activity | DAD | Meaning |
| -------- | --- | ------- |
| Trigger | `,0` | Root, first (only) dimension |
| First child of trigger | `,0,0` | One level deep, first dimension |
| Second-level child | `,0,0,0` | Two levels deep, first dimension |
| Cycle re-entry of child at depth 1 | `,0,1` | Same depth, second dimension |
| Children of re-entered activity | `,0,1,0` | Fresh plane under new dimension |

## How Children Receive Their DAD

When an activity spawns children in Step 2:

1. The parent resolves its own DAD (accounting for cycle index via `resolveDad()`)
2. The parent appends `,0` to create the **adjacent DAD** (via `resolveAdjacentDad()`)
3. All children receive this adjacent DAD in their stream metadata
4. Each child's own children will extend the DAD by one more level

## How Cycle Changes the DAD

When a cycle activity targets an ancestor:

1. The current DAD (e.g., `,0,0,1,0`) is trimmed to the ancestor's position in the graph
2. The last segment is replaced with `,0` (reset for new dimension)
3. The ancestor re-enters with this new DAD
4. The ancestor's Leg 2 counter increments, producing a `dimensionalIndex > 0`
5. `resolveDad()` replaces the trailing `,0` with the new index (e.g., `,0,1`)
6. All children of this re-entry run under the new dimensional prefix

## Dimensional Isolation

Each dimensional address maps to an independent state namespace. Activity state stored under DAD `,0,0` is invisible to state stored under DAD `,0,1`. This ensures:

* Cycle iterations do not overwrite prior iteration state
* Parallel branches in the graph execute in isolated namespaces
* The ledger tracks each dimension independently via the `getDimensionsById()` mapping

## Semaphore Accounting Across Generations

The semaphore tracks all open obligations regardless of dimensional depth:

1. **Trigger** initializes semaphore = number of direct children
2. **Each completing activity** applies `delta = (N - 1)` where N = children spawned
3. **Cycle re-entries** do not change the semaphore directly; the cycle activity sets `status = 0` (no-op), and the re-entered ancestor's step protocol handles its own children
4. **The job completes** when the semaphore reaches the threshold (default: `0`), meaning all activities across all dimensions have completed
