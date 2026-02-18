# Activity Collation Ledger (15-Digit) + Job Semaphore

**Robust idempotency for 2-leg distributed transactions**

This document defines the **collation strategy** used to guarantee **exactly-once phase execution**, **safe retries**, and **deterministic resume** across a two-leg distributed transaction model.

HotMesh tracks activity correctness using:

1. A **15-digit activity ledger** (one per activity instance)
2. A **15-digit per-message GUID ledger** (one per Leg2 transition message)
3. A **job semaphore** (one per job) to guarantee **job closure** and **exactly-once completion tasks**

All ledgers are **monotonic (increment-only)** and are updated **atomically** with durable writes to ensure correctness under crashes, retries, and message replays.

## Goals

This design guarantees:

* **Idempotent processing** of transition messages
* **Crash-safe resume** at phase boundaries and within Leg2 steps
* **High retry capacity** (Leg1 supports 999 entry attempts)
* **High cycle capacity** (Leg2 supports 99,999,999 entries per activity)
* **Deterministic reentry decisions** using a single integer read
* **Exactly-once semantics** for each durable step via transaction bundling
* **Correct job closure** even if the server crashes after the job becomes complete

## Core Principle: Transaction Bundling

Every meaningful step follows one rule:

> **Durable work is committed in the same atomic transaction as its concluding digit update.**

If the digit is set, the work committed.
If the digit is not set, the work must be retried.

This pattern is the foundation of correctness.

### Transaction Bundling in a Layered Architecture

HotMesh engines and activities do **not** author SQL directly. They operate against a small set of **durable primitives** exposed by the store provider.

Those primitives are implemented per backend (Postgres) and must guarantee the bundling rule.

**Principle:**

> Correctness is defined by what the primitive commits atomically, not by what the engine “intends” to do.

### Transactions as a Command Bundle

When a transaction is present, the provider must:

* collect a sequence of primitive calls into a **single BEGIN/COMMIT**
* ensure each primitive’s SQL executes inside that same transaction
* rely on **database atomicity** for crash safety

The engine must assume:

* if COMMIT happens, all bundled effects happened
* if COMMIT does not happen, none happened

This is how step markers become durable proofs.

## 1) The 15-Digit Activity Ledger

Each activity owns a single 15-digit integer, starting at:

```
000000000000000
```

The integer is incremented in targeted positions to represent attempts and durable completion markers.

### Digit Map (15 digits)

```
Position:   1    2    3   4     5    6   7     8    9   10    11   12  13   14   15
Weight:    100T  10T  1T  100B  10B  1B  100M  10M  1M  100K  10K  1K  100  10   1
```

### Semantic Meaning

| Position(s) | Weight Range | Meaning                                          |
| ----------: | ------------ | ------------------------------------------------ |
|           3 | 1T           | **Leg1 entry attempt counter** (0..999)          |
|           4 | 100B         | **Leg1 completion marker** (0/1)                 |
|           5 | 10B          | **Leg2 step marker**: work done                  |
|           6 | 1B           | **Leg2 step marker**: children spawned           |
|           7 | 100M         | **Leg2 step marker**: job completion tasks done  |
|        8–15 | 10M..1       | **Leg2 entry counter** (8 digits, 0..99,999,999) |

All other digits are reserved.

## 2) Derived Fields (How to Interpret the Activity Ledger)

Given a 15-digit ledger `L`:

### Leg1 attempt counter

Tracks how many times Leg1 has been entered (including reentries).

* Range: `0..999`
* Increment: `+1,000,000,000,000` (1 trillion)

### Leg1 completion marker

Indicates whether Leg1 durable work committed.

* Values: `0` (not complete) or `1` (complete)
* Increment: `+100,000,000,000` (100 billion)

### Leg2 entry counter (8 digits)

Tracks how many times Leg2 has been entered.

* Range: `0..99,999,999`
* Increment: `+1` (ones digit)

This counter is used for dimensional isolation and cycle indexing.

### Leg2 step markers (3 flags)

These are durable “step completed” proofs:

* **10B**: Leg2 work done
* **1B**: children spawned
* **100M**: job completion tasks done

## 3) Overflow Rules (Hard Constraints)

The ledger must **never exceed 15 digits**.

### Leg1 attempt ceiling

Leg1 entry attempts are capped at **999**.

* If the Leg1 attempt counter is already 999, the activity must fail fast and escalate.

### Leg2 entry ceiling

Leg2 entry counter is capped at **99,999,999**.

* If Leg2 entry counter reaches 99,999,999, the activity must fail fast and escalate.

### Carry isolation (critical)

The Leg2 entry counter uses the **last 8 digits** only.

This prevents overflow carry into the **100M digit**, which is reserved for the Leg2 job completion step marker.

## 4) Leg1 Protocol

Leg1 is the first half of the distributed transaction.

### 4.1 Leg1 Entry (attempt marker)

On claim/dequeue of a Leg1 transition message:

1. Atomically increment the activity ledger by **+1T**
2. Read the updated ledger value

This update is **not** bundled with Leg1 work.
It exists only to mark entry and count attempts.

#### Leg1 entry increment

```
+1_000_000_000_000
```

### 4.2 Leg1 Reentry Decision

Immediately after incrementing the Leg1 attempt counter:

* If the **Leg1 completion marker (100B)** is already set, Leg1 has already completed.

  * The message is stale/replayed.
  * **Ack/delete the message immediately.**
* If the Leg1 completion marker is not set, Leg1 work must execute.

### 4.3 Leg1 Completion (bundled with work)

Leg1 completion is a durable proof.

At the end of Leg1 processing, in the **same atomic transaction** as all Leg1 durable writes:

1. Perform Leg1 durable work
2. Increment **100B** to mark Leg1 complete
3. Commit atomically

#### Leg1 completion increment

```
+100_000_000_000
```

If the transaction commits, Leg1 completion is guaranteed.

## 5) Leg2 Protocol

Leg2 supports cyclic activity behavior and repeated entries.
It must support:

* millions of entries
* message-level uniqueness
* step-level resume within a single Leg2 message

Leg2 uses two ledgers:

1. **Activity ledger** (15-digit)
2. **GUID ledger** (15-digit) per Leg2 transition message (in essence, a synthetic collation integer that is created per unique Leg2 stream message processed)

## 6) The 15-Digit GUID Ledger (Per Leg2 Message)

Each Leg2 transition message has a unique GUID.
For each GUID, the system stores a **15-digit integer** that tracks:

* attempt count for this GUID
* step completion markers for this GUID
* a durable “job closed snapshot” bit for correctness under crash

The GUID ledger is increment-only and supports idempotent resume.

### GUID Ledger Digit Map (15 digits)

```
Position:   1    2    3   4     5    6   7     8    9   10    11   12  13   14   15
Weight:    100T  10T  1T  100B  10B  1B  100M  10M  1M  100K  10K  1K  100  10   1
```

### GUID Ledger Meaning

| Position(s) | Weight Range | Meaning                                                      |
| ----------: | ------------ | ------------------------------------------------------------ |
|           4 | 100B         | **Job closed snapshot** (0/1) captured at Step 2 commit time |
|           5 | 10B          | Step marker: work done                                       |
|           6 | 1B           | Step marker: children spawned                                |
|           7 | 100M         | Step marker: job completion tasks done                       |
|        8–15 | 10M..1       | Attempt counter (8 digits, 0..99,999,999)                    |

All other digits are reserved.

## 7) Leg2 Entry (attempt marker + dimensional counter)

On claim/dequeue of a Leg2 transition message:

### 7.1 Increment the activity Leg2 entry counter

Atomically increment the activity ledger by:

```
+1
```

This increments the **last 8 digits** only and yields a new `Leg2EntryCount`.

### 7.2 Increment the GUID attempt counter

In the same transaction that incremented the activity Leg2 entry counter, persist the returned value as the initial value for the GUID ledger *if the GUID ledger does not exist*.

## 8) Leg2 Step-Level Idempotency

Leg2 processing contains multiple durable steps.
Each step has a corresponding digit marker.

The system must be able to crash after any step and resume safely.

### Step Markers (applied to GUID ledger)

| Step   | Meaning                        | Increment         |
| ------ | ------------------------------ | ----------------- |
| Step 1 | Work completed                 | `+10_000_000_000` |
| Step 2 | Children spawned               | `+1_000_000_000`  |
| Step 3 | Job completion tasks completed | `+100_000_000`    |

When a step completes, the step marker in the *GUID Ledger* must be incremented **in the same atomic transaction** as the durable writes that implement that step.

### Step 2 Requires a Compound Primitive

Step 2 has an additional correctness requirement:

* it changes global job state (job semaphore)
* it may close the job (edge event)
* it must persist the “job closed” fact into the GUID ledger deterministically

This cannot rely on application control flow or in-memory decisions.

It must be expressed as a single durable commit.

## 9) Job Semaphore (Per Job)

A **job semaphore** is a durable integer that represents the number of **open activity obligations** remaining for a job.

It exists to guarantee:

* deterministic job completion
* exactly-once job completion tasks
* crash-safe closure detection (even if the job reaches `0` and the server crashes)

### 9.1 Definition

For each job `jid`, maintain:

```
jobSemaphore: integer >= 0
```

Interpretation:

* `jobSemaphore > 0` → job is still active (open obligations remain)
* `jobSemaphore == 0` → job is complete

This is a job-level counter, not an activity counter.

---

### 9.2 Semaphore Delta Rule (Spawn-Aware)

When an activity completes its work, it may spawn `N` child activities.

At that moment, the job semaphore must be updated by:

```
delta = (N - 1)
```

This models:

* parent closes: `-1`
* children open: `+N`

Examples:

| Spawned children (N) | Delta (N - 1) | Meaning                                      |
| -------------------: | ------------: | -------------------------------------------- |
|                    0 |            -1 | parent closes, nothing replaces it           |
|                    1 |             0 | parent closes, exactly one child replaces it |
|                    2 |            +1 | parent closes, two children open (net +1)    |
|                    3 |            +2 | parent closes, three children open (net +2)  |

---

### 9.3 The Job Closed Snapshot (Crash-Safe Edge Capture)

The job semaphore reaching a threshold (typically `0`) is an **edge event**, not just a state.

If the server crashes after the job semaphore reaches the threshold but before job completion tasks run, the system must still guarantee completion tasks execute exactly once.

To guarantee this, the system captures a durable **job closed snapshot bit** into the GUID ledger at the moment Step 2 commits:

* `GUID.jobClosedSnapshot = 1` if this transition caused the semaphore to reach the threshold
* otherwise it remains `0`

This snapshot encodes **only 0/1** and is not recomputed on replay.

## 10) Leg2 Processing Order (Deterministic Resume)

Leg2 processing is always driven by the **GUID ledger**.

On processing a Leg2 transition message:

1. Load the GUID ledger
2. Load the job semaphore
3. Execute steps conditionally based on step markers and job snapshot bit

## 11) Step 1: Do Work (Idempotent)

If the GUID ledger does **not** have the `10B` marker set:

* Perform Leg2 work
* Atomically commit:

  * work writes
  * GUID ledger `+10B`
  * activity ledger `+10B`

If the marker is already set, skip.

## 12) Step 2: Spawn Children + Update Job Semaphore (Idempotent + Transactional)

Step 2 is the transactional heart of job correctness.

If the GUID ledger does **not** have the `1B` marker set:

* Publish/spawn child activities (durably)
* Update the job semaphore using `delta = (N - 1)`
* Persist a job closed snapshot (0/1) onto the GUID ledger if the threshold was reached
* Atomically commit:

  * child stream inserts
  * GUID ledger `+1B`
  * activity ledger `+1B`
  * job semaphore update
  * GUID job closed snapshot digit update (100B) if threshold reached

If the marker is already set, skip.

---

### 12.1 Step 2 Bundling Invariant (Required)

Step 2 must obey this invariant:

> **Child stream inserts, job semaphore update, “children spawned” marker increments, and the job closed snapshot bit must commit in one atomic transaction.**

If the marker is set, the semaphore update committed.
If the marker is not set, the update did not commit and must be retried.

This invariant is the durable proof that:

* children were spawned exactly once
* job semaphore moved exactly once for this activity completion
* if the job was closed by this transition, the GUID ledger records responsibility

---

### 12.2 Compound Primitive Requirement (Required)

Engines and activities do not bind SQL outputs to subsequent SQL inputs.

Instead, the provider must expose a compound primitive that performs the required Step 2 bundling in one durable commit.

Conceptually:

```
setStatusAndCollateGuid(
  statusDelta,
  threshold,
  guidField,
  guidWeight
) -> thresholdHit (0/1)
```

This primitive must be implemented as a single SQL statement that:

1. applies the semaphore delta to the job row
2. computes whether the post-update semaphore equals the threshold (0/1)
3. increments the GUID ledger by `(thresholdHit * guidWeight)` to persist the snapshot bit
4. returns `thresholdHit`

This ensures the “job closed snapshot” is persisted even if the server crashes immediately after Step 2.

---

### 12.3 Threshold Semantics

The threshold is typically `0` (job completion), but the primitive supports any integer threshold:

* `0` → job fully complete
* `1`, `12`, etc. → alternate thresholds for specialized semantics

The snapshot bit encodes only whether the threshold was reached:

```
thresholdHit = (statusAfter == threshold ? 1 : 0)
```

The snapshot bit is persisted by incrementing the GUID ledger at the reserved digit weight (typically the 100B digit).

## 13) Step 3: Job Completion Tasks (Conditional + Idempotent)

Step 3 must run exactly once.

Step 3 eligibility is driven by:

* GUID job closed snapshot bit (`GUID.100B`)
* Step 3 completion marker (`GUID.100M`)
* job semaphore state (optional fast-path)

### 13.1 Step 3 Execution Rules

Step 3 must execute if:

1. GUID `100M` marker is **not set** (completion tasks not done yet)
2. GUID job closed snapshot is set (`GUID.100B` is set)

If Step 3 runs:

* Run job completion tasks
* Atomically commit:

  * completion task writes
  * GUID ledger `+100M`
  * activity ledger `+100M`

If the marker is already set, skip.

### 13.2 Why Step 3 Uses the GUID Snapshot

The job semaphore being at the threshold (ex: `0`) indicates the job is complete, but does not identify **which transition message** is responsible for running completion tasks.

The GUID snapshot provides that identity.

This guarantees correctness under the crash case:

* Step 2 closes the job and commits
* server crashes before Step 3 runs
* replay sees job semaphore already complete
* GUID snapshot proves this transition must still run Step 3 if not already marked

## 14) Ack/Delete (Always Forward)

After all required steps are completed or skipped, the system must:

* **ack/delete the transition message**
* flow forward deterministically

All reentry logic exists to guarantee forward progress.

## 15) Dimensional Indexing (Cycles)

The Leg2 entry counter provides dimensional isolation for cyclic activities.

Let:

```
Leg2EntryCount = activityLedger % 100_000_000
```

Then:

```
dimensionalIndex = Leg2EntryCount - 1
```

This ensures each cycle iteration can spawn descendants into a unique dimensional space.

## 16) Correctness Guarantees

This design guarantees:

### Exactly-once step execution

Each step is “exactly-once” because:

* the step’s durable writes and its marker increment commit atomically
* on reentry, markers determine which steps must be skipped

### Crash-safe resume

If the process crashes mid-flight:

* unmarked steps are retried
* marked steps are skipped
* processing always converges to ack/delete

### Message replay safety

If a transition message is replayed:

* GUID ledger markers prevent repeated execution
* activity ledger provides global visibility and dimensional indexing

### Correct job closure under server crash

Job completion tasks cannot be lost because:

* Step 2 transaction persists a **job closed snapshot bit** into the GUID ledger
* Step 3 is eligible even if the job semaphore is already `0`, as long as the GUID snapshot proves responsibility and Step 3 marker is not set

### High retry tolerance

* Leg1: up to **999** entry attempts
* Leg2: up to **99,999,999** entry attempts per activity
* GUID attempt counter: up to **99,999,999** per message GUID

## 17) Required Implementation Rules

### Rule A: Monotonic increments only

Ledgers are increment-only. No decrements. No resets.

### Rule B: Step markers must be transaction-bundled

Step marker increments must occur in the same transaction as the step’s durable writes.

### Rule C: GUID ledger drives resume

Leg2 step decisions are based on the GUID ledger.

### Rule D: Step 2 must use a compound primitive

Step 2 must commit atomically:

* child stream inserts
* job semaphore delta update
* “children spawned” marker increments
* job closed snapshot persistence to the GUID ledger

This must be implemented as a single compound provider primitive (one SQL statement).

### Rule E: Job closed snapshot encodes only 0/1

The job closed snapshot is a single durable bit persisted to the GUID ledger (100B digit).

### Rule F: Overflow checks are mandatory

Before any entry increment:

* enforce Leg1 attempt ceiling
* enforce Leg2 entry ceiling
* enforce 15-digit maximum invariant

### Rule G: Ack/delete must always happen

Every transition message must eventually be acknowledged or deleted.
All reentry logic exists to guarantee forward progress.

## 18) Example State Progression

### 18.1 Leg1 entry and completion

Start:

```
000000000000000
```

Leg1 entry attempt #1:

```
001000000000000
```

Leg1 completes (bundled with Leg1 work):

```
001100000000000
```

Replayed/stale Leg1 message entry:

```
002100000000000
```

Since `100B` is set, the processor immediately ack/deletes the message.

---

### 18.2 Leg2 entry and step resume (single GUID)

Activity ledger after Leg1:

```
001100000000000
```

GUID ledger starts:

```
000000000000000
```

Leg2 entry attempt #1:

* activity `+1`:

```
001100000000001
```

* guid `+1`:

```
000000000000001
```

Step 1 completes:

* activity `+10B`:

```
001110000000001
```

* guid `+10B`:

```
000010000000001
```

Crash occurs before spawning children.

Reprocess same GUID:

* GUID has `10B` set → skip Step 1
* Step 2 runs and sets `1B` markers
* Step 3 runs only if eligible

---

### 18.3 Step 2 job semaphore update (N=2 children)

Assume job semaphore before Step 2:

```
jobSemaphore = 5
```

Activity spawns `N = 2` children:

```
delta = (2 - 1) = +1
jobSemaphoreAfter = 6
jobClosedSnapshot = 0
```

Step 2 transaction commits:

* inserts child transitions
* updates job semaphore
* sets children spawned markers

Result:

* GUID ledger `1B` set
* Activity ledger `1B` set
* GUID jobClosedSnapshot remains 0

---

### 18.4 Step 2 closes job (N=0 children)

Assume job semaphore before Step 2:

```
jobSemaphore = 1
```

Activity spawns `N = 0` children:

```
delta = (0 - 1) = -1
jobSemaphoreAfter = 0
jobClosedSnapshot = 1
```

Step 2 transaction commits:

* inserts no children
* updates job semaphore to 0
* sets children spawned markers
* sets GUID jobClosedSnapshot digit (100B)

Result:

* Job is complete
* GUID ledger proves: “this transition closed the job”

If the server crashes immediately after this commit (before Step 3):

* on replay, jobSemaphore is already 0
* Step 3 still runs because GUID jobClosedSnapshot is set and Step 3 marker is not

## Summary

This collation strategy provides a durable, monotonic, overflow-safe ledger for distributed transaction correctness:

* **15-digit activity ledger** tracks lifecycle, dimensional indexing, and global step completion
* **15-digit GUID ledger** tracks per-message uniqueness, step-level resume, and crash-safe job closure responsibility
* **job semaphore** guarantees correct job closure detection and exactly-once completion tasks
* **transaction-bundled digit increments** provide exactly-once proofs for each durable step
* **high retry and cycle capacity** ensure long-running, reentrant workflows remain correct and forward-progressing under arbitrary failures
