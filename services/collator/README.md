# Monotonic Collation: Exactly-Once Distributed Workflows

Every activity in a distributed workflow can crash, retry, or replay at any point. The collation ledger makes this safe. It is a monotonic integer that records what has durably committed, so the system always knows what happened and what remains.

The approach is simple: **every durable write is committed in the same atomic transaction as the digit that proves it occurred.** If the digit is set, the work committed. If not, the work must be retried. There is no ambiguity, no coordination protocol, and no external lock. A single integer read determines the full recovery path.

Three structures enforce this:

| Structure | Scope | Purpose |
| --------- | ----- | ------- |
| **Activity Ledger** (15-digit) | One per activity instance | Tracks entry attempts, leg completion, finalization, and cycle count |
| **GUID Ledger** (15-digit) | One per Leg2 transition message | Tracks per-message step completion and crash-safe job closure responsibility |
| **Job Semaphore** | One per job | Counts open activity obligations; determines when the job is complete |

The two ledgers are **monotonic** (increment-only). The semaphore is **convergent** (increments when children are spawned, decrements when activities complete, converging toward the completion threshold). All three are **transaction-bundled** (updated atomically with the work they certify).

---

## Table of Contents

- [Goals](#goals)
- [Core Principle: Transaction Bundling](#core-principle-transaction-bundling)
  - [Transaction Bundling in a Layered Architecture](#transaction-bundling-in-a-layered-architecture)
  - [Transactions as a Command Bundle](#transactions-as-a-command-bundle)
- [1) The 15-Digit Activity Ledger](#1-the-15-digit-activity-ledger)
- [2) Derived Fields](#2-derived-fields-how-to-interpret-the-activity-ledger)
- [3) Overflow Rules](#3-overflow-rules-hard-constraints)
- [4) Leg1 Protocol](#4-leg1-protocol)
- [5) Leg2 Protocol](#5-leg2-protocol)
- [6) The 15-Digit GUID Ledger](#6-the-15-digit-guid-ledger-per-leg2-message)
- [7) Leg2 Entry](#7-leg2-entry-ordinal-seed--dimensional-counter)
- [8) Leg2 Step-Level Idempotency](#8-leg2-step-level-idempotency)
- [9) Job Semaphore](#9-job-semaphore-per-job)
- [10) Leg2 Processing Order](#10-leg2-processing-order-deterministic-resume)
- [11) Step 1: Do Work](#11-step-1-do-work-idempotent)
- [12) Step 2: Spawn Children + Update Semaphore](#12-step-2-spawn-children--update-job-semaphore-idempotent--transactional)
- [13) Step 3: Job Completion Tasks](#13-step-3-job-completion-tasks-conditional--idempotent)
- [14) Ack/Delete](#14-ackdelete-always-forward)
- [15) Dimensional Indexing](#15-dimensional-indexing-cycles)
- [16) Correctness Guarantees](#16-correctness-guarantees)
- [17) Required Implementation Rules](#17-required-implementation-rules)
- [18) Example State Progression](#18-example-state-progression)
- [19) Activity Categories](#19-activity-categories)
- [20) Implementation by Activity Type](#20-implementation-by-activity-type)
  - [Trigger](#201-trigger)
  - [Worker](#202-worker)
  - [Await](#203-await)
  - [Hook](#204-hook)
  - [Signal](#205-signal)
  - [Cycle](#206-cycle)
  - [Interrupt](#207-interrupt)
- [21) Parent/Child Transitions and Dimensional Addressing](#21-parentchild-transitions-and-dimensional-addressing)
- [Summary](#summary)

---

## Goals

This design guarantees:

* **Idempotent processing** of transition messages
* **Crash-safe resume** at phase boundaries and within Leg2 steps
* **High retry capacity** (Leg1 supports 99 entry attempts)
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

> Correctness is defined by what the primitive commits atomically, not by what the engine "intends" to do.

### Transactions as a Command Bundle

When a transaction is present, the provider must:

* collect a sequence of primitive calls into a **single BEGIN/COMMIT**
* ensure each primitive's SQL executes inside that same transaction
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
|           1 | 100T         | **Finalize** (0 = active, 2 = finalized) |
|         2-3 | 10T..1T      | **Leg1 entry attempt counter** (0..99)           |
|           4 | 100B         | **Leg1 completion marker** (0/1)                 |
|         5-7 | 10B..100M    | Reserved                                         |
|        8-15 | 10M..1       | **Leg2 entry counter** (8 digits, 0..99,999,999) |

Positions 5-7 are structurally reserved but **not written by the Leg2 step protocol**. Step markers (work done, children spawned, job completion tasks) are tracked on the **GUID ledger**, not the activity ledger. This is because Leg 2 supports multiple entries per activity (hook cycles, retries), and each entry gets its own GUID with independent step tracking.

## 2) Derived Fields (How to Interpret the Activity Ledger)

Given a 15-digit ledger `L`:

### Leg1 attempt counter

Tracks how many times Leg1 has been entered (including reentries). Uses positions 2-3 as a 2-digit counter.

* Range: `0..99`
* Increment: `+1,000,000,000,000` (1 trillion, position 3; carries into position 2)

### Leg1 completion marker

Indicates whether Leg1 durable work committed.

* Values: `0` (not complete) or `1` (complete)
* Increment: `+100,000,000,000` (100 billion)

### Leg2 entry counter (8 digits)

Tracks how many times Leg2 has been entered.

* Range: `0..99,999,999`
* Increment: `+1` (ones digit)

This counter is used for dimensional isolation and cycle indexing.

### Leg2 step markers

Step-level completion proofs are tracked on the **GUID ledger** (not the activity ledger). See [Section 6](#6-the-15-digit-guid-ledger-per-leg2-message) for the GUID ledger digit map and [Section 8](#8-leg2-step-level-idempotency) for the step protocol.

## 3) Overflow Rules (Hard Constraints)

The ledger must **never exceed 15 digits**.

### Leg1 attempt ceiling

Leg1 entry attempts are capped at **99**.

* If the Leg1 attempt counter reaches 99, the activity must fail fast and escalate.
* Position 1 is reserved for finalize (2). If the entry counter overflows past 99, it would carry into position 1, falsely indicating finalization.

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

* ordinal (the Leg2 entry sequence number at the time this GUID was created)
* step completion markers for this GUID
* a durable "job closed snapshot" bit for correctness under crash

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
|        8-15 | 10M..1       | **Ordinal** (8 digits, seeded at creation with the activity's Leg2 entry count) |

All other digits are reserved.

## 7) Leg2 Entry (ordinal seed + dimensional counter)

On claim/dequeue of a Leg2 transition message:

### 7.1 Increment the activity Leg2 entry counter

Atomically increment the activity ledger by:

```
+1
```

This increments the **last 8 digits** only and yields a new `Leg2EntryCount`.

### 7.2 Seed the GUID ordinal

In the same transaction that incremented the activity Leg2 entry counter, persist the returned `Leg2EntryCount` as the initial value for the GUID ledger *if the GUID ledger does not exist*. This value becomes the GUID's **ordinal** — it records the sequence position of this message among all Leg2 messages processed for the activity.

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
* it must persist the "job closed" fact into the GUID ledger deterministically

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

* `jobSemaphore > 0` -> job is still active (open obligations remain)
* `jobSemaphore == 0` -> job is complete

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

  * work writes (setState)
  * GUID ledger `+10B`

The step marker is written to the **GUID ledger only**. The activity ledger is not updated during Step 1. Because Leg 2 can be entered multiple times (each with a unique GUID), step progress must be tracked per-message, not per-activity.

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
  * job semaphore delta update
  * GUID job closed snapshot digit update (100B) if threshold reached

The semaphore delta and the job-closed snapshot are computed and persisted in a single compound SQL statement (`setStatusAndCollateGuid`). The compound primitive applies the delta, checks whether the post-update semaphore equals the threshold, and if so, increments the GUID ledger's 100B digit — all within the same commit. This is how the GUID can later prove "I was the message that closed this job" even after a crash.

If the marker is already set, skip.

---

### 12.1 Step 2 Bundling Invariant (Required)

Step 2 must obey this invariant:

> **Child stream inserts, job semaphore delta, GUID "children spawned" marker (+1B), and GUID job-closed snapshot (+100B if threshold hit) must commit in one atomic transaction.**

If the GUID `1B` marker is set, the semaphore update committed.
If the GUID `1B` marker is not set, the update did not commit and must be retried.

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

This ensures the "job closed snapshot" is persisted even if the server crashes immediately after Step 2.

---

### 12.3 Threshold Semantics

The threshold is typically `0` (job completion), but the primitive supports any integer threshold:

* `0` -> job fully complete
* `1`, `12`, etc. -> alternate thresholds for specialized semantics

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
  * activity ledger `+200T` (finalize, for non-cycle activities — sets position 1 to 2)

The Step 3 marker is written to the **GUID ledger only**. The finalize marker (`+200T`) is the only activity ledger update in the entire step protocol — it sets position 1 to `2` (finalized), closing the activity to new Leg2 GUIDs. A value of `2` at position 1 means this activity took responsibility for the edge event and ran job completion tasks. Cycle activities skip finalize so they can accept future re-entries.

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

Each step is "exactly-once" because:

* the step's durable writes and its marker increment commit atomically
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

* Leg1: up to **99** entry attempts
* Leg2: up to **99,999,999** entry attempts per activity
* GUID ordinal: up to **99,999,999** per activity (one GUID per Leg2 entry)

## 17) Required Implementation Rules

### Rule A: Monotonic increments only

Ledgers are increment-only. No decrements. No resets.

### Rule B: Step markers must be transaction-bundled

Step marker increments must occur in the same transaction as the step's durable writes.

### Rule C: GUID ledger drives resume

Leg2 step decisions are based on the GUID ledger.

### Rule D: Step 2 must use a compound primitive

Step 2 must commit atomically:

* child stream inserts
* job semaphore delta update
* GUID ledger `+1B` ("children spawned" marker)
* GUID ledger `+100B` (job-closed snapshot, if threshold reached)

The semaphore delta and snapshot bit must be computed and persisted in a single compound provider primitive (one SQL statement).

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

* guid: copy the ordinal position returned when activity was `+1` incremented (but only if guid did not already exist):

```
000000000000001
```

Step 1 completes (GUID ledger only):

* activity ledger unchanged:

```
001100000000001
```

* guid `+10B`:

```
000010000000001
```

The activity ledger does not change during Step 1. The GUID ledger alone proves that work committed for this message.

Crash occurs before spawning children.

Reprocess same GUID:

* GUID has `10B` set -> skip Step 1
* Step 2 runs: spawns children, updates semaphore, captures edge snapshot — sets GUID `1B` (and `100B` if threshold hit)
* Step 3 runs only if GUID job-closed snapshot is set and Step 3 marker is not

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
* updates job semaphore (+1)
* sets GUID `1B` (children spawned marker)

Result:

* GUID ledger `1B` set (children spawned marker)
* GUID jobClosedSnapshot remains 0 (threshold not reached)
* Activity ledger unchanged (step markers are GUID-only)

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

Step 2 transaction commits (single compound primitive):

* inserts no children
* updates job semaphore to 0
* sets GUID `1B` (children spawned marker)
* sets GUID `100B` (job-closed snapshot — threshold reached)

Result:

* Job is complete
* GUID ledger proves: "this transition closed the job"

If the server crashes immediately after this commit (before Step 3):

* on replay, jobSemaphore is already 0
* Step 3 still runs because GUID jobClosedSnapshot is set and Step 3 marker is not

---

## 19) Activity Categories

Every activity in HotMesh falls into one of four categories based on its leg structure and how it interacts with the step protocol. The category determines which entry method, step protocol, and ledger increments are used.

| Category | Legs | Step Protocol | Entry Method | Activities |
| -------- | ---- | ------------- | ------------ | ---------- |
| **A** | Leg 1 + Leg 2 | Leg2 3-step (`executeStepProtocol`) | `verifyEntry` (L1), `verifyReentry` (L2) | Worker, Await, Hook (web/time) |
| **B** | Leg 1 only, with children | Leg1 3-step (`executeLeg1StepProtocol`) | `verifyLeg1Entry` | Signal, Hook (passthrough), Interrupt (another) |
| **C** | Leg 1 only, no children | None | `verifyEntry` | Cycle, Interrupt (self) |
| **D** | Root | None (direct transition) | N/A (creates job) | Trigger |

### Why the categories matter

The category determines:

1. **Which ledger increments occur** and when they are bundled.
2. **Whether a GUID ledger is created** for crash-safe step resume.
3. **Whether the job semaphore is updated** via the compound primitive or directly.

Category A activities defer semaphore updates to Leg 2. Category B activities perform semaphore updates in Leg 1 using the same 3-step protocol. Category C activities never touch the semaphore (they either redirect to an ancestor or decrement status directly). Category D (trigger) initializes the semaphore.

### Shared pattern: Worker and Await

Worker and Await are structurally identical. Both represent a function invocation with a request (Leg 1) and a response (Leg 2). The only difference is message type: Worker publishes to a worker topic; Await publishes an await message to a subscriber topic. Their ledger behavior, entry methods, and step protocols are the same.

---

## 20) Implementation by Activity Type

This section defines the concrete leg structure, phases, and monotonic tracking for each activity type. Every ledger increment described here is bundled in the same atomic transaction as its associated durable work, per the [core principle](#core-principle-transaction-bundling).

---

### 20.1 Trigger

**Category D** | `services/activities/trigger.ts`

The root activity. Creates a new job, initializes the semaphore, and spawns the first generation of children. Uses a 3-step GUID-backed protocol for crash-safe exactly-once execution.

#### 3-Step Protocol

| Step | Action | GUID Marker | Transaction |
| ---- | ------ | ----------- | ----------- |
| 1 | **Inception**: `setStateNX` (conditional job creation) + GUID seed | `+10B` (pos 5) | Bundled |
| 2 | **Work**: `setState` + `setStats` + publish children | `+1B` (pos 6) | Bundled |
| 3 | **Completion**: `runJobCompletionTasks` (only if job immediately complete) | `+100M` (pos 7) | Bundled |

#### Step 1: Inception

Job creation and GUID ownership assertion are bundled in the same transaction. The GUID STEP1_WORK marker (+10B) is written atomically with the `setStateNX` insert.

After `exec()`, the results distinguish three cases:

| `hsetnx` result | GUID value | Meaning |
|-----------------|------------|---------|
| 1 (created) | = 10B | **New job** — proceed to Step 2 |
| 0 (existed) | > 10B | **Crash recovery** — GUID was seeded before; load `guidLedger`, resume |
| 0 (existed) | = 10B | **True duplicate** — another process owns this job; return silently |

This eliminates the prior crash gap where a job could be created (`setStateNX`) but never have its GUID ownership proof written.

#### Step 2: Work

State writing, stats, and child publication are bundled in a single transaction with the GUID STEP2_SPAWN marker (+1B). On crash recovery, `isGuidStep2Done(guidLedger)` skips this step if already committed.

Children are published in this transaction (not in a separate `transition()` call), eliminating the gap where state was written but children were never published.

#### Step 3: Completion

Only executes when the job is immediately complete (`initialStatus <= 0`, or `shouldEmit`/`shouldPersistJob`). Bundles `runJobCompletionTasks` with the GUID STEP3_CLEANUP marker (+100M).

#### Activity Ledger

Trigger pre-seeds its activity ledger to a completed state:

```
101100000000001
| ||          |
| ||          +-- Leg2 entry = 1 (pre-seeded)
| |+------------- Leg1 complete (+100B)
| +-------------- Leg1 entry = 1 (+1T)
+---------------- Authorized (seed, +100T)
```

#### GUID Ledger (per trigger message)

```
000010000000000    after Step 1 (inception)
000011000000000    after Step 2 (work + children)
000011100000000    after Step 3 (completion, if applicable)
    ||+----------- Step 3: completion tasks (+100M, pos 7)
    |+------------ Step 2: work done (+1B, pos 6)
    +------------- Step 1: inception done (+10B, pos 5)
```

#### Semaphore

The trigger sets the job semaphore to the count of adjacent children. Each child is an open obligation. As children complete, the semaphore decrements toward the threshold (default: `0`).

---

### 20.2 Worker

**Category A** | `services/activities/worker.ts`

Delegates work to an external worker topic. Leg 1 publishes the work request; Leg 2 processes the response.

#### Leg 1: Publish Work Request

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyEntry()`: load state, assert job active, increment attempt counter | Activity `+1T` | Unbundled (entry marker only) |
| Work | Map input data, publish worker message to topic, set status = 0 | Activity `+100B` (Leg1 complete) | Bundled with message publish + setState |

After Leg 1 commits, the activity is waiting for the worker to respond. The semaphore is unchanged (`setStatus(0)` is a no-op delta).

**Activity ledger after Leg 1:**

```
001100000000000
```

#### Leg 2: Process Worker Response

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyReentry()`: assert Leg1 complete, increment Leg2 counter, seed GUID ledger | Activity `+1`, GUID seeded with ordinal | Bundled |
| Step 1 | Save response data (`setState`) | GUID `+10B` | Bundled with setState |
| Step 2 | Spawn adjacent children, update semaphore via compound primitive, capture job-closed snapshot | GUID `+1B`, GUID `+100B` (if threshold hit) | Bundled (compound) |
| Step 3 | Run job completion tasks (if this GUID closed the job) | GUID `+100M` | Bundled with completion writes |
| Finalize | Mark activity finalized (sets position 1 to 2, blocks new Leg2 GUIDs) | Activity `+200T` | Bundled with Step 3 |

**Activity ledger after Leg 2 (typical, non-closing, finalized):**

```
201100000000001
| ||          |
| ||          +-- Leg2 entry count = 1
| |+------------- Leg1 complete (+100B)
| +-------------- Leg1 entry count = 1 (+1T)
+---------------- Finalized (pos 1 = 2: +200T)
```

Position 1 encodes finalization. Finalize sets it to `2`, closing the activity to new Leg2 GUIDs. A value of `2` at position 1 means this activity ran job completion tasks and took responsibility for the edge event. Positions 2-3 preserve the entry count (here `01` = 1 attempt). Step markers do not appear on the activity ledger — they are tracked on the GUID ledger only.

**GUID ledger after full processing (non-closing):**

```
000011000000001
    ||        |
    ||        +-- Ordinal (seeded at entry)
    |+------------ Children spawned (Step 2, +1B)
    +------------- Work done (Step 1, +10B)
```

No job-closed snapshot (`100B`) and no Step 3 marker (`100M`) because this GUID did not close the job.

#### Crash Recovery

If the server crashes between steps, the GUID ledger drives resume:

* Step 1 marker (`10B`) set -> skip Step 1, execute Step 2
* Steps 1+2 markers set, no job-closed snapshot -> skip all (no Step 3 needed)
* Steps 1+2 markers set, job-closed snapshot set -> execute Step 3

---

### 20.3 Await

**Category A** | `services/activities/await.ts`

Publishes an await message to a subscriber topic and waits for a response. Structurally identical to Worker.

#### Leg 1: Publish Await Message

Same as [Worker Leg 1](#leg-1-publish-work-request), except:

* Publishes an `AWAIT` message type (not a worker message)
* Topic is resolved from `config.subtype`
* Supports `config.await = false` for non-awaiting patterns (parent receives job ID immediately)
* Supports `config.retry` for stream-level retry policies

#### Leg 2: Process Response

Same as [Worker Leg 2](#leg-2-process-worker-response). The 3-step protocol, ledger increments, and crash recovery are identical.

#### Key Difference from Worker

The await message can carry a `StreamDataType.AWAIT` type marker and an optional `await: false` flag. When `await` is false, the spawned child job immediately sends a confirmation message back to the parent without waiting for the child to complete. The ledger behavior is unchanged.

---

### 20.4 Hook

**Category A or B** (runtime-determined) | `services/activities/hook.ts`

The hook activity supports three distinct patterns, determined at runtime by evaluating `config.sleep` and `config.hook.topic`:

| Variant | Condition | Category | Legs |
| ------- | --------- | -------- | ---- |
| **Time hook** (sleep) | `config.sleep` resolves to a positive number | A | Leg 1 + Leg 2 |
| **Web hook** (signal) | `config.hook.topic` is set | A | Leg 1 + Leg 2 |
| **Passthrough** | Neither sleep nor hook topic | B | Leg 1 only |

All three variants enter through `verifyLeg1Entry()`, which provides GUID-ledger-backed crash recovery regardless of which path is taken.

#### Time Hook (Sleep)

Registers a timer that fires after a configured duration.

**Leg 1:**

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyLeg1Entry()`: load state, assert job active, increment attempt counter | Activity `+1T` | Unbundled |
| Work | `registerTimeHook()`: schedule timer, `setState`, set status = 0, mark Leg1 complete | Activity `+100B` | Single transaction |

The timer registration, state writes, status update, and Leg1 completion marker all commit in a single atomic transaction. The activity is now sleeping. The timer service will publish a wake message after the duration elapses.

**Leg 2** (via `processTimeHookEvent`):

Same 3-step protocol as [Worker Leg 2](#leg-2-process-worker-response). The timer wake message is processed as a standard Leg 2 event.

#### Web Hook (Signal)

Registers a webhook subscription and waits for an external callback.

**Leg 1:**

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyLeg1Entry()` | Activity `+1T` | Unbundled |
| Work | `registerWebHook()`: register subscription on `config.hook.topic` | Activity `+100B` | Bundled with setState + status |

The activity is now waiting for an external signal. When a signal arrives on the registered topic, the task service resolves the waiting job and publishes a wake message.

**Leg 2** (via `processWebHookEvent`):

Same 3-step protocol as Worker Leg 2. After processing, the webhook registration is deleted (for code 200) or kept alive (for code 202).

#### Passthrough

No hook is registered. The activity transitions directly to its children using the Leg 1 step protocol. This is the form used as a **goto target for cycle activities** (when `config.cycle = true`, the cycle activity publishes a message to this hook's ancestor, which re-enters and runs the passthrough path).

**Leg 1:**

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyLeg1Entry()`: load state, seed GUID ledger | Activity `+1T`, GUID seeded | Unbundled |
| Step A | Map output/job data, `setState`, mark Leg1 complete | GUID `+10B`, Activity `+100B` | Bundled |
| Step B | Spawn adjacent children, update semaphore, capture edge | GUID `+1B`, GUID `+100B` (if threshold hit) | Bundled (compound) |
| Step C | Run job completion tasks (if this transition closed the job) | GUID `+100M`, Activity `+200T` (finalize) | Bundled |

The passthrough uses `executeLeg1StepProtocol()`, which is the same 3-step protocol as Leg 2 but executed during Leg 1. The GUID ledger is seeded at entry so crash recovery works identically.

#### Cycle Re-entry

When a hook activity has `config.cycle = true` and receives a cycle message, `verifyLeg1Entry()` detects that Leg 1 is already complete (the `100B` marker is set from the original execution). Instead of rejecting the message as a duplicate, it increments the Leg 2 entry counter via `notarizeLeg2Entry()` to derive a new dimensional index. The passthrough then runs in this new dimensional plane, spawning children into a fresh thread.

---

### 20.5 Signal

**Category B** | `services/activities/signal.ts`

Sends signals to one or many paused jobs, then spawns its own children. Signal is always Leg 1 only with children.

| Variant | Condition | Signal Delivery | Transactional |
| ------- | --------- | --------------- | ------------- |
| **hookOne** | `config.subtype !== 'all'` | Single target job | Yes (bundled with Leg1 completion) |
| **hookAll** | `config.subtype === 'all'` | All matching jobs by key | No (best-effort, multi-step) |

#### Leg 1

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyLeg1Entry()`: load state, seed GUID ledger | Activity `+1T`, GUID seeded | Unbundled |
| Step A | Send signal (hookOne or hookAll), `setState`, mark Leg1 complete | GUID `+10B`, Activity `+100B` | Bundled (hookOne is in txn; hookAll precedes txn) |
| Step B | Spawn adjacent children, update semaphore, capture edge | GUID `+1B`, GUID `+100B` (if threshold hit) | Bundled (compound) |
| Step C | Run job completion tasks (if edge) | GUID `+100M`, Activity `+200T` | Bundled |

#### hookOne

Resolves the target topic from `config.topic`, maps signal data, and calls `engine.hook()` within the Step A transaction. The signal delivery and the Leg 1 completion marker commit atomically: if the transaction commits, both the signal and the marker are durable. If it rolls back, neither occurred.

#### hookAll

Maps signal data and key resolver data, queries all jobs matching `key_name:key_value`, and resumes each with the signal payload. Because this involves multiple independent job updates, it executes **before** the Step A transaction as a best-effort operation. The subsequent Step A transaction then commits the Leg 1 completion marker and state. If `config.scrub = true`, the index is self-cleaned after use.

---

### 20.6 Cycle

**Category C** | `services/activities/cycle.ts`

Re-executes an ancestor activity in a new dimensional thread. Cycle is Leg 1 only with no children of its own; instead, it publishes a message to the ancestor activity.

#### Leg 1

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyEntry()`: load state, assert job active | Activity `+1T` | Unbundled |
| Work + Cycle | Map input data, `setState`, set status = 0, `cycleAncestorActivity()` (publish message to ancestor), mark Leg1 complete | Activity `+100B` + Activity `+1` (Leg2 pre-seed) | Single transaction |

All durable Leg 1 work — state writes, status, ancestor message publication, and the Leg1 completion marker — commits in a single atomic transaction. If the transaction commits, the cycle message was published and the activity is provably complete. If not, nothing happened.

#### Dimensional Re-entry

The cycle resolves the ancestor's position in the graph and computes a new dimensional address:

1. The current DAD (e.g., `,0,1,0,2`) is trimmed to the ancestor's depth
2. A new index `,0` is appended
3. The ancestor receives a message with this new DAD

The ancestor (typically a hook with `cycle = true`) detects that Leg 1 is already complete and increments its Leg 2 counter, placing all subsequent children in a fresh dimensional plane.

#### Leg2 Pre-seed

`notarizeLeg1Completion()` for cycle activities increments both `+100B` (Leg1 complete) and `+1` (Leg2 entry). This pre-seed ensures the first real Leg 2 entry gets `dimensionalIndex = 1` instead of `0`, guaranteeing the new cycle iteration runs in a distinct dimensional thread from the original.

**Activity ledger after Leg 1:**

```
001100000000001
| ||          |
| ||          +-- Leg2 entry = 1 (pre-seeded for next dimension)
| |+------------- Leg1 complete (+100B)
| +-------------- Leg1 entry = 1 (+1T)
+---------------- Active (pos 1 = 0, not finalized)
```

#### Input Remapping

Cycle activities support `config.input.maps`, allowing the re-invoked ancestor to receive different input data than the original invocation. This enables retry-with-modification patterns.

---

### 20.7 Interrupt

**Category B or C** (determined by `config.target`) | `services/activities/interrupt.ts`

Interrupts a job with an error state. The behavior depends on whether the interrupt targets the current job or another job.

| Variant | Condition | Category | Legs |
| ------- | --------- | -------- | ---- |
| **Self-interrupt** | `config.target` is empty or resolves to current job ID | C | Leg 1 only, no children |
| **Interrupt another** | `config.target` resolves to a different job ID | B | Leg 1 only, with children |

#### Self-Interrupt (Category C)

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyEntry()`: load state, assert job active | Activity `+1T` | Unbundled |
| Proof | Map job data, `setState` (if configured), mark Leg1 complete, set status = -1 | Activity `+100B` | Single transaction |
| Interrupt | Send interrupt message to own topic (best-effort, fires after proof) | None | Own call |

All durable Leg 1 work — state writes (if configured), Leg1 completion marker, and semaphore decrement — commits in a single atomic transaction. The interrupt message fires **after** the proof transaction commits, ensuring the ledger records the activity as complete before the interrupt propagates. Self-interrupt sets `status = -1`, which decrements the semaphore by 1. This does not use the compound primitive; the activity simply exits.

Configuration options:

* `config.reason`: error message
* `config.throw`: whether to throw (default: true)
* `config.code`: HTTP error code (default: 410)
* `config.descend`: interrupt child jobs too (default: false)

#### Interrupt Another (Category B)

| Phase | Action | Ledger Increment | Transaction |
| ----- | ------ | ---------------- | ----------- |
| Entry | `verifyLeg1Entry()`: load state, seed GUID ledger | Activity `+1T`, GUID seeded | Unbundled |
| Interrupt | Send interrupt message to target job (best-effort, before step protocol) | None | Own call |
| Step A | Map output/job data, `setState`, mark Leg1 complete | GUID `+10B`, Activity `+100B` | Bundled |
| Step B | Spawn adjacent children, update semaphore, capture edge | GUID `+1B`, GUID `+100B` (if threshold hit) | Bundled (compound) |
| Step C | Run job completion tasks (if edge) | GUID `+100M`, Activity `+200T` | Bundled |

The interrupt message to the target job fires before the step protocol. This is best-effort: the interrupt is sent, then the activity's own step protocol handles semaphore accounting for the current job. The target job's interruption is independent of the current job's ledger state.

---

## 21) Parent/Child Transitions and Dimensional Addressing

### Dimensional Address (DAD)

Every activity executes in a dimensional plane identified by its **DAD** (Dimensional Address). The DAD is a comma-separated path that encodes the activity's position in the execution tree:

```
,<trigger_index>,<child1_index>,<child2_index>,...
```

All activities start at index `0` in their dimension. Cycle re-entry increments the index to create a new plane.

### Examples

| Activity | DAD | Meaning |
| -------- | --- | ------- |
| Trigger | `,0` | Root, first (only) dimension |
| First child of trigger | `,0,0` | One level deep, first dimension |
| Second-level child | `,0,0,0` | Two levels deep, first dimension |
| Cycle re-entry of child at depth 1 | `,0,1` | Same depth, second dimension |
| Children of re-entered activity | `,0,1,0` | Fresh plane under new dimension |

### How Children Receive Their DAD

When an activity spawns children in Step 2:

1. The parent resolves its own DAD (accounting for cycle index via `resolveDad()`)
2. The parent appends `,0` to create the **adjacent DAD** (via `resolveAdjacentDad()`)
3. All children receive this adjacent DAD in their stream metadata
4. Each child's own children will extend the DAD by one more level

### How Cycle Changes the DAD

When a cycle activity targets an ancestor:

1. The current DAD (e.g., `,0,0,1,0`) is trimmed to the ancestor's position in the graph
2. The last segment is replaced with `,0` (reset for new dimension)
3. The ancestor re-enters with this new DAD
4. The ancestor's Leg 2 counter increments, producing a `dimensionalIndex > 0`
5. `resolveDad()` replaces the trailing `,0` with the new index (e.g., `,0,1`)
6. All children of this re-entry run under the new dimensional prefix

### Dimensional Isolation

Each dimensional address maps to an independent state namespace. Activity state stored under DAD `,0,0` is invisible to state stored under DAD `,0,1`. This ensures:

* Cycle iterations do not overwrite prior iteration state
* Parallel branches in the graph execute in isolated namespaces
* The ledger tracks each dimension independently via the `getDimensionsById()` mapping

### Semaphore Accounting Across Generations

The semaphore tracks all open obligations regardless of dimensional depth:

1. **Trigger** initializes semaphore = number of direct children
2. **Each completing activity** applies `delta = (N - 1)` where N = children spawned
3. **Cycle re-entries** do not change the semaphore directly; the cycle activity sets `status = 0` (no-op), and the re-entered ancestor's step protocol handles its own children
4. **The job completes** when the semaphore reaches the threshold (default: `0`), meaning all activities across all dimensions have completed

---

## Summary

This collation strategy provides a durable, monotonic, overflow-safe ledger for distributed transaction correctness:

* **15-digit activity ledger** tracks lifecycle (entry attempts, leg completion, finalization) and dimensional indexing
* **15-digit GUID ledger** tracks per-message uniqueness, step-level resume, and crash-safe job closure responsibility
* **Job semaphore** guarantees correct job closure detection and exactly-once completion tasks
* **Transaction-bundled digit increments** provide exactly-once proofs for each durable step
* **Four activity categories** (A through D) map each activity type to a precise combination of legs, step protocols, and ledger increments
* **Dimensional addressing** isolates cycle iterations and parallel branches into independent state namespaces
* **High retry and cycle capacity** ensure long-running, reentrant workflows remain correct and forward-progressing under arbitrary failures

Nothing is ledgered without a supporting proof in the same transaction. Nothing is proven without a durable commit. The monotonic ledger is the single source of truth for what has occurred, and the step protocol guarantees that every activity converges to completion regardless of crashes, retries, or message replays.
