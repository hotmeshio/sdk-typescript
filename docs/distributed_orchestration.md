# Orchestrating Multidimensional Workflows in a Distributed System
Orchestrating multidimensional workflows within a distributed environment, devoid of a central controlling entity, presents a unique set of challenges. These challenges intensify when managing long-running or human-mediated tasks, necessitating high levels of consistency and efficiency. This document outlines an approach designed to surmount these hurdles through leveraging key architectural patterns and principles.

At the heart of the solution is a mechanism that decomposes actions into asynchronous units, aptly suited to manage long-running processes. It features Event-Condition-Action (ECA) units, a pillar of event-driven architectures, and incorporates the power of Enterprise Application Integration (EAI) for orchestrating cross-system data exchange.

To ensure optimal manageability and efficiency, the approach leverages the Command-Query Responsibility Segregation (CQRS) pattern, fostering a self-perpetuating system that runs without the need for a central controller.

The ultimate goal of this approach is to provide a robust, efficient, and reliable method for orchestrating multidimensional workflows, facilitating smooth operation in the demanding context of a distributed environment.

## Table of Contents
1. [Understanding Asynchronous Activities in Workflow Systems](#understanding-asynchronous-activities-in-workflow-systems)
2. [Event-Condition-Action: The Computational Unit for Event-Driven Architectures](#event-condition-action-the-computational-unit-for-event-driven-architectures)
3. [Enabling Duplexing for Long-Running Business Processes](#enabling-duplexing-for-long-running-business-processes)
4. [From ECA Units to Meaningful Business Processes: The Role of Enterprise Application Integration](#from-eca-units-to-meaningful-business-processes-the-role-of-enterprise-application-integration)
5. [Leveraging CQRS to Enable Self-Perpetuation](#leveraging-cqrs-to-enable-self-perpetuation)
6. [Orchestrate Jobs through Shared Semaphore State](#orchestrate-jobs-through-shared-semaphore-state)
7. [Orchestrate Activities through Shared Collation State](#orchestrate-activities-through-shared-collation-state)
8. [Conclusion](#conclusion)

## Understanding Asynchronous Activities in Workflow Systems
Asynchronous activities are integral components of systems dealing with multiple independent processes, especially within a workflow system. Asynchronous operations afford the advantage of non-blocking execution, meaning that multiple tasks can progress simultaneously, each without the necessity of waiting for others to complete. Typical instances of asynchronous activities could involve issuing a request to a database, invoking a third-party service, or conducting a computation-heavy operation.

To better understand, consider the differences between synchronous and asynchronous executions. In a *synchronous* execution model, tasks are executed sequentially. For instance, if you had three tasks (`A`, `B`, and `C`), they would be performed one after another:

```
A --> B --> C
```

This flow signifies that task `B` can't start until task `A` is finished, and task `C` waits for task `B` to complete before starting. Each task is a blocking operation for the next one.

In contrast, an *asynchronous* system permits tasks to initiate independently of each other. They might begin in sequence, but they can progress without waiting for their predecessor to complete:

```
A  |  B  |  C
```

In this scenario, tasks `A`, `B`, and `C` are started almost simultaneously and proceed in parallel. The vertical bars (`|`) denote the independence of the tasks from each other. They are not waiting for the preceding task to complete before moving forward, thus exhibiting non-blocking behavior.

This independence and parallelism inherently presented in asynchronous operations introduce a core challenge for distributed orchestration systems: how to ensure that the final result of the workflow accurately and efficiently reflects the outcomes of all completed tasks. The resolution of this challenge calls for strategies that not only manage the orchestration of asynchronous operations but also accurately consolidate the results to drive subsequent processes. The forthcoming sections of this document detail such a strategy.

## Event-Condition-Action: The Computational Unit for Event-Driven Architectures

The essential computational unit for event-driven architectures is the Event-Condition-Action (ECA) pattern. This pattern is widely acknowledged for its proficiency in managing diverse workloads efficiently, rendering a high level of performance and flexibility.

Let's dissect the ECA pattern:

```
On EVENT:
  If CONDITION:
    Execute ACTION
```

In this pattern, an **Event** triggers the computational unit, a **Condition** then verifies whether the execution should proceed, and finally, an **Action** is performed if the condition is satisfied.

Within the scope of a workflow system, these constituents can be interpreted as follows: The **Event** could denote the completion of a preceding task in a workflow (or of the preceding leg in a multi-leg activity); the **Condition** might represent the *successful* completion of that preceding task (or a set of tasks), signifying that the operational preconditions for the next task have been fulfilled; and the **Action** would correspond to the initiation of the subsequent task (or leg) in the workflow.

This ECA pattern aligns harmoniously with the distributed, asynchronous nature of event-driven architectures, thereby offering an effective means of managing tasks and their dependencies. However, the pattern, in its conventional form, falls short when confronted with the complexities of long-running business processes, especially those that necessitate human intervention, such as reviews and approvals. These processes require the data exchange to be duplexed to achieve the flexibility required in handling prolonged or interruptible tasks.

The following section elaborates on this aspect and introduces a strategy for enabling duplexing to accommodate long-running business processes within the ECA model's constraints.

## Enabling Duplexing for Long-Running Business Processes
The conventional ECA (Event-Condition-Action) model treats the *Action* as a single atomic operation, primarily because it does not inherently support state retention. Therefore, in order to handle long-running business processes and ensure uninterrupted data exchange, it becomes necessary to divide the *Action* into two distinct components. This division forms the basis for a full-duplex system, where each activity comprises two legs, "beginning" and "conclusion," bridged by an asynchronous wait state. Importantly, this transformation adheres to the fundamental principles of ECA by giving rise to two distinct ECA sequences for initiating and concluding the activity.

The duplexing principle is fundamental to the operation of the engine (the quorum), which interprets an activity's execution as two interconnected yet standalone actions. The following pseudo-code representation provides an insight into the engine's role in processing an activity:

```
On EVENT (PARENT ACTIVITY COMPLETED):
  If CONDITION:
    EXECUTE ACTION-BEGIN (Duplex Leg 1)

--------------- EXTERNAL SYSTEM PROCESSING ----------------

On EVENT (WORKER COMPLETED):
  If CONDITION (JOB STILL ACTIVE):
    EXECUTE ACTION-END (Duplex Leg 2)
```

In this context, **ACTION BEGIN** marks the commencement of a process, such as dispatching a request or launching a long-running operation. **EXTERNAL SYSTEM PROCESSING** symbolizes the asynchronous event that the engine awaits, like user approval or the completion of a complex calculation. Upon fulfilling this condition, **ACTION END** is executed, finalizing the results.

Importantly, this dual-action approach spawns a seemingly perpetual chain of activities. The engine consistently finds itself processing either the concluding leg of a previous activity or the initiating leg of the subsequent one. This method of duplexing serves as the linchpin in accomplishing fluid, responsive, and efficient orchestration of long-running processes in a distributed system. It adheres to the ECA pattern, restricts the execution scope to one unit at a time, and critically, allows the system to maintain high throughput by optimally managing its computational resources.

And from a data modeling standpoint, the resulting information flow is cabaple of modeling repeated inputs into leg 2, allowing for signaling patterns crucial to building a robust reentrant process engine.

## From ECA Units to Meaningful Business Processes: The Role of Enterprise Application Integration
The transformation of isolated event-driven operations, or ECA units, into cohesive business processes calls for an intermediary abstraction layer to direct and synchronize these individual units. Enterprise Application Integration (EAI) plays this pivotal role, acting as a crucial orchestrator.

EAI serves as a principal scheme for unification, amalgamating separate ECA units into a comprehensive network of business processes. It describes the rules for data exchange among these units, fostering their collective participation in executing complex workflows that span across varied services and subsystems. EAI ensures that the transmitted data complies with predetermined schemas and data types, thereby enhancing interoperability and ensuring data consistency across the distributed system.

## Leveraging CQRS to Enable Self-Perpetuation
Command Query Responsibility Segregation (CQRS) plays a pivotal role in delivering *operational continuity*. CQRS fundamentally decouples the 'write' operations (commands) from the 'read' operations (queries) in a system, thus enabling an operationally resilient and efficient environment.

Let's take a sequence of tasks: `A`, `B`, and `C`. In a conventional execution flow, the completion of `A` directly initiates `B`, which in turn sets off `C`:

```
A --> B --> C
```

This presents a chain of dependencies where the execution of one task is directly bound to its predecessor, making the system vulnerable to bottlenecks and cascading failures.

In contrast, a system exploiting the potency of CQRS introduces an element of fluidity and independent control. Instead of `A` triggering `B` directly, the completion of `A` is chronicled as an event in an append-only log data structure, a widely adopted approach in CQRS:

```
A --> log[A completed] --> B --> log[B completed] --> C
```

In this scenario, the producers (tasks) merely inscribe their completion events onto the log. Concurrently, the consumers (the triggers for ensuing tasks) read from this log. This separation is of key significance: the progression of the workflow is driven not by the producer prompting the next task directly, but by the consumer's act of reading from the log.

This dynamic begets a self-perpetuating system where workflows advance uninterruptedly through the simple act of reading from an append-only log. The progress of each task morphs into a self-propelling force for the entire workflow, thereby minimizing dependencies and creating an operationally efficient environment. To that end, CQRS grants quorum-based systems the ability to navigate the process to completion more efficiently than their control-dependent counterparts.

The CQRS strategy not only enhances the system's responsiveness and scalability but also improves its overall resilience by isolating failures. As a result, systems can continue to function and recover gracefully even when individual components encounter issues, proving CQRS to be a strategically beneficial pattern for asynchronous workflow orchestration.

## Orchestrate Jobs through Shared Semaphore State
Efficiently tracking workflow execution (i.e., "job") state is critical to asynchronous workflow systems and is accomplished through a semaphore that will count down to `0` once all activities have completed for the flow.

The semaphore is updated via increment/decrement calls to the central server each time an activity in the graph completes its full lifecycle. The value sent to the semaphore will always be the length of the adjacent activity list (the number of child activity nodes that should execute) minus `1` (the current node).

If the adjacency list has members, each child activity in the adjacency list will be journaled to its designated stream and the pattern will repeat.

If there are no adjacent children, `-1` will be sent to the server (adjacencyList.length - 1). If the incremented/decremented status result returned from the server is `0`, then the job is complete (this activity was the last of all activities to complete).


*The act of the caller saving individual state triggers a server response with full job semaphore state.*

## Orchestrate Activities Through Shared Collation State
Activity state is managed using a 15-digit integer. This value is set to `999000000000000` for triggers, the entry activity in the flow, when the flow first initializes. For all other activities, this value is initialized by the parent activity (the preceding activity) in the DAG. Presetting the value for subsequent generations is critical to establishing return receipt checks, so that the system can guarantee durability and idempotency in the result of system failure. 
The first three digits of the 15-digit integer track and convey the activity lifecycle status. Supporting 3 states using any 3 integers is the only critical requirement for the chosen integers. As long as the backend system supports decrement and increment commands that return the modified integer value, the system can durably track state using the strategy described here.  In the reference implementation, the digit `9` is “pending”; `8` is “complete”; and `7` is “error”. Additional digits can be employed to convey additional states. 

The remaining 12 digits offer 1 million distinct dimensional threads for activity expansion. Dimensional Threads isolate and track those activities in the workflow that run in a cycle. They ensure that no naming collisions occur, even if the same activity is run multiple times. Each time duplex leg 2 of an activity returns with its payload, it can traverse the primary execution tree that remains (the remaining nodes in the graph); however, if the message includes a ‘pending’ status, it means that the channel will remain open, necessitating a dimensional execution thread be added to the flow, so that subsequent incoming messages can be tracked. This pattern likewise exists for cycle activity in that every adjacent activity that follows in the DAG will be uniquely addressed using a sequential dimensional thread that reflects its location in the collection being iterated.

```
  999000000000000
  ^-------------- Leg1 Entry Status
   ^------------- Leg1 Exit Status
     ^^^^^^------ Leg2 Dimensional Thread Entry Count
           ^^^^^^ Leg2 Dimensional Thread Exit Count
    ^------------ Leg2 Exit Status
```

>Streams are used when executing an activity (such as transitioning to a child activity) as they guarantee that the child activity will be fully created and initialized before the request is marked for deletion. Even if the system has a catastrophic failure, the chain of custody can be guaranteed through the use of streams when the system comes online.

## Conclusion
Designing and orchestrating multidimensional workflows in distributed environments can present significant challenges. Nevertheless, these complexities become tractable with a thorough understanding and prudent application of key architectural principles and design patterns. 
