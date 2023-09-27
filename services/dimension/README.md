# Optimizing Cycle Management Through Shared Collation State
Activity state is managed using a 15-digit integer. For all non-trigger activities, this value is initialized by the parent activity/preceding in the DAG. (Triggers are initialized in a 'completed' state, since their execution only reflects the  completion of the Leg 2 Duplex.)

>Presetting the value for subsequent generations is critical to establishing return receipt checks, so that the system can guarantee durability and idempotency in the result of system failure during handoff.

The first three digits of the 15-digit integer track and convey the activity lifecycle status. Supporting 3 states using any 3 integers is the only critical requirement for the chosen integers. As long as the backend system supports decrement and increment commands that return the modified integer value, the system can durably track state using the strategy described here.  In the reference implementation, the digit `9` is “pending” and `8` is “complete”. Additional digits can be employed to convey additional states. 

The remaining 12 digits offer 1 million distinct dimensional threads for activity expansion. Dimensional Threads isolate and track those activities in the workflow that run in a cycle. They ensure that no naming collisions occur, even if the same activity is run multiple times. Each time duplex leg 2 of an activity returns with its payload, it can traverse the primary execution tree that remains (the remaining nodes in the graph); however, if the message includes a ‘pending’ status, it means that the channel will remain open, necessitating a dimensional execution thread be added to the flow, so that subsequent incoming messages can be tracked. This pattern likewise exists for `iterator` and `mark/goto` activities in that every adjacent activity that follows in the DAG will be uniquely addressed using a sequential dimensional thread that reflects its location in the collection being iterated.

In the following series, [S] represents interactions with the STREAM while [D] represents interactions with the backend DATA STORE. The job begins with the trigger ( the entry point for the rooted tree). Every activity runs as a duplexed exchange with the request and response channels fully decoupled. 

The trigger is unique, however, in that it does not execute Leg 1. This is because the outside caller is responsible for Leg 1, including generating the input data to be sent to the trigger to kick off the workflow.

TRIGGER LEG 2

```bash
                  [D] a  A  | SET IF UNIQUE <JOBID> (ENSURE UNIQUE JOB ID)
  888000001000001 [D] a  B  | SET TRIGGER STATE (SET TO 888000001000001)
                  [S] b  C  | [SPAWN ADJACENT (CHILD) NODES]
  999000000000000 [D] a  E  | PRESET SPAWNED ADJACENT NODE(S) ACTIVITY STATE
                         F    => RETURN JOB ID TO CALLER
WORKER ACTIVITY LEG 1
                  [S] a a  | [DEQUEUE ACTIVITY]
  999000000000000           *ACTIVITY STATUS WAS PRESET BY THE PARENT NODE*
  ^-------------- [D] b c  | UPDATE ACTIVITY STATUS (DECREMENT to 899*)
                             => FIX or CANCEL `IF < /^8\d+$/` [ACK/DELETE]
                  [S] c d  | [ENQUEUE WORKER REQUEST]
   ^------------- [D] b e  | UPDATE ACTIVITY STATUS (DECREMENT to 889*)
                  [S] a f  | [ACK/DELETE]
WORKER (SYNCHRONOUS | VARIANT 1 OF 2)
                  [S] a  g  | [DEQUEUE WORKER REQUEST]
  ^^^------------ [D] b  h  | QUERY ACTIVITY Status (CHECK IF JOB ACTIVE)
                              => CANCEL `IF != /^889\d+$/` => [ACK/DELETE]
                      X  X    => EXEC WORKER
                  [S] d  j  | [ENQUEUE WORKER RESPONSE]
                  [S] a  k  | [ACK/DELETE]
WORKER (ASYNCHRONOUS | VARIANT 2 OF 2)
                  [S] a  g  | [DEQUEUE WORKER REQUEST]
  ^^^------------ [D] b  h  | QUERY ACTIVITY Status (CHECK IF JOB ACTIVE)
                              => CANCEL `IF != /^889\d+$/` => [ACK/DELETE]
                      X  X    => SAVE JOB METADATA | START WORKER
                  [S] a  k  | [ACK/DELETE]
                              => => =>
                      X  X    => => => STOP WORKER (ALL DONE)
                  [S] d  j  | [ENQUEUE WORKER RESPONSE]
WORKER ACTIVITY LEG 2 (PROCESS PENDING | VARIANT 1 of 3)
                  [S] a  l  | [DEQUEUE WORKER RESPONSE]
     ^^^^^^------ [D] b  m  | INCREMENT DIMENSIONAL THREAD ENTRY COUNT by 1
                              => CANCEL `IF != /^889\d+$/` => [ACK/DELETE]
                  [S] c  n  | [ENQUEUE ADJACENT (CHILD) NODES]
           ^^^^^^ [D] b  o  | INCREMENT DIMENSIONAL THREAD EXIT COUNT by 1
  999000000000000 [D] b  q  | PRESET SPAWNED ADJACENT NODE(S) STATE
                  [S] a  s  | [ACK/DELETE]
WORKER ACTIVITY LEG 2 (PROCESS SUCCESS [OR CAUGHT ERROR] | VARIANT 2 of 3)
                  [S] a  l  | [DEQUEUE WORKER RESPONSE]
     ^^^^^^------ [D] b  m  | INCREMENT DIMENSIONAL THREAD ENTRY COUNT by 1
                              => CANCEL `IF != /^889\d+$/` => [ACK/DELETE]
                  [S] c  n  | [ENQUEUE ADJACENT (CHILD) NODES]
           ^^^^^^ [D] b  o  | INCREMENT DIMENSIONAL THREAD EXIT COUNT by 1
    ^------------ [D] b  p  | UPDATE ACTIVITY STATUS (DECREMENT to 888*)
  999000000000000 [D] b  q  | PRESET SPAWNED ADJACENT NODE(S) STATE
                  [S] d  r  | [ENQUEUE `JOB CLEANUP TASKS` IF JOB STATE = 0]
                  [S] a  s  | [ACK/DELETE]
WORKER ACTIVITY LEG 2 (PROCESS UNCAUGHT ERROR | VARIANT 3 of 3)
                  [S] a  l  | [DEQUEUE WORKER RESPONSE]
     ^^^^^^------ [D] b  m  | INCREMENT DIMENSIONAL THREAD ENTRY COUNT by 1
                              => CANCEL `IF != /^\d{2}9\d+$/` => [ACK/DELETE]
    ^------------ [D] b  p  | UPDATE ACTIVITY STATUS (DECREMENT to 887*)
                  [S] d  r  | [ENQUEUE `JOB CLEANUP TASKS`]
                  [S] a  s  | [ACK/DELETE]
```

Streams are used when executing an activity (such as transitioning to a child activity) as they guarantee that the child activity will be fully created and initialized before the request is marked for deletion. Even if the system has a catastrophic failure, the chain of custody can be guaranteed through the use of streams when the system comes online. If, for example, the system crashed during activity processing, the event history will reveal exactly where in the process it occurred, so that it is possible to restore state while still guaranteeing idempotency.
