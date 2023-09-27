# Activity State Tracking

This document explains how HotMesh guarantees activity state management using a 15-digit integer. This mechanism is crucial for monitoring and controlling the activity's lifecycle across two legs (Leg1 and Leg2) and ensures that duplicate activities are never processed. The process further ensures that catastrophic, in-process failures will be automatically resolved with respect to idempotency, makig it possible to build 100% fault-tolerant workflows.

## Overview

The 15-digit integer serves as a semaphore, starting with the value `999000000000000`. Each digit has a specific purpose:

- The first three digits track the lifecycle status for Legs 1 and 2.
- The last 12 digits represent 1 million possible dimensional threads that can be spawned by this activity.

### Digit Details

    999000000000000
    ^-------------- Leg1 Entry Status
     ^------------- Leg1 Exit Status
       ^^^^^^------ Leg2 Dimensional Thread Entry Count
             ^^^^^^ Leg2 Dimensional Thread Exit Count
      ^------------ Leg2 Exit Status

>*Dimensional Threads* isolate and track those activities in the workflow that run in a *cycle*. They ensure that no naming collisions occur, even if the same activity is run multiple times.

## Leg1 Lifecycle

### Initialization

The parent activity initializes the integer value (`999000000000000`) and saves it to Redis upon saving its own state. The parent then adds the child activity to the proper stream channel for eventual processing.

>Streams are used when executing an activity (such as transitioning to a child activity) as they guarantee that the child activity will be fully created and initialized before the request is marked for deletion. Even if the system has a catastrophic failure, the chain of custody can be guaranteed through the use of streams when the system comes online.

### Beginning of Leg1

When Leg1 begins (when an activity is dequeued from its stream channel), the integer is decremented by 100 trillion:

    HINCRBY -100000000000000

Result:

    899000000000000

### Conclusion of Leg1

At the conclusion of Leg1, the integer is decremented by 10 trillion:

    HINCRBY -10000000000000

Result:

    889000000000000

### Error Handling

- If the value upon entering is `799############` (after decrementing the integer), Leg1 began but crashed before completion. The activity will perform the necessary cleanup and re-run the activity.
- If the value upon entering is `789############` (after decrementing the integer), Leg1 completed successfully and the activity should end. It is likely that the system crashed last time before acking and deleting. Verify transitions succeeded and resolve as necessary.

## Leg2 Lifecycle

Leg2 supports multiple inputs and repeated responses. A worker can respond with a 'pending' status, allowing multiple inputs.

### Beginning of Leg2

On the first Leg2 input, the integer is incremented by 1 million:

    HINCRBY 1000000

Result:

    889000001000000

### Pending Status

If the call status is 'pending', the digit is updated to reflect the successful completion of Leg2 but the third digit from the left will be untouched as the activity is pending and Leg2 should not end quite yet:

    HINCRBY 1

Result:

    889000001000001

### Success Status

If the next message call to Leg2 entry is 'success', the process will begin as before, and the integer will be incremented by 1 million:

    HINCRBY 1000000

Result:

    889000002000001

 But because this is not a 'pending' message, the integer should be updated differently, targeting both the dimensional thread counter as well as the third digit from the left that tracks Leg2 status (e.g., `1 - 1000000000 = -999999999`).

    HINCRBY -999999999999

Result:

    888000002000002

>If an additional Leg2 call were to be received after this point, the result would be `888000003000002`. However, becuase the third digit is `8`, Leg2 access is not allowed and the system will conclude by decrementing the value it just set and returning immediately, so that the calling worker can mark and delete the stream entry.

## Conclusion

This 15-digit integer allows sophisticated tracking of complex activities across two legs, with error handling and support for cycles using multiple dimensional threads.
