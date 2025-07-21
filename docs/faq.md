# HotMesh FAQ

## What is HotMesh?
HotMesh is a wrapper for Postgres that exposes a higher level set of domain constructs like ‘activities’, ‘workflows’, 'jobs', etc.

## Are there Advantages to a Reentrant Process Architecture?
A key component of Reentrant Processes is baked-in support for retries, idempotency, and the ability to handle failures. HotMesh provides a simple, yet powerful, mechanism for handling retries and idempotency. If the execution fails, the engine will retry the activity until the retry limit is reached. If the job succeeds, the engine will transition to the next activity.

<img src="./img/lifecycle/self_perpetuation.png" alt="HotMesh Self-Perpetuation" style="max-width:100%;width:600px;">

>Code that is backed by a Reentrant Process Engine, needn't include *retry* and *timeout* logic. The engine handles all of that for you. This is a huge advantage over traditional code that must be written to handle failures. It's also a significant advantage over traditional Service Mesh architectures which lack support for durable function execution.

## What gets installed?
HotMesh is a lightweight NPM package that connects any microservice where its installed to the Postgres-backed Service Mesh. The installed modules serve as both the *Control Plane* and *Side Car* in the deployment, while Postgres serves as the *Data Plane*.

## Is HotMesh an Orchestration Hub/Bus?
Yes and No. HotMesh was designed to deliver the functionality of an orchestration server but without the additional infrastructure demands of a traditional server. Only the outcome (process orchestration) exists. Process Orchestration is an emergent property of the data journaling process as the service mesh manages the data flow.

## How does HotMesh operate without a central controller?
HotMesh is designed as a distributed quorum where each member adheres to the principles of CQRS. According to CQRS, *consumers* are instructed to read events from assigned topic queues while *producers* write to said queues. This division of labor is essential to the smooth running of the system. HotMesh leverages this principle to drive the perpetual behavior of engines and workers (along with other advantages described [here](./distributed_orchestration.md)). 

As long as their assigned topic queue has items, consumers will read exactly one item and then journal the result to another queue. As long as all consumers (engines and workers) adhere to this principle, sophisticated workflows emerge.
