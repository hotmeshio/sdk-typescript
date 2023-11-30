# Refactoring 101: Simplifying Microservice Workflows

Consider the following microservices network. It doesn't matter what the services do, or if it's AWS, GCP or Azure. We've all encountered systems like this and the burdensome *tech debt* they represent.

<img src="./img/refactor/rf1.png" alt="Current State of the microservices network with functions" style="max-width:100%;width:600px;">

There's real value in there. Functions A, B, and C represent important company IP. But reasoning through a migration can be so challenging that often it's easier to just leave it alone or add another bandage.

### Enter Redis
As cluttered as the network might be, there's a common thread: **Redis**. If the legacy functions can access Redis, then Redis can access the legacy functions.

<img src="./img/refactor/rf2.png" alt="Redis is the common broker" style="max-width:100%;width:600px;">

From this vantage point, networking doesn't change. The network remains chaotic. But now we have a strategy to simplify things and organize the functions as activities in a business process.

<img src="./img/refactor/rf3.png" alt="HotMesh reverses the information flow" style="max-width:100%;width:600px;">

HotMesh steps in as a mediator, leveraging all 3 Redis communication channels (`streams`, `data`, and `events`) to reverse the information flow and orchestrate the activities. This inversion of control is a game-changer, simplifying refactoring by plucking and orchestrating target functions independent of the legacy network clutter.
