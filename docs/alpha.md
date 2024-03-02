# HotMesh Alpha Release

## Motivation  
I hold a patent in [Ajax and Single Page Applications](https://patents.google.com/patent/US8136109) and consider it a defining aspect of my early engineering career. I still dabble in front-ends and APIs, but the more time I've spent in cloud technologies (microservices in particular), the more I've come to perseverate on just how difficult it is to design distributed applications.

It borders on unacceptable now two decades in just how unnecessarily complex it is to reason about multi-service applications in the cloud. Anyone can deploy that initial "Hello World" cloud application using Postgres and Node. But the journey from that first deployment to something that scales across billions of events requires its own genius. How does one reason about a code base that solves both initial and eventual states? The cloud makes this easy, right? right?

But every time I sift through a company's legacy code base, it feels like most of the IP is buried in a tangle of custom, home-grown workflows. 

<img src="./img/refactor/rf1.png" alt="Current State of the microservices network with functions" style="max-width:100%;width:600px;">

There's real value in there. Functions A, B, and C represent important company IP. But it's the service-to-service interactions (retries, scaling, assymetry, back-pressure, load-balancing, dlqs, rollbacks, etc) that feels unmanageable.

The promise (at least what was pitched two decades ago) is that the cloud was about to become that "big appliance in the sky". I had hoped the cloud would one day feel like a singular, unified system. And I know I'm not alone as others have voiced a desire to solve this same issue through other means, with monolith architectures getting their day in the sun.

## Approach  
I strongly prefer *distributed* architectures and enjoy modifying traditional systems using this lens. It's what led me to stumble upon the Ajax pattern, even though the architectural standard at the time was to push HTML content using server-side engines like JSP, ASP, Cold Fusion, etc.

In early 2023 I revisited an orchestration engine I had built years ago. But I took another pass at it using the Command Query Responsibility Segregation (CQRS) pattern. It's central to event-driven systems like Kafka and is a foundational principle around their scale and reslience.

In this scenario, the producers merely inscribe their completion events onto the log. Concurrently, the consumers read from this log. This separation is of key significance: the progression of the workflow is driven not by the producer prompting the next task directly, but by the consumer's act of reading from the log. Note in the following how the Engine and Worker are decoupled from each other (and from the outside callers as well):

<img src="./img/lifecycle/self_perpetuation.png" alt="HotMesh Self-Perpetuation" style="max-width:100%;width:600px;">

This simple mechanism of reading from one stream and writing to another is the basis for the entire system and how complex workflows are achieved. Every complex workflow is simply a series of singular activities implicitly stitched together by writing to streams in a sequence.
