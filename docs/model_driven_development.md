# Model Driven Development

The HotMesh model is a YAML-based alternative to WSDL/SOAP, offering a flexible and extensible way to define and manage distributed process execution.

## Table of Contents

- [Introduction](#1-introduction)
  - [1.1. Overview of the Model](#11-overview-of-the-model)
  - [1.2. YAML Structure](#12-yaml-structure)
  - [1.3. Key Concepts: Graphs, Activities, and Transitions](#13-key-concepts-graphs-activities-and-transitions)
- [2. Settings](#2-settings)
  - [2.1. Defining Global Settings](#21-defining-global-settings)
  - [2.2. Accessing Settings Values](#22-accessing-settings-values)
- [3. Graphs](#3-graphs)
  - [3.1. Structure of a Graph](#31-structure-of-a-graph)
  - [3.2. Subscription and Publishing Mechanism](#32-subscription-and-publishing-mechanism)
- [4. Activities](#4-activities)
  - [4.1. Types of Activities](#41-types-of-activities)
    - [4.1.1. Hook](#411-hook)
    - [4.1.2. Trigger](#412-trigger)
    - [4.1.3. Await](#413-await)
    - [4.1.4. Worker](#414-worker)
    - [4.1.5. Cycle](#415-cycle)
    - [4.1.6. Signal](#415-signal)
  - [4.2. Input and Output Schemas](#42-input-and-output-schemas)
  - [4.3. Activity Mapping](#43-activity-mapping)
- [5. Transitions](#5-transitions)
  - [5.1. Defining Transitions](#51-defining-transitions)
  - [5.2. Conditional Transitions](#52-conditional-transitions)
- [6. The '@pipes' Functional Mapper](#6-functional-mapper-pipes)
  - [6.1. Introduction to '@pipes'](#61-introduction-to-pipes)
  - [6.2. Using '@pipes' in Conditions](#62-using-pipes-in-conditions)
  - [6.3. Supported Functions](#63-supported-functions)
- [7. Stats](#7-stats)
  - [7.1. Defining Stats in Activities](#7-stats1)
  - [7.2. Aggregating Data with Stats](#72-aggregating-data-with-stats)
- [8. Example Walkthrough](#8-example-walkthrough)
  - [8.1. Order Approval Process](#81-order-approval-process)
  - [8.2. Price Approval Process](#82-price-approval-process)

## 1. Introduction
When engineering solutions for critical business processes, it is essential to adopt a cross-functional approach where *all* stakeholders (engineering, operations, product, etc)concentrate on the core models and high-level design. One of the primary benefits of model-driven development lies in its ability to focus on the application's overall design and logic rather than becoming entangled in implementation specifics. Consequently, this leads to accelerated development cycles, as the required skill to design a workflow is a logical understanding of the business process and how to move it forward, not how to express it in code.

### 1.1. Overview of the Model
HotMesh utilizes an *execution-oriented* model to define an application's behavior and data flow. The approach is designed to be flexible, modular, and easy to understand.

A standard `graph` serves as the foundational building block--essentially a collection of activities (nodes) connected by transitions (edges). Each activity represents a specific task or operation, while transitions define the flow between these activities based on certain conditions. This structure allows the model to accommodate complex workflows and processes that span multiple services and systems.

One of the key features of this model is the functional mapper called '@pipes', which provides an expressive and versatile way to manipulate and transform data between activities. Using '@pipes', developers can create complex data mappings and conditional flows without the need for extensive scripting or programming.

The model is highly adaptable and can be used to define a wide range of applications, from simple data processing tasks to sophisticated business processes. From a cost of ownership perspective, it represents a more maintainable, scalable, and efficient approach.

### 1.2. YAML Structure
The model is represented as a JSON object by the engine once compiled (although YAML can be interchangeably used for authoring given its better readability). The structure is comprised of various nested properties that define the application's settings, and execution behavior. The primary components of the JSON model are as follows:

- `app`: The top-level object that contains the application's metadata, settings, and graphs.
  - `id`: A unique identifier for the application.
  - `version`: The version number of the application.
  - `settings`: An optional object that contains global settings and configuration values that can be accessed throughout the application.
  - `graphs`: An array of graph objects that define the application's logic and data flow.
    - `subscribes`: The event or message that the graph listens to, which triggers the graph's execution.
    - `publishes`: The event or message that the graph emits upon completion.
    - `input`: The input data model.
    - `output`: The output data model.
    - `activities`: An object that contains the activities (nodes) within the graph. Each activity has a unique key and consists of properties such as title, type, input, output, and job.
    - `transitions`: An object that defines the flow between activities based on specified conditions. Each key in the transitions object corresponds to an activity key, and its value is an array of transition objects.
    - `hooks`: An event or message that a sleeping activity is subscribed to, which will awaken it
- `@pipes`: A functional mapper that allows for data transformation and manipulation between activities using various functions.

The JSON structure of the model is designed to be easy to read and modify, allowing developers to understand the application's logic and behavior at a glance. By using a JSON-based format, the model can be easily shared, versioned, and integrated into various programming languages and platforms.

### 1.3. Key Concepts: Graphs, Activities, and Transitions
This section will introduce the key concepts of the model, including graphs, activities, and transitions. We will use YAML for better readability while discussing specific parts of the model.

`Graphs`: A graph represents a workflow or a sequence of activities and their transitions. It serves as a container for the activities and defines the overall flow and logic of the application. Each graph subscribes to an event or message and publishes its result upon completion. Here's an example of a graph:

```yaml
graphs:
  - subscribes: order.approval.requested
    publishes: order.approval.responded
    activities: {...}
    transitions: {...}
```

`Activities`: Activities are the individual tasks or operations performed within a graph. Each activity has a unique key and can be of different types, such as trigger, await, or job. Activities have input and output schemas, which define the data structure they expect and produce. Triggers are the one exception to this rule and reference the `input` schema for the overall job as their input/output schema:

```yaml
input:
  schema:
    type: object
    properties:
      id:
        type: string
        description: The unique identifier for the object.
      price:
        type: number
        description: The price of the item.
        minimum: 0
      object_type:
        type: string
        description: The type of the order (e.g, widgetA, widgetB)
output:
  schema: {...}

activities:
  a1:
    title: Get Approval
    type: trigger
```

`Transitions`: Transitions define the flow between activities based on certain conditions. They connect the activities within a graph and determine the path of execution. Transitions can be conditional, which means they depend on the output of previous activities. The conditions property within a transition allows for matching expected values against actual values, with the option to use the '@pipes' functional mapper for data manipulation. Here's an example of a transition:

```yaml
transitions:
  a1:
    - to: a2
  a2:
    - to: a3
      conditions:
        match:
          - expected: true
            actual: "{a2.output.data.approved}"
    - to: a4
      conditions:
        match:
          - expected: false
            actual: "{a2.output.data.approved}"
```

Understanding these key concepts is essential for working with the model, as they form the foundation of the application's logic and data flow. By defining graphs, activities, and transitions, developers can create complex workflows and processes that are modular and maintainable.

## 2. Settings
### 2.1. Defining Global Settings
Global settings are an important aspect of the application model, as they allow developers to define and manage shared configuration values that can be used throughout the application. This promotes maintainability and consistency in your application by centralizing configuration values and reducing the need for hardcoded values within activities and transitions.

To define global settings, include a settings object in the app object of the JSON model. The settings object can contain any number of key-value pairs, with nested objects allowed for more complex configurations.

Here's an example of defining global settings in YAML (note that the `version` field is a string, in support of semantic versioning `0.0.1`):

```yaml
app:
  id: myapp
  version: '1'
  settings:
    some_boolean: true
    some:
      nested:
        string: hello
        integer: 2
```

In this example, a boolean setting 'some_boolean' is defined with the value `true`, and a nested object 'some' contains a 'nested' object with a string setting 'string' set to 'hello', and an integer setting 'integer' set to 2.

Defining global settings in this manner ensures that they are easily accessible and modifiable, promoting maintainability and adaptability as your application evolves.

### 2.2. Accessing Settings Values

Once global settings are defined within the app object, they can be accessed and used throughout the application in activities, transitions, and conditions. This is achieved by referencing the settings values using the {app.settings.*} syntax.

To access a global settings value, use the {app.settings.*} placeholder, replacing the asterisk (*) with the key or path of the desired setting. For nested objects, use dot notation to specify the path to the desired value.

Here's an example of accessing settings values in YAML:

```yaml
app:
  id: myapp
  version: 1
  settings:
    some_boolean: true
    some:
      nested:
        string: hello
        integer: 2

graphs:
  - activities:
      a1:
        title: "Example Activity"
        type: "activity"
        job:
          maps:
            setting_boolean: "{$app.settings.some_boolean}"
            setting_string: "{$app.settings.some.nested.string}"
            setting_integer: "{$app.settings.some.nested.integer}"
```

In this example, the global settings values are accessed and mapped to the properties of the 'job' object in activity 'a1'. The 'setting_boolean' property is assigned the value of the 'some_boolean' setting, while 'setting_string' and 'setting_integer' are assigned the values of the 'string' and 'integer' settings within the nested 'some.nested' object.

By accessing global settings values in this manner, developers can promote maintainability and consistency throughout their application, ensuring that configuration values can be easily managed and updated as needed.

## 3. Graphs
### 3.1. Structure of a Graph

A graph is a key component of the application model, representing a collection of activities and transitions that define a specific workflow or process within the application. Graphs allow developers to model complex processes, manage dependencies between activities, and handle various application events.

The structure of a graph (aka `flow`) is as follows:

1. `subscribes`: The event that the graph subscribes to in order to start its execution.
2. `publishes` (optional): The event that the graph publishes when it completes its execution.
3. `input` (optional): The schema for the data the graph expects on start.
4. `output` (optional): The schema for the data the graph produces on completion.
5. `activities`: An object containing the activities (nodes) that make up the graph. Each activity has a unique identifier and represents a specific task or operation within the workflow.
6. `transitions`: An object containing the transitions (edges) between activities. Transitions define the order in which activities are executed and can include conditions that determine the execution path.

Here's an example of a graph structure in YAML:

```yaml
graphs:
  - subscribes: order.approval.requested
    publishes: order.approval.responded
    input:
      schema:
        $ref: '../schemas/order.approval.requested.yaml#/input'
    output:
      schema:
        $ref: '../schemas/order.approval.requested.yaml#/output'

    activities:
      a1:
        title: "Get Approval"
        type: "trigger"
        # ... (activity properties)
      a2:
        title: "Get Price Approval"
        type: "await"
        # ... (activity properties)
      a3:
        title: "Return True"
        type: "activity"
        # ... (activity properties)
      a4:
        title: "Return False"
        type: "activity"
        # ... (activity properties)
    transitions:
      a1:
        - to: a2
      a2:
        - to: a3
          conditions:
            match:
              - expected: true
                actual: "{a2.output.data.approved}"
        - to: a4
          conditions:
            match:
              - expected: false
                actual: "{a2.output.data.approved}"
```

In this example, the graph subscribes to the `order.approval.requested` event and publishes the `order.approval.responded` event. It includes four activities (a1, a2, a3, and a4) and defines transitions between them. The transitions object outlines the flow between activities, including conditional logic that determines the execution path based on the output data of previous activities.

By structuring graphs in this manner, developers can create flexible and efficient workflows that model various processes within their applications.

### 3.2. Subscription and Publishing Mechanism
The subscription and publishing mechanism in the graph represent entry and exit points for connecting to *external* event systems.

1. `subscribes`: A graph subscribes to outside triggers by specifying the topic name in the subscribes property. Outside callers can trigger the workflow by publishing a payload to this topic (e.g, `hotMesh.pub(topic, payload)`).
2. `publishes`: A graph will publish an event only if there is a publishes property with a topcic. When the graph completes its execution, it will emit the specified event. Outside callers can subscribe to the outcome of all associated workflow runs of the graph by subscribing to this topic (e.g, `hotMesh.psub(topic + '.*', handlerFn)`).

Here's an example of a subscription and publishing mechanism in YAML:

```yaml
graphs:
  - subscribes: order.approval.requested
    publishes: order.approval.responded
    activities: #...
    transitions: #...

  - subscribes: order.approval.price.requested
    publishes: order.approval.price.responded
    activities: #...
    transitions: #...
```

## 4. Activities
### 4.1. Types of Activities
Activities are the core building blocks of a graph, representing individual tasks or operations that need to be performed as part of the workflow. There are several types of activities, each with its own purpose and functionality.

#### 4.1.1. Hook

An `hook` represents an activity that listens for outside events, including time hook (sleep), Web hook, and cycle (repeat). It can be used to resolve complex mapping values that would be too expensive to resolve more than once.

Example of an `activity` in YAML:

```yaml
a3:
  title: "Return True"
  type: "activity"
  job:
    maps:
      id: "{a1.output.data.id}"
      price: "{a1.output.data.price}"
      approvals:
        price: "{a2.output.data.approved}"
      approved: true
```

These types of activities, when combined, allow for the creation of versatile and dynamic workflows within the graph model.

#### 4.1.2. Trigger
A `trigger` activity is the starting point of a graph. It is executed when the graph is triggered by an event that it subscribes to. A trigger activity is typically used to receive and process data from the triggering event, and its output can be used as input for subsequent activities.

Example of a `trigger` in YAML:

```yaml
a1:
  title: "Get Approval"
  type: "trigger"
  output:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        # ...
```

#### 4.1.3. Await

An `await` activity is used to wait for an external event or a specific condition to be met before proceeding with the execution of the graph. This can be useful when the workflow depends on data or events from other components in the application.

Example of an `await` in YAML:

```yaml
a2:
  title: "Get Price Approval"
  type: "await"
  topic: "order.approval.price.requested"
  input:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        # ...
  output:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        # ...
```

#### 4.1.4. Worker

A `worker` activity is used to call a registered worker function where HotMesh has a presence. The typical workflow would be to instantiate an instance of HotMesh and register a callback function to handle the associated topic. As the engine orchestrates worflows, it will invoke the callback handler, providing the defined payload (the `input`) with the invocation. Handler functions are expected to return a data payload as defined by the activity's YAML definition (the `output`). The handler function may return a `status` field in the payload with a value of `pending` (`success` is implied) in order to emit state updates back to the caller. This is useful when the worker function is long-running and the parent flow/graph needs to be notified of progress incrementally.

Example of a `worker` in YAML:

```yaml
my_calculator_activity:
  title: Calculate Something
  type: worker
  topic: calculation.execute
  input:
    schema:
      $ref: '../schemas/calculate.yaml#/input'
    maps:
      $ref: '../maps/calculation.execute.yaml#/executor/input'
  job:
    maps:
      $ref: '../maps/calculation.execute.yaml#/executor/job'
        # ...
```

#### 4.1.5. Cycle
The cycle activity targets an ancestor activity in the graph and re-executes it. This is useful when the graph needs to be re-executed from a specific point in the workflow.

Cycle activities can also include input mappings in order to overrride and/or provide additional data to the ensuing cycle. The target ancestor will re-run Leg 2 in a new dimensional thread, ensuring no activity state collisions; however, it is possible for any dimensional thread to update the shared job state.

Consider the following graph where the `c1` activity is a cycle activity that targets the `a1` activity. Each time the worker activity, `w1` returns a message with an error status (`code:500`), the cycle activity will re-execute the DAG, starting at the `a1` activity. The cycle repeats until the worker activity returns a message with a success status (`code:200`).

>NOTE: It is important to include a sleep activity in such cases in order to avoid unnecessary cycles when handling errors.

```yaml
app:
  id: cycle
  version: '2'
  graphs:
    - subscribes: cycle.test
      expire: 120

      output:
        schema:
          type: object
          properties:
            counter:
              type: number

      activities:
        t1:
          title: Flow entry point
          type: trigger

        a1:
          title: Ancestor to return to upon error
          type: hook
          cycle: true

        w2:
          title: Calls a function that returns 500 or 200
          type: worker
          topic: cycle.err

          output:
            schema:
              type: object
              properties:
                counter:
                  type: number
            500:
              schema:
                type: object
                properties:
                  counter:
                    type: number

          job:
            maps:
              counter: '{$self.output.data.counter}'

        a2:
          title: Sleep for 60s
          type: hook
          sleep: 60

        c1:
          title: Go to ancestor a1
          type: cycle
          ancestor: a1

      transitions:
        t1:
          - to: a1
        a1:
          - to: w2
        w2:
          - to: a2
            conditions:
              code: 500
        a2:
          - to: c1
```
#### 4.1.6. Signal
The signal activity allows any flow to send a signal to any other flow, regardless of the relationship between the flows.

[TODO]

### 4.2. Input and Output Schemas
Input and output schemas are used in activities to define the structure and data types of the data that is being passed between activities. They ensure that the data is validated and transformed correctly, improving the reliability and maintainability of the workflow.

`Input Schema`: The input schema defines the structure and data types of the data that the activity expects as input. It can be used to map the output data of previous activities to the input data of the current activity.

Example of an input schema in YAML:

```yaml
a2:
  title: "Get Price Approval"
  type: "await"
  # ...
  input:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        price:
          type: "number"
          description: "The price of the item."
          minimum: 0
        object_type:
          type: "string"
          description: "The type of the order (e.g, widgetA, widgetB)"
    maps:
      id: "{a1.output.data.id}"
      price: "{a1.output.data.price}"
      object_type: "{a1.output.data.object_type}"
```

`Output Schema`: The output schema defines the structure and data types of the data that the activity produces as output. It can be used to map the output data of the current activity to the input data of subsequent activities.
Example of an output schema in YAML:

```yaml
a2:
  title: "Get Price Approval"
  type: "await"
  # ...
  output:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        price:
          type: "number"
          description: "The price of the item."
          minimum: 0
        approved:
          type: "boolean"
          description: "Approval status of the object."
```

Input and output schemas play a crucial role in ensuring data integrity and consistency throughout the workflow, making it easier to develop, maintain, and troubleshoot the application.

### 4.3. Activity Mapping

Activity mapping is the process of connecting the output data of one activity to the input data of another activity in the graph. This enables the flow of data between activities, allowing for the creation of complex workflows.

Mappings are specified using a combination of activity identifiers and data paths in the input section of an activity. These mappings are expressed using a special syntax that refers to the data path in the output of the previous activity.

Example of activity mapping in YAML:

```yaml
a2:
  title: "Get Price Approval"
  type: "await"
  # ...
  input:
    schema:
      type: "object"
      properties:
        id:
          type: "string"
          description: "The unique identifier for the object."
        # ...
    maps:
      id: "{a1.output.data.id}"
      price: "{a1.output.data.price}"
      object_type: "{a1.output.data.object_type}"
```

In this example, the maps section of the input schema maps the `id`, `price`, and `object_type` properties of the input data to the corresponding output data of the `a1` activity. The syntax `{a1.output.data.id}` refers to the `id` property in the output data of the `a1` activity.

Activity mapping is essential for passing data between activities and building interconnected workflows within the graph model.

## 5. Transitions
Transitions define the flow of control between activities in the graph. They specify the relationships between the activities, determining how the execution should proceed through the workflow.

### 5.1. Defining Transitions

Transitions are defined in a separate transitions section within the graph model. Each transition is associated with an activity identifier (e.g., "a1") and contains a list of possible transitions to other activities, specified by their identifiers.

Example of defining transitions in YAML:

```yaml
transitions:
  a1:
    - to: "a2"
  a2:
    - to: "a3"
    - to: "a4"
```

In this example, there is a transition from activity `a1` to activity `a2`, and two transitions from activity `a2` to activities `a3` and `a4`.

### 5.2. Conditional Transitions

Conditional transitions allow the execution to branch based on the output data of an activity. They are specified using a conditions field in the transition definition, which contains a list of match conditions that must be met for the transition to be taken.

Example of conditional transitions in YAML:

```yaml
transitions:
  a2:
    - to: "a3"
      conditions:
        match:
          - expected: true
            actual: "{a2.output.data.approved}"
    - to: "a4"
      conditions:
        match:
          - expected: false
            actual: "{a2.output.data.approved}"
```

In this example, there are two conditional transitions from activity a2. If the approved property in the output data of a2 is true, the execution proceeds to activity a3. If the approved property is false, the execution proceeds to activity a4.

Conditional transitions enable complex decision-making within the graph model, allowing for dynamic control flow based on the data produced by activities.

## 6. Functional Mapper: @pipes
### 6.1. Introduction to @pipes
The '@pipes' functional mapper is a powerful feature of the model that allows for expressive and versatile data manipulation and transformation between activities. By using '@pipes', developers can create complex data mappings and conditional flows without the need for extensive scripting or programming.

The '@pipes' syntax is designed to be easy to read and understand, with a focus on brevity and clarity. It consists of a sequence of functions, where the output of one function becomes the input of the next function in the sequence. Functions can be chained together to create sophisticated transformations, comparisons, and evaluations.

Here's an example of using '@pipes' in a transition:

```yaml
transitions:
  a5:
    - to: a6
      conditions:
        match:
          - expected: true
            actual:
              "@pipe":
                - ["{a5.output.data.price}", 100]
                - ["{@number.lt}"]
    - to: a7
      conditions:
        match:
          - expected: true
            actual:
              "@pipe":
                - ["{a5.output.data.price}", 100]
                - ["{@number.gte}"]
```

In this example, '@pipes' is used to compare the price from the output of activity 'a5' against a constant value (100). The '@number.lt' function checks if the price is less than 100, and the '@number.gte' function checks if the price is greater than or equal to 100. Based on these comparisons, the transition either moves to activity 'a6' or 'a7'.

Understanding how to use '@pipes' effectively can greatly enhance the model's flexibility and power, enabling developers to create more efficient and sophisticated applications.

### 6.2. Using '@pipes' in Conditions

The '@pipes' functional mapper can be particularly useful when defining conditions within transitions. By using '@pipes', developers can create complex conditions based on the output of previous activities, allowing for dynamic decision-making within the application's flow.

To use '@pipes' in conditions, you need to define an array of functions within the 'actual' field of the 'match' object. Each function in the sequence takes the output of the previous function as its input, with the first function receiving the data to be transformed or compared. Functions can be chained together to create intricate conditions that depend on multiple data points or transformations.

Here's an example of using '@pipes' in a condition:

```yaml
transitions:
  a5:
    - to: a6
      conditions:
        match:
          - expected: true
            actual:
              "@pipe":
                - ["{a5.output.data.price}", 100]
                - ["{@number.lt}"]
    - to: a7
      conditions:
        match:
          - expected: true
            actual:
              "@pipe":
                - ["{a5.output.data.price}", 100]
                - ["{@number.gte}"]
```

In this example, the transition moves to activity 'a6' if the price from the output of activity 'a5' is less than 100, and to activity 'a7' if the price is greater than or equal to 100. The '@number.lt' and '@number.gte' functions are used to perform the comparison, with the output of the functions being compared against the 'expected' value (true) to determine the next activity in the sequence.

The use of '@pipes' in conditions enables developers to create more advanced and versatile workflows, with the ability to make decisions based on various data points, transformations, and comparisons. By mastering '@pipes', you can greatly expand the capabilities of your application and create more efficient and powerful processes.

### 6.3. Supported Functions
The '@pipes' functional mapper provides a range of built-in functions for common operations, such as mathematical calculations, string manipulation, and logical comparisons. The functions are organized based on their data type categories, such as array, object, number, string, etc., which should be familiar to JavaScript developers. However, it's essential to understand that the approach in HotMesh is functional. Each transformation is a function that expects one or more input parameters from the prior row in the mapping rules.

 - [Array Functions](./functions/array.md)
 - [Bitwise Functions](./functions/bitwise.md)
 - [Conditional Functions](./functions/conditional.md)
 - [Date/Time Functions](./functions/date.md)
 - [JSON Functions](./functions/json.md)
 - [Math Functions](./functions/math.md)
 - [Number Functions](./functions/number.md)
 - [Object Functions](./functions/object.md)
 - [String Functions](./functions/string.md)
 - [Symbol Functions](./functions/symbol.md)
 - [Unary Functions](./functions/unary.md)

## 7. Stats
Stats are used to collect and aggregate data generated during the execution of activities in the graph model. They allow for the calculation of metrics such as averages, counts, sums, and other aggregations based on the activity output data.

### 7.1. Defining Stats in Activities

Stats are defined within an activity by specifying a stats section, which includes a key, an identifier, and a list of measures to be calculated. The key is a unique identifier for the stat and is usually based on the output data of the activity. The identifier is used to group data by a specific property (e.g., "id").

Example of defining stats in YAML:

```yaml
a5:
  title: "Get Price Approval"
  type: "trigger"
  # ...
  stats:
    key: "{a5.output.data.object_type}"
    id: "{a5.input.data.id}"
    measures:
      - measure: "avg"
        target: "{a5.input.data.price}"
      - measure: "count"
        target: "{a5.input.data.object_type}"
```

In this example, the stats section of activity a5 defines two measures: the average price and the count of objects by their type.

### 7.2. Aggregating Data with Stats

Aggregating data with stats allows you to compute various metrics based on the output data of activities in the graph model. This can be useful for monitoring, reporting, and analysis purposes.

When an activity with a stats section is executed, the specified measures are calculated using the output data of the activity. The aggregated results are stored in the system and can be queried and analyzed later.

The available aggregation functions include:

- `avg`: Calculate the average value of the target property.
- `count`: Count the number of occurrences of the target property.
- `sum`: Calculate the sum of the target property values.
- `min`: Find the minimum value of the target property.
- `max`: Find the maximum value of the target property.

By using stats, you can track and analyze various aspects of your graph model's execution, gaining valuable insights into the behavior and performance of your application.

## 8. Example Walkthrough
In this section, we will walk through the order approval process example using the given JSON model. This example demonstrates how various components of the graph model work together to implement a simple business workflow.

### 8.1. Order Approval Process
The order approval process consists of two separate graphs:

1. The main graph listens for order.approval.requested events and publishes order.approval.responded events.
2. A secondary graph listens for order.approval.price.requested events and publishes order.approval.price.responded events.
The first graph includes the following activities:

- `a1`: A trigger activity that receives order approval requests.
- `a2`: An await activity that requests price approval by emitting an `order.approval.price.requested` event and waits for the corresponding response.
- `a3`: An activity that returns a positive approval response if the price approval was successful.
- `a4`: An activity that returns a negative approval response if the price approval was unsuccessful.
The transitions in the first graph are as follows:

- From `a1` to `a2`: The flow moves from the trigger activity to the await activity.
- From `a2` to `a3`: If the price approval is successful, the flow moves to the positive approval response.
- From `a2` to `a4`: If the price approval is unsuccessful, the flow moves to the negative approval response.

The second graph handles price approval and includes the following activities:

- `a5`: A trigger activity that receives price approval requests and calculates stats for average price and count by object type.
- `a6`: An activity that returns a positive price approval response if the price is less than 100.
- `a7`: An activity that returns a negative price approval response if the price is equal to or greater than 100.
The transitions in the second graph are as follows:

- From `a5` to `a6`: If the price is less than 100, the flow moves to the positive price approval response.
- From `a5` to `a7`: If the price is equal to or greater than 100, the flow moves to the negative price approval response.

In YAML, the overall structure of the order approval process looks like this:

```yaml
app:
  # ...
  graphs:
    - subscribes: "order.approval.requested"
      publishes: "order.approval.responded"
      activities:
        # ... (a1, a2, a3, a4 definitions)
      transitions:
        # ... (transitions between a1, a2, a3, a4)
    - subscribes: "order.approval.price.requested"
      publishes: "order.approval.price.responded"
      activities:
        # ... (a5, a6, a7 definitions)
      transitions:
        # ... (transitions between a5, a6, a7)
```

This example demonstrates how the graph model can be used to implement a simple order approval process with separate graphs handling different aspects of the workflow. The model leverages activities, transitions, and stats to define the flow of control and gather useful data throughout the process.

### 8.2. Price Approval Process
In this section, we will delve deeper into the price approval process, which is handled by the second graph in our example. This graph listens for `order.approval.price.requested` events and publishes `order.approval.price.responded` events. The purpose of this graph is to evaluate whether the price of an order meets certain criteria for approval. The graph includes the following activities:

- `a5`: A trigger activity that receives price approval requests and calculates stats for average price and count by object type (e.g., widgetA, widgetB). This activity outputs the unique identifier, price, and object type of the received order.
- `a6`: An activity that returns a positive price approval response (i.e., the approved attribute is set to true) if the price is less than 100.
- `a7`: An activity that returns a negative price approval response (i.e., the approved attribute is set to false) if the price is equal to or greater than 100.

The transitions in the second graph are as follows:

- From `a5` to `a6`: A conditional transition that moves the flow to the positive price approval response if the price is less than 100. The condition uses the @pipe functional mapper to check whether the price of the order is less than the specified threshold using the `@number.lt` function.
- From `a5` to `a7`: A conditional transition that moves the flow to the negative price approval response if the price is equal to or greater than 100. The condition uses the `@pipe` functional mapper to check whether the price of the order is greater than or equal to the specified threshold using the `@number.gte` function.

The YAML representation of the price approval process within the second graph looks like this:

```yaml
app:
  # ...
  graphs:
    - subscribes: "order.approval.price.requested"
      publishes: "order.approval.price.responded"
      activities:
        # ... (a5, a6, a7 definitions)
      transitions:
        # ... (transitions between a5, a6, a7)
```

In this example, the second graph illustrates how the model can be used to implement a specific part of a larger workflow, in this case, the price approval process. It demonstrates the use of various activities, conditional transitions with the @pipe functional mapper, and stats to gather data about the received orders. By organizing the logic within separate graphs, the model promotes modularity and separation of concerns, making it easier to understand and maintain.
