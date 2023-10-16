/**
 * 1) `maxSystemRetries` | can be 0 to 3 and represents milliseconds;
 * if there is an error, the workflow will retry up to `maxSystemRetries` times
 * delaying by 10, 100, and 1000ms; this is a system level retry
 * and is not configurable. It exists to handle intermittent network
 * errors. (NOTE: each retry spawns a new transition stream)
 * 
 * 2) `backoffExponent` | can be any number and represents `seconds` when applied;
 * retries will happen indefinitely and adhere to the
 * exponential backoff algorithm by multiplying by `backoffExponent`.
 * For example, if `backoffExponent` is 10, the workflow will retry
 * in 10s, 100s, 1000s, 10000s, etc.
 *
 * EXAMPLE | Using `maxSystemRetries = 3` and `backoffExponent = 10`, errant
 * workflows will be retried on the following schedule (8 times in 27 hours):
 * => 10ms, 100ms, 1000ms, 10s, 100s, 1_000s, 10_000s, 100_000s
 */
const getWorkflowYAML = (topic: string, version = '1', maxSystemRetries = 2, backoffExponent = 10) => {
  return `app:
  id: ${topic}
  version: '${version}'
  graphs:
    - subscribes: ${topic}
      publishes: ${topic}
      expire: 120
      input:
        schema:
          type: object
          properties:
            workflowId:
              type: string
            arguments:
              type: array
      output:
        schema:
          type: object
          properties:
            response:
              type: any

      activities:
        t1:
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'

        a1:
          type: activity
          cycle: true
          output:
            schema:
              type: object
              properties:
                duration:
                  type: number
            maps:
              duration: ${backoffExponent}

        w1:
          type: worker
          topic: ${topic}
          retry:
            '599': [${maxSystemRetries}]
          input:
            schema:
              type: object
              properties:
                workflowId:
                  type: string
                arguments:
                  type: array
            maps:
              workflowId: '{t1.output.data.workflowId}'
              arguments: '{t1.output.data.arguments}'
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
          job:
            maps:
              response: '{$self.output.data.response}'

        a599:
          title: Sleep before trying again
          type: activity
          sleep: "{a1.output.data.duration}"

        c1:
          title: Goto Activity a1
          type: cycle
          ancestor: a1
          input:
            maps:
              duration:
                '@pipe':
                  - ['{a1.output.data.duration}', ${backoffExponent}]
                  - ['{@math.multiply}']

      transitions:
        t1:
          - to: a1
        a1:
          - to: w1
        w1:
          - to: a599
            conditions:
              code: 599
        a599:
        - to: c1
`;
}

const getActivityYAML = (topic: string, version = '1') => {
  return `app:
  id: ${topic}
  version: '${version}'
  graphs:
    - subscribes: ${topic}
      input:
        schema:
          type: object
          properties:
            workflowId:
              type: string
            workflowTopic:
              type: string
            activityName:
              type: array
            arguments:
              type: array
      output:
        schema:
          type: object
          properties:
            response:
              type: any

      activities:
        t1:
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
        a1:
          type: worker
          topic: ${topic}
          input:
            schema:
              type: object
              properties:
                workflowId:
                  type: string
                workflowTopic:
                  type: string
                activityName:
                  type: array
                arguments:
                  type: array
            maps:
              workflowId: '{t1.output.data.workflowId}'
              workflowTopic: '{t1.output.data.workflowTopic}'
              activityName: '{t1.output.data.activityName}'
              arguments: '{t1.output.data.arguments}'
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
          job:
            maps:
              response: '{$self.output.data.response}'
      transitions:
        t1:
          - to: a1`;
}

export {
  getActivityYAML,
  getWorkflowYAML
};
