const getWorkflowYAML = (topic: string, version = '1') => {
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
          type: worker
          topic: ${topic}
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
      transitions:
        t1:
          - to: a1`;
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
