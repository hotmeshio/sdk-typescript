import { HMNS } from "../../../modules/key";
export const VERSION = '1';

/**
 * Provides the YAML necessary to create a workflow for the `MeshCall` service.
 * The `appId` parameter is optional and defaults to the HMNS constant ('hmsh').
 * 
 * All running workflows will be located in Redis, prefixed with this namespace.
 * The version is a string as it supports semantic versioning. It is also optional
 * and defaults to '1'.
 */
export const getWorkflowYAML = (appId = HMNS, version = VERSION): string => {
  return `app:
  id: ${appId}
  version: '${version}'
  graphs:
    - subscribes: ${appId}.call

      expire: '{trigger.output.data.expire}'

      input:
        schema:
          type: object
          properties:
            id:
              type: string
              description: unique id for this workflow
            expire:
              type: number
              description: time in seconds to expire the completed job and remove it from the cache
            topic:
              type: string
              description: topic assigned to locate the worker
            args:
              type: array
              description: arguments to pass to the worker function
              items:
                type: any

      output:
        schema:
          type: object
          properties:
            response:
              type: any

      activities:
        trigger:
          type: trigger
        stats:
          id: '{$self.input.data.id}'

        worker:
          type: worker
          topic: '{trigger.output.data.topic}'
          input:
            schema:
              type: object
              properties:
                args:
                  type: any
            maps:
              args: '{trigger.output.data.args}'
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
        trigger:
          - to: worker
`; 
};
