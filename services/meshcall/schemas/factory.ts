import { HMNS } from '../../../modules/key';
export const VERSION = '2';

/**
 * Provides the YAML necessary to create a workflow for the `MeshCall` service.
 * The `appId` parameter is optional and defaults to the HMNS constant ('hmsh').
 *
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

    - subscribes: ${appId}.cron

      input:
        schema:
          type: object
          properties:
            id:
              type: string
              description: unique id for this workflow
            delay:
              type: number
              description: time in seconds to sleep before invoking the first cycle
            interval:
              type: number
              description: time in seconds to sleep before the next cycle (also min interval in seconds if cron is provided)
            cron:
              type: string
              description: cron expression to determine the next cycle (takes precedence over interval)
            topic:
              type: string
              description: topic assigned to locate the worker
            args:
              type: array
              description: arguments to pass to the worker function
              items:
                type: any
            maxCycles:
              type: number
              description: maximum number of cycles to run before stopping

      output:
        schema:
          type: object
          properties:
            response:
              type: any

      activities:
        trigger_cron:
          type: trigger
          stats:
            id: '{$self.input.data.id}'
        
        cycle_hook_cron:
          type: hook
          output:
            schema:
              type: object
              properties:
                sleepSeconds:
                  type: number
                iterationCount:
                  type: number
            maps:
              sleepSeconds:
                '@pipe':
                  - ['{trigger_cron.output.data.delay}', '{@symbol.undefined}']
                  - ['{@conditional.nullish}']
              iterationCount: 1

        sleep_cron:
          type: hook
          sleep: '{cycle_hook_cron.output.data.sleepSeconds}'
        
        worker_cron:
          type: worker
          topic: '{trigger_cron.output.data.topic}'
          input:
            schema:
              type: object
              properties:
                args:
                  type: any
            maps:
              args: '{trigger_cron.output.data.args}'

        cycle_cron:
          type: cycle
          ancestor: cycle_hook_cron
          input:
            maps:
              sleepSeconds:
                '@pipe':
                  - ['{trigger_cron.output.data.cron}']
                  - ['{@cron.nextDelay}', '{trigger_cron.output.data.interval}']
                  - ['{@math.max}']
              iterationCount:
                '@pipe':
                  - ['{cycle_hook_cron.output.data.iterationCount}', 1]
                  - ['{@math.add}']

      transitions:
        trigger_cron:
          - to: cycle_hook_cron
        cycle_hook_cron:
          - to: sleep_cron
        sleep_cron:
          - to: worker_cron
        worker_cron:
          - to: cycle_cron
            conditions:
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - ['{cycle_hook_cron.output.data.iterationCount}', '{trigger_cron.output.data.maxCycles}']
                      - ['{@conditional.less_than}']
`;
};
