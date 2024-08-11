/**
 *********** HOTMESH 'MESHFLOW' MODULE APPLICATION GRAPH **********
 *
 * This HotMesh application spec uses 50 activities and 25 transitions
 * to model and emulate the Temporal Application & Query servers using
 * Redis as the backend.
 *
 * It's particularly useful for organizations with high-speed, high-volume
 * use cases as it uses in-memory Redis Streams for transactional,
 * workflow processing, while adhering to Temporal's developer-friendly syntax.
 *
 * This YAML file can also serve as a useful starting point for building
 * Integration/BPM/Workflow servers in general (MuleSoft, etc) without the need
 * for a physical application server.
 *
 * Possible use cases include:
 * * Orchestration servers
 * * Integration servers
 * * BPMN engines
 * * Reentrant process servers
 * * Service Meshes
 * * Master Data Management systems
 */

const APP_VERSION = '4';
const APP_ID = 'durable';

/**
 * returns a new durable workflow schema
 * @param {string} app - app name (e.g., 'durable')
 * @param {string} version - number as string (e.g., '1')
 * @returns {string} HotMesh App YAML
 */
const getWorkflowYAML = (app: string, version: string): string => {
  return `app:
  id: ${app}
  version: '${version}'
  graphs:

    ###################################################
    #         THE MESHFLOW-REENTRANT-WORKFLOW         #
    #                                                 #
    - subscribes: ${app}.execute
      publishes: ${app}.executed

      persistent: '{trigger.output.data.persistent}'
      expire:
        '@pipe':
          - ['{trigger.output.data.persistent}']
          - ['{@number.isNaN}', '{trigger.output.data.originJobId}']
          - ['{@logical.and}', 1,'{trigger.output.data.expire}']
          - ['{@conditional.ternary}']

      input:
        schema:
          type: object
          properties:
            originJobId:
              description: the entry point from the outside world; subflows will inherit this value
              type: string
            workflowId:
              description: the id for this workflow (see \`trigger.stats.id\`)
              type: string
            parentWorkflowId:
              type: string
            arguments:
              description: the arguments to pass to the flow
              type: array
            workflowTopic:
              description: the Redis stream topic the worker is listening on
              type: string
            backoffCoefficient:
              description: the time multiple in seconds to backoff before retrying
              type: number
            maximumAttempts:
              description: the maximum number of retries to attempt before failing the workflow
              type: number
            maximumInterval:
              description: the maximum time in seconds to wait between retries; provides a fixed limit to exponential backoff growth
              type: number
            expire:
              description: the time in seconds to expire the workflow in Redis once it completes
              type: number
            persistent:
              description: if true, the workflow emit the 'job completed' event while remaining open to outside signals
              type: boolean
            signalIn:
              description: if false, the job will not support subordinated hooks
              type: boolean

      output:
        schema:
          type: object
          properties:
            response:
              description: the final response from the workflow if any
              type: any
            done:
              description: true if the workflow is complete (whether successful or not)
              type: boolean
            $error:
              description: only present if a structured error ocurred (598, 597, etc)
              type: object
              properties:
                code:
                  type: number
                message:
                  type: string
                stack:
                  type: string
            workflowId:
              type: string
            jc:
              type: string
            ju:
              type: string

      activities:

        ######## MAIN ACTIVITIES (RESPONSIBLE FOR FLOW ENTRY/STARTUP) ########
        trigger:
          title: Main Flow Trigger
          type: trigger
          job:
            maps:
              done: false
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            parent: '{$self.input.data.originJobId}'
            adjacent: '{$self.input.data.parentWorkflowId}'

        cycle_hook:
          title: Main Cycle Pivot - Cycling Descendants send execution back to this point
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                retryCount:
                  type: number
            maps:
              retryCount: 0
              throttleSeconds: 0

        throttler:
          title: Pauses for an exponentially-throttled amount of time after a retryable error or passes through immediately
          type: hook
          sleep: '{cycle_hook.output.data.throttleSeconds}'

        worker:
          title: Main Worker - Calls linked Workflow functions
          type: worker
          topic: '{trigger.output.data.workflowTopic}'
          emit: '{$job.data.done}'
          input:
            schema:
              type: object
              properties:
                originJobId:
                  type: string
                workflowId:
                  type: string
                arguments:
                  type: array
                workflowTopic:
                  type: string
                canRetry:
                  type: boolean
                expire:
                  type: number
            maps:
              originJobId: '{trigger.output.data.originJobId}'
              workflowId: '{trigger.output.data.workflowId}'
              arguments: '{trigger.output.data.arguments}'
              workflowTopic: '{trigger.output.data.workflowTopic}'
              expire: '{trigger.output.data.expire}'
              canRetry:
                '@pipe':
                  - '@pipe':
                    - ['{cycle_hook.output.data.retryCount}']
                  - '@pipe':
                    - ['{trigger.output.data.maximumAttempts}', 5]
                    - ['{@conditional.nullish}']
                  - ['{@conditional.less_than}']
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
            589:
              schema:
                type: object
                properties:
                  workflowId:
                    type: string
                  parentWorkflowId:
                    type: string
                  originJobId:
                    type: string
                  index:
                    type: number
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  items:
                    type: array
                    description: the items to pass
                    items:
                      type: object
                  size:
                    type: number
                    description: the number of items to pass
            590:
              schema:
                type: object
                properties:
                  workflowId:
                    type: string
                  parentWorkflowId:
                    type: string
                  originJobId:
                    type: string
                  workflowName:
                    type: string
                  index:
                    type: number
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  arguments:
                    type: array
                    description: the arguments to pass to the flow (recursive)
                    items:
                      type: any
                  backoffCoefficient:
                    type: number
                  maximumAttempts:
                    type: number
                  maximumInterval:
                    type: number
                  expire:
                    type: number
                  persistent:
                    type: boolean
                  signalIn:
                    type: boolean
                  await:
                    type: string
                    description: when set to false, do not await the child flow's completion
            591:
              schema:
                type: object
                properties:
                  activityName:
                    type: string
                  index:
                    type: number
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  arguments:
                    type: array
                    description: the arguments to pass to the activity
                    items:
                      type: any
                  workflowId:
                    type: string
                  workflowTopic:
                    type: string
                  parentWorkflowId:
                    type: string
                  originJobId:
                    type: string
                  backoffCoefficient:
                    type: number
                  maximumAttempts:
                    type: number
                  maximumInterval:
                    type: number
            588:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleepFor duration in seconds
                  index:
                    type: number
                    description: the replay index (COUNTER++)
            595:
              schema:
                type: object
                properties:
                  index:
                    type: number
                    description: the index of the first signal in the array
                  signal:
                    type: object
                    properties:
                      signal:
                        type: string
          job:
            maps:
              response: '{$self.output.data.response}'

        sleeper:
          title: Pauses the main thread for a set amount of time; all other subprocess threads remain active, and new ones may be started
          type: hook
          sleep: '{worker.output.data.duration}'
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['-sleep', '{worker.output.data.workflowDimension}', '-', '{worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - [duration, '{worker.output.data.duration}', ac, '{$self.output.metadata.ac}', au, '{$self.output.metadata.au}']
                    - ['{@object.create}']
                  - ['{@object.create}']

        sleep_cycler:
          title: Cycles back to the cycle_hook pivot
          type: cycle
          ancestor: cycle_hook
          input:
            maps:
              retryCount: 0
              throttleSeconds: 0

        childer:
          title: Awaits a child flow to be executed/started
          type: await
          topic: ${app}.execute
          await: '{worker.output.data.await}'
          input:
            schema:
              type: object
              properties:
                workflowTopic:
                  type: string
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
                expire:
                  type: number
                persistent:
                  type: boolean
                signalIn:
                  type: boolean
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                  description: the baseId + index
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
            maps:
              arguments: '{worker.output.data.arguments}'
              workflowDimension: '{worker.output.data.workflowDimension}'
              index: '{worker.output.data.index}'
              originJobId: '{worker.output.data.originJobId}'
              parentWorkflowId: '{worker.output.data.parentWorkflowId}'
              expire: '{worker.output.data.expire}'
              persistent: '{worker.output.data.persistent}'
              signalIn: '{worker.output.data.signalIn}'
              workflowId: '{worker.output.data.workflowId}'
              workflowName: '{worker.output.data.workflowName}'
              workflowTopic: '{worker.output.data.workflowTopic}'
              backoffCoefficient:
                '@pipe':
                  - ['{worker.output.data.backoffCoefficient}','{trigger.output.data.backoffCoefficient}']
                  - ['{@conditional.nullish}', 10]
                  - ['{@conditional.nullish}']
              maximumAttempts:
                '@pipe':
                  - ['{worker.output.data.maximumAttempts}','{trigger.output.data.maximumAttempts}']
                  - ['{@conditional.nullish}', 5]
                  - ['{@conditional.nullish}']
              maximumInterval:
                '@pipe':
                  - ['{worker.output.data.maximumInterval}','{trigger.output.data.maximumInterval}']
                  - ['{@conditional.nullish}', 120]
                  - ['{@conditional.nullish}']
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['{worker.output.data.await}', true]
                    - ['{@conditional.nullish}', '-child', '-start']
                    - ['{@conditional.ternary}', '{worker.output.data.workflowDimension}', '-', '{worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.$error}', '{$self.output.metadata.$error}']
                      - ['{@logical.or}']
                    - '@pipe':
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data.$error}', '{$self.output.metadata.$error}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}', '{$self.output.metadata.ac}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}', '{$self.output.metadata.au}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - [job_id]
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - ['{$self.output.data.done}']
                      - '@pipe':
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.response}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$self.output.data.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{$self.output.data.ju}']
                        - '@pipe':
                          - [job_id]
                        - '@pipe':
                          - ['{$self.output.data.workflowId}']
                        - ['{@object.create}']
                      - '@pipe':
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$self.output.data.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{$self.output.data.ju}']
                        - '@pipe':
                          - [job_id]
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - ['{@object.create}']
                      - ['{@conditional.ternary}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

        child_cycler:
          title: Cycles back to the cycle_hook
          type: cycle
          ancestor: cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - '@pipe':
                    - ['{childer.output.metadata.err}']
                  - '@pipe':
                    - ['{cycle_hook.output.data.retryCount}', 0]
                    - ['{@logical.or}', 1]
                    - ['{@math.add}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - ['{childer.output.metadata.err}']
                  - '@pipe':
                    - '@pipe':
                      - '@pipe':
                        - ['{trigger.output.data.backoffCoefficient}', 10]
                        - ['{@conditional.nullish}']
                      - '@pipe':
                        - ['{cycle_hook.output.data.retryCount}', 0]
                        - ['{@conditional.nullish}']
                      - ['{@math.pow}']
                    - '@pipe':
                      - ['{trigger.output.data.maximumInterval}', 120]
                      - ['{@logical.or}']
                    - ['{@math.min}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']

        proxyer:
          title: Invokes the activity flow and awaits the response
          type: await
          topic: ${app}.activity.execute
          input:
            schema:
              type: object
              properties:
                activityName:
                  type: string
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                  description: the baseId + index
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
                expire:
                  type: number
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
            maps:
              activityName: '{worker.output.data.activityName}'
              arguments: '{worker.output.data.arguments}'
              workflowDimension: '{worker.output.data.workflowDimension}'
              index: '{worker.output.data.index}'
              originJobId: '{worker.output.data.originJobId}'
              parentWorkflowId: '{worker.output.data.workflowId}'
              workflowId: '{worker.output.data.workflowId}'
              workflowTopic: '{worker.output.data.workflowTopic}'
              expire: '{worker.output.data.expire}'
              backoffCoefficient:
                '@pipe':
                  - ['{worker.output.data.backoffCoefficient}','{trigger.output.data.backoffCoefficient}']
                  - ['{@conditional.nullish}', 10]
                  - ['{@conditional.nullish}']
              maximumAttempts:
                '@pipe':
                  - ['{worker.output.data.maximumAttempts}','{trigger.output.data.maximumAttempts}']
                  - ['{@conditional.nullish}', 5]
                  - ['{@conditional.nullish}']
              maximumInterval:
                '@pipe':
                  - ['{worker.output.data.maximumInterval}','{trigger.output.data.maximumInterval}']
                  - ['{@conditional.nullish}', 120]
                  - ['{@conditional.nullish}']
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['-proxy', '{worker.output.data.workflowDimension}', '-', '{worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.$error}']
                    - '@pipe':
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data.$error}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}']
                      - '@pipe':
                        - ['job_id']
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - [data]
                      - '@pipe':
                        - ['{$self.output.data.response}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}']
                      - '@pipe':
                        - ['job_id']
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

        proxy_cycler:
          title: Cycles back to the cycle_hook
          type: cycle
          ancestor: cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - '@pipe':
                    - ['{proxyer.output.metadata.err}']
                  - '@pipe':
                    - ['{cycle_hook.output.data.retryCount}', 0]
                    - ['{@logical.or}', 1]
                    - ['{@math.add}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - ['{proxyer.output.metadata.err}']
                  - '@pipe':
                    - '@pipe':
                      - '@pipe':
                        - ['{trigger.output.data.backoffCoefficient}', 10]
                        - ['{@conditional.nullish}']
                      - '@pipe':
                        - ['{cycle_hook.output.data.retryCount}', 0]
                        - ['{@conditional.nullish}']
                      - ['{@math.pow}']
                    - '@pipe':
                      - ['{trigger.output.data.maximumInterval}', 120]
                      - ['{@logical.or}']
                    - ['{@math.min}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']

        collator:
          title: Awaits the collator flow to simultaneously resolve the idempotent items and return as a sequential set
          type: await
          topic: ${app}.collator.execute
          input:
            schema:
              type: object
              properties:
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                size:
                  type: number
                  description: the number of idempotent items to resolve
                items:
                  type: array
                  description: the idempotent items to resolve
                  items:
                    type: object
                    properties:
                      index:
                        type: number
                      data:
                        type: object
                expire:
                  type: number
            maps:
              items: '{worker.output.data.items}'
              size: '{worker.output.data.size}'
              workflowDimension: '{worker.output.data.workflowDimension}'
              index: '{worker.output.data.index}'
              originJobId: '{worker.output.data.originJobId}'
              parentWorkflowId: '{worker.output.data.workflowId}'
              workflowId: '{worker.output.data.workflowId}'
              workflowTopic: '{worker.output.data.workflowTopic}'
              expire: '{worker.output.data.expire}'
          output:
            schema:
              type: object
              properties:
                size:
                  type: number
                data:
                  type: object
                  properties:
                    response:
                      description: the collated response, returned as a object with numeric keys, representing the order in the Promise.all array
                      type: object
                      patternProperties:
                        '^[0-9]+$':
                          type: object
                          properties:
                            type:
                              type: string
                              enum: [wait, proxy, child, start, sleep]
                            data:
                              type: unknown
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - ['{$self.output.data.response}', {}]
                  - '@reduce':
                      - '@pipe':
                          - - '{$output}'
                      - '@pipe':
                          - '@pipe':
                              - - '-'
                          - '@pipe':
                              - - '{$item}'
                                - type
                              - - '{@object.get}'
                          - '@pipe':
                              - - '{worker.output.data.workflowDimension}'
                          - '@pipe':
                              - - '-'
                          - '@pipe':
                              - '@pipe':
                                  - - '{worker.output.data.index}'
                              - '@pipe':
                                  - - '{$index}'
                              - - '{@math.add}'
                          - '@pipe':
                              - - '-'
                          - - '{@string.concat}'
                      - '@pipe':
                          - - '{$item}'
                      - - '{@object.set}'

        collate_cycler:
          title: Cycles back to the cycle_hook after collating the results
          type: cycle
          ancestor: cycle_hook
          input:
            maps:
              retryCount: 0
              throttleSeconds: 0

        retryer:
          title: Cycles back to the cycle_hook pivot, increasing the retryCount (the exponential)
          type: cycle
          ancestor: cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - ['{cycle_hook.output.data.retryCount}', 0]
                  - ['{@logical.or}', 1]
                  - ['{@math.add}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - '@pipe':
                      - ['{trigger.output.data.backoffCoefficient}', 10]
                      - ['{@conditional.nullish}']
                    - '@pipe':
                      - ['{cycle_hook.output.data.retryCount}', 0]
                      - ['{@conditional.nullish}']
                    - ['{@math.pow}']
                  - '@pipe':
                    - ['{trigger.output.data.maximumInterval}', 120]
                    - ['{@logical.or}']
                  - ['{@math.min}']
        ender:
          title: Sets job data; ignores the \`Signal In\` Hook Channel which was suppressed; sends the final response
          type: hook
          persist: '{trigger.output.data.signalIn}'
          job:
            maps:
              done: true
              $error: '{worker.output.data.$error}'
              jc: '{$job.metadata.jc}'
              ju:
                '@pipe':
                  - ['{@date.toISOXString}']

        closer:
          title: Closes the \`Signal In\` Hook Channel, so the workflow can exit
          type: signal
          subtype: one
          topic: ${app}.flow.signal
          statusThreshold: 1
          signal:
            schema:
              type: object
              properties:
                id:
                  type: string
            maps:
              id: '{$job.metadata.jid}'
          job:
            maps:
              done: true
              $error: '{worker.output.data.$error}'
              jc: '{$job.metadata.jc}'
              ju:
                '@pipe':
                  - ['{@date.toISOXString}']

        stopper:
          title: Stops 'Signal In' when retryCount is exceeded (and as a result ends the job)
          type: signal
          subtype: one
          topic: ${app}.flow.signal
          signal:
            schema:
              type: object
              properties:
                id:
                  type: string
            maps:
              id: '{$job.metadata.jid}'
          job:
            maps:
              done: true
              $error:
                '@pipe':
                  - ['{worker.output.data.$error}', 'code', 597]
                  - ['{@object.set}']
              jc: '{$job.metadata.jc}'
              ju:
                '@pipe':
                  - ['{@date.toISOXString}']

        ######## SIGNAL-IN ACTIVITIES (RESPONSIBLE FOR FLOW REENTRY) ########
        signaler:
          title: Signal-In Reentry point for subordinated hook flows
          type: hook
          statusThreshold: 1
          hook:
            type: object
            properties:
              id:
                type: string
              arguments:
                type: array
              workflowTopic:
                type: string
          job:
            maps:
              workflowId: '{trigger.output.data.workflowId}'
    
        signaler_cycle_hook:
          title: Signal In Cycle Pivot - Cycling Descendants Send Execution Back Here
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                retryCount:
                  type: number
            maps:
              retryCount: 0
              throttleSeconds: 0

        signaler_throttler:
          title: Pauses between failed hook executions for an exponentially-throttled amount of time after a retryable error
          type: hook
          sleep: '{signaler_cycle_hook.output.data.throttleSeconds}'
    
        signaler_worker:
          title: Signal In - Worker
          type: worker
          topic: '{signaler.hook.data.workflowTopic}'
          input:
            schema:
              type: object
              properties:
                workflowId:
                  type: string
                originJobId:
                  type: string
                workflowDimension:
                  type: string
                arguments:
                  type: array
                canRetry:
                  type: boolean
                expire:
                  type: number
            maps:
              workflowId: '{trigger.output.data.workflowId}'
              originJobId: '{trigger.output.data.originJobId}'
              workflowDimension: '{signaler.output.metadata.dad}'
              arguments: '{signaler.hook.data.arguments}'
              expire: '{trigger.output.data.expire}'
              canRetry:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_cycle_hook.output.data.retryCount}']
                  - '@pipe':
                    - ['{trigger.output.data.maximumAttempts}', 5]
                    - ['{@conditional.nullish}']
                  - ['{@conditional.less_than}']

          output:
            schema:
              type: object
            589:
              schema:
                description: the worker function output when Promise.all is used
                type: object
                properties:
                  workflowId:
                    type: string
                    description: the id for the new child workflow to spawn
                  parentWorkflowId:
                    type: string
                    description: parent workflow id (this workflow's id)
                  originJobId:
                    type: string
                    description: entry flow id (where outside world met the mesh)
                  index:
                    type: number
                    description: the replay index (COUNTER++)
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  items:
                    type: array
                    description: the items to collate
                    items:
                      type: object
                  size:
                    type: number
                    description: the number of items to collate
            590:
              schema:
                description: the worker function output when execChild or startChild are called
                type: object
                properties:
                  workflowId:
                    type: string
                    description: the id for the new child workflow to spawn
                  parentWorkflowId:
                    type: string
                    description: parent workflow id (this workflow's id)
                  originJobId:
                    type: string
                    description: entry flow id (where outside world met the mesh)
                  workflowName:
                    type: string
                    description: the linked function name
                  index:
                    type: number
                    description: the replay index (COUNTER++)
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  arguments:
                    type: array
                    description: the arguments to pass to the flow (recursive)
                    items:
                      type: any
                  backoffCoefficient:
                    type: number
                    description: the time multiple in seconds to backoff before retrying
                  maximumAttempts:
                    type: number
                    description: the maximum number of retries to attempt before failing the workflow
                  maximumInterval:
                    type: number
                    description: the maximum time in seconds to wait between retries; provides a fixed limit to exponential backoff growth
                  expire:
                    type: number
                  persistent:
                    type: boolean
                  signalIn:
                    type: boolean
                    description: if false, the spawned child will not support subordinated hooks
                  await:
                    type: string
                    description: when set to false, do not await the child flow's completion
            591:
              schema:
                type: object
                properties:
                  activityName:
                    type: string
                  index:
                    type: number
                  workflowDimension:
                    type: string
                    description: empty string or dimensionsal path (,0,0,1)
                  arguments:
                    type: array
                    description: the arguments to pass to the activity
                    items:
                      type: any
                  workflowId:
                    type: string
                  workflowTopic:
                    type: string
                  parentWorkflowId:
                    type: string
                  originJobId:
                    type: string
                  expire:
                    type: number
                  backoffCoefficient:
                    type: number
                  maximumAttempts:
                    type: number
                  maximumInterval:
                    type: number
            588:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleepFor duration in seconds
                  index:
                    type: number
                    description: the replay index (COUNTER++)
            595:
              schema:
                type: object
                properties:
                  index:
                    type: number
                    description: the index of the first signal in the array
                  signal:
                    type: object
                    properties:
                      signal:
                        type: string

        signaler_sleeper:
          title: Pauses a single thread within the worker for a set amount of seconds while the main flow thread and all other subthreads remain active
          type: hook
          sleep: '{signaler_worker.output.data.duration}'
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['-sleep', '{signaler_worker.output.data.workflowDimension}', '-', '{signaler_worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - [duration, '{signaler_worker.output.data.duration}', ac, '{$self.output.metadata.ac}', au, '{$self.output.metadata.au}']
                    - ['{@object.create}']
                  - ['{@object.create}']

        signaler_sleep_cycler:
          title: Cycles back to the signaler_cycle_hook pivot
          type: cycle
          ancestor: signaler_cycle_hook
          input:
            maps:
              retryCount: 0
              throttleSeconds: 0

        signaler_childer:
          title: Awaits a child flow to be executed/started
          type: await
          topic: ${app}.execute
          await: '{signaler_worker.output.data.await}'
          input:
            schema:
              type: object
              properties:
                workflowTopic:
                  type: string
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
                expire:
                  type: number
                persistent:
                  type: boolean
                signalIn:
                  type: boolean
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                  description: the baseId + index
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
            maps:
              arguments: '{signaler_worker.output.data.arguments}'
              workflowDimension: '{signaler_worker.output.data.workflowDimension}'
              index: '{signaler_worker.output.data.index}'
              originJobId: '{signaler_worker.output.data.originJobId}'
              parentWorkflowId: '{signaler_worker.output.data.parentWorkflowId}'
              expire: '{signaler_worker.output.data.expire}'
              persistent: '{signaler_worker.output.data.persistent}'
              signalIn: '{signaler_worker.output.data.signalIn}'
              workflowId: '{signaler_worker.output.data.workflowId}'
              workflowName: '{signaler_worker.output.data.workflowName}'
              workflowTopic: '{signaler_worker.output.data.workflowTopic}'
              backoffCoefficient:
                '@pipe':
                  - ['{signaler_worker.output.data.backoffCoefficient}','{trigger.output.data.backoffCoefficient}']
                  - ['{@conditional.nullish}', 10]
                  - ['{@conditional.nullish}']
              maximumAttempts:
                '@pipe':
                  - ['{signaler_worker.output.data.maximumAttempts}','{trigger.output.data.maximumAttempts}']
                  - ['{@conditional.nullish}', 5]
                  - ['{@conditional.nullish}']
              maximumInterval:
                '@pipe':
                  - ['{signaler_worker.output.data.maximumInterval}','{trigger.output.data.maximumInterval}']
                  - ['{@conditional.nullish}', 120]
                  - ['{@conditional.nullish}']
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_worker.output.data.await}', true]
                    - ['{@conditional.nullish}', '-child', '-start']
                    - ['{@conditional.ternary}', '{signaler_worker.output.data.workflowDimension}', '-', '{signaler_worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.$error}', '{$self.output.metadata.$error}']
                      - ['{@logical.or}']
                    - '@pipe':
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data.$error}', '{$self.output.metadata.$error}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}', '{$self.output.metadata.ac}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}', '{$self.output.metadata.au}']
                        - ['{@logical.or}']
                      - '@pipe':
                        - ['job_id']
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - ['{$self.output.data.done}']
                      - '@pipe':
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.response}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$self.output.data.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{$self.output.data.ju}']
                        - '@pipe':
                          - [job_id]
                        - '@pipe':
                          - ['{$self.output.data.workflowId}']
                        - ['{@object.create}']
                      - '@pipe':
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$self.output.data.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{$self.output.data.ju}']
                        - '@pipe':
                          - ['job_id']
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - ['{@object.create}']
                      - ['{@conditional.ternary}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

        signaler_child_cycler:
          title: Cycles back to the signaler_cycle_hook
          type: cycle
          ancestor: signaler_cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_childer.output.metadata.err}']
                  - '@pipe':
                    - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                    - ['{@logical.or}', 1]
                    - ['{@math.add}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_childer.output.metadata.err}']
                  - '@pipe':
                    - '@pipe':
                      - '@pipe':
                        - ['{trigger.output.data.backoffCoefficient}', 10]
                        - ['{@conditional.nullish}']
                      - '@pipe':
                        - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                        - ['{@conditional.nullish}']
                      - ['{@math.pow}']
                    - '@pipe':
                      - ['{trigger.output.data.maximumInterval}', 120]
                      - ['{@logical.or}']
                    - ['{@math.min}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']
  
        signaler_proxyer:
          title: Invokes the activity flow and awaits the response
          type: await
          topic: ${app}.activity.execute
          input:
            schema:
              type: object
              properties:
                activityName:
                  type: string
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                  description: the baseId + index
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
                expire:
                  type: number
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
            maps:
              activityName: '{signaler_worker.output.data.activityName}'
              arguments: '{signaler_worker.output.data.arguments}'
              workflowDimension: '{signaler_worker.output.data.workflowDimension}'
              index: '{signaler_worker.output.data.index}'
              originJobId: '{signaler_worker.output.data.originJobId}'
              parentWorkflowId: '{signaler_worker.output.data.workflowId}'
              workflowId: '{signaler_worker.output.data.workflowId}'
              workflowTopic: '{signaler_worker.output.data.workflowTopic}'
              expire: '{signaler_worker.output.data.expire}'
              backoffCoefficient:
                '@pipe':
                  - ['{signaler_worker.output.data.backoffCoefficient}','{trigger.output.data.backoffCoefficient}']
                  - ['{@conditional.nullish}', 10]
                  - ['{@conditional.nullish}']
              maximumAttempts:
                '@pipe':
                  - ['{signaler_worker.output.data.maximumAttempts}','{trigger.output.data.maximumAttempts}']
                  - ['{@conditional.nullish}', 5]
                  - ['{@conditional.nullish}']
              maximumInterval:
                '@pipe':
                  - ['{signaler_worker.output.data.maximumInterval}','{trigger.output.data.maximumInterval}']
                  - ['{@conditional.nullish}', 120]
                  - ['{@conditional.nullish}']
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - '@pipe':
                    - ['-proxy', '{signaler_worker.output.data.workflowDimension}', '-', '{signaler_worker.output.data.index}', '-']
                    - ['{@string.concat}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.$error}']
                    - '@pipe':
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data.$error}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}']
                      - '@pipe':
                        - ['job_id']
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - [data]
                      - '@pipe':
                        - ['{$self.output.data.response}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$self.output.data.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{$self.output.data.ju}']
                      - '@pipe':
                        - ['job_id']
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

        signaler_proxy_cycler:
          title: Cycles back to the signaler_cycle_hook
          type: cycle
          ancestor: signaler_cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_proxyer.output.metadata.err}']
                  - '@pipe':
                    - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                    - ['{@logical.or}', 1]
                    - ['{@math.add}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - ['{signaler_proxyer.output.metadata.err}']
                  - '@pipe':
                    - '@pipe':
                      - '@pipe':
                        - ['{trigger.output.data.backoffCoefficient}', 10]
                        - ['{@conditional.nullish}']
                      - '@pipe':
                        - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                        - ['{@conditional.nullish}']
                      - ['{@math.pow}']
                    - '@pipe':
                      - ['{trigger.output.data.maximumInterval}', 120]
                      - ['{@logical.or}']
                    - ['{@math.min}']
                  - '@pipe':
                    - [0]
                  - ['{@conditional.ternary}']

        signaler_collator:
          title: Awaits the collator to resolve the idempotent items as a sequential set
          type: await
          topic: ${app}.collator.execute
          input:
            schema:
              type: object
              properties:
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                size:
                  type: number
                  description: the number of idempotent items to collate
                items:
                  type: array
                  description: the idempotent items to collate
                  items:
                    type: object
                    properties:
                      index:
                        type: number
                      data:
                        type: object
                expire:
                  type: number
            maps:
              items: '{signaler_worker.output.data.items}'
              size: '{signaler_worker.output.data.size}'
              workflowDimension: '{signaler_worker.output.data.workflowDimension}'
              index: '{signaler_worker.output.data.index}'
              originJobId: '{signaler_worker.output.data.originJobId}'
              parentWorkflowId: '{signaler_worker.output.data.workflowId}'
              workflowId: '{signaler_worker.output.data.workflowId}'
              workflowTopic: '{signaler_worker.output.data.workflowTopic}'
              expire: '{signaler_worker.output.data.expire}'
          output:
            schema:
              type: object
              properties:
                size:
                  type: number
                data:
                  type: object
                  properties:
                    response:
                      description: the collated response, returned as a object with numeric keys, representing the order in the Promise.all array
                      type: object
                      patternProperties:
                        '^[0-9]+$':
                          type: object
                          properties:
                            type:
                              type: string
                              enum: [wait, proxy, child, start, sleep]
                            data:
                              type: unknown
          job:
            maps:
              idempotentcy-marker[-]:
                '@pipe':
                  - ['{$self.output.data.response}', {}]
                  - '@reduce':
                      - '@pipe':
                          - - '{$output}'
                      - '@pipe':
                          - '@pipe':
                              - - '-'
                          - '@pipe':
                              - - '{$item}'
                                - type
                              - - '{@object.get}'
                          - '@pipe':
                              - - '{signaler_worker.output.data.workflowDimension}'
                          - '@pipe':
                              - - '-'
                          - '@pipe':
                              - '@pipe':
                                  - - '{signaler_worker.output.data.index}'
                              - '@pipe':
                                  - - '{$index}'
                              - - '{@math.add}'
                          - '@pipe':
                              - - '-'
                          - - '{@string.concat}'
                      - '@pipe':
                          - - '{$item}'
                      - - '{@object.set}'

        signaler_collate_cycler:
          title: Cycles back to the signaler_cycle_hook after collating the results
          type: cycle
          ancestor: signaler_cycle_hook
          input:
            maps:
              retryCount: 0
              throttleSeconds: 0

        signaler_retryer:
          title: Cycles back to the signaler_cycle_hook pivot, increasing the retryCount (the exponential)
          type: cycle
          ancestor: signaler_cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                  - ['{@logical.or}', 1]
                  - ['{@math.add}']
              throttleSeconds:
                '@pipe':
                  - '@pipe':
                    - '@pipe':
                      - ['{trigger.output.data.backoffCoefficient}', 10]
                      - ['{@conditional.nullish}']
                    - '@pipe':
                      - ['{signaler_cycle_hook.output.data.retryCount}', 0]
                      - ['{@conditional.nullish}']
                    - ['{@math.pow}']
                  - '@pipe':
                    - ['{trigger.output.data.maximumInterval}', 120]
                    - ['{@logical.or}']
                  - ['{@math.min}']
  
      transitions:
        trigger:
          - to: cycle_hook
          - to: signaler
            conditions:
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - ['{$self.output.data.signalIn}', true]
                      - ['{@conditional.nullish}']
        ## MAIN PROCESS TRANSITIONS ##
        cycle_hook:
          - to: throttler
        throttler:
          - to: worker
        worker:
          - to: ender
            conditions:
              code: [200, 596, 597, 598]
              match:
                - expected: false
                  actual: 
                    '@pipe':
                      - ['{trigger.output.data.signalIn}', '{@symbol.undefined}']
                      - ['{@conditional.strict_equality}']
          - to: closer
            conditions:
              code: [200, 596, 597, 598]
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - ['{trigger.output.data.signalIn}', '{@symbol.undefined}']
                      - ['{@conditional.strict_equality}']
          - to: sleeper
            conditions:
              code: 588
          - to: collator
            conditions:
              code: 589
          - to: childer
            conditions:
              code: 590
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - '@pipe':
                        - ['{cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.less_than}']
          - to: proxyer
            conditions:
              code: 591
          - to: retryer
            conditions:
              code: 599
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - '@pipe':
                        - ['{cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.less_than}']
          - to: stopper
            conditions:
              code: [590, 591, 596, 597, 598, 599]
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - '@pipe':
                        - ['{cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.greater_than_or_equal}']
        collator:
          - to: collate_cycler
        childer:
          - to: child_cycler
        proxyer:
          - to: proxy_cycler
        sleeper:
          - to: sleep_cycler
        ### SUBPROCESS TRANSITIONS (REENTRY) ###
        signaler:
          - to: signaler_cycle_hook
            conditions:
              code: 202
        signaler_cycle_hook:
          - to: signaler_throttler
        signaler_throttler:
          - to: signaler_worker
        signaler_worker:
          - to: signaler_sleeper
            conditions:
              code: 588
          - to: signaler_collator
            conditions:
              code: 589
          - to: signaler_childer
            conditions:
              code: 590
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - '@pipe':
                        - ['{signaler_cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.less_than}']
          - to: signaler_proxyer
            conditions:
              code: 591
          - to: signaler_retryer
            conditions:
              code: 599
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - '@pipe':
                        - ['{signaler_cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.less_than}']
        signaler_collator:
          - to: signaler_collate_cycler
        signaler_childer:
          - to: signaler_child_cycler
        signaler_proxyer:
          - to: signaler_proxy_cycler
        signaler_sleeper:
          - to: signaler_sleep_cycler

      hooks:
        ${app}.flow.signal:
          - to: signaler
            conditions:
              match:
                - expected: '{trigger.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'



    ###################################################
    #          THE REENTRANT COLLATOR FLOW            #
    #                                                 #
    - subscribes: ${app}.collator.execute
      publishes: ${app}.collator.executed

      expire:
        '@pipe':
          - ['{collator_trigger.output.data.expire}', 1]
          - ['{@conditional.nullish}']

      input:
        schema:
          type: object
          properties:
            parentWorkflowId:
              type: string
            originJobId:
              type: string
            workflowId:
              type: string
            workflowTopic:
              type: string
            size:
              title: The number of idempotent items to resolve
              type: number
            items:
              title: Idempotent items to resolve
              type: array
              items:
                type: object
                properties:
                  index:
                    type: index
                  data:
                    type: object
            expire:
              type: number
      output:
        schema:
          type: object
          properties:
            responses:
              type: array
              items:
                type: object
                properties:
                  index:
                    type: number
                  response:
                    type: any
                  $error:
                    type: object
                    properties:
                      code:
                        type: number
                      message:
                        type: string
                      stack:
                        type: string

      activities:
        collator_trigger:
          title: Collator Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            parent: '{$self.input.data.originJobId}'
            adjacent: '{$self.input.data.parentWorkflowId}'
          job:
            maps:
              cur_index: 0

        collator_cycle_hook:
          title: Pivot - Cycling Descendant Points Here
          type: hook
          cycle: true
          job:
            maps:
              size:
                '@pipe':
                  - ['{collator_trigger.output.data.items}']
                  - ['{@array.length}']
              cur_index:
                '@pipe':
                  - ['{$job.data.cur_index}', 1]
                  - ['{@math.add}']
          output:
            maps:
              cur_index: '{$job.data.cur_index}'

        collator_cycler:
          title: Cycles back to the collator_cycle_hook pivot
          type: cycle
          ancestor: collator_cycle_hook
          input:
            maps:
              cur_index: '{$job.data.cur_index}'

        collator_sleeper:
          title: Pauses a single thread within the collator for a set amount of time while all other threads remain active
          type: hook
          sleep:
            '@pipe':
              - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
              - ['{@array.get}', duration]
              - ['{@object.get}']
          job:
            maps:
              response[25]:
                '@pipe':
                  - '@pipe':
                    - ['{collator_cycle_hook.output.data.cur_index}']
                  - '@pipe':
                    - '@pipe':
                      - [type]
                    - '@pipe':
                      - [sleep]
                    - '@pipe':
                      - [duration]
                    - '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', duration]
                      - ['{@object.get}']
                    - '@pipe':
                      - [ac]
                    - '@pipe':
                      - ['{$self.output.metadata.ac}']
                    - '@pipe':
                      - [au]
                    - '@pipe':
                      - ['{$self.output.metadata.au}']
                    - ['{@object.create}']
                  - ['{@object.create}']

        collator_waiter:
          title: Waits for a matching signal to be sent to the collator workflow
          type: hook
          hook:
            type: object
            properties:
              signalData:
                type: object
          job:
            maps:
              response[25]:
                '@pipe':
                  - ['{collator_trigger.output.data.items}']
                  - '@pipe':
                    - ['{collator_cycle_hook.output.data.cur_index}']
                  - '@pipe':
                    - [type, wait, data, '{$self.hook.data}', ac, '{$job.metadata.jc}', au, '{$self.output.metadata.au}']
                    - ['{@object.create}']
                  - ['{@object.create}']

        collator_childer:
          title: Awaits a call for a child flow to be executed/started
          type: await
          topic: ${app}.execute
          await:
            '@pipe':
              - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
              - ['{@array.get}', await]
              - ['{@object.get}']
          input:
            schema:
              type: object
              properties:
                workflowTopic:
                  type: string
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
                expire:
                  type: number
                persistent:
                  type: boolean
                signalIn:
                  type: boolean
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
            maps:
              arguments:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', arguments]
                  - ['{@object.get}']
              workflowDimension:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowDimension]
                  - ['{@object.get}']
              index:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', index]
                  - ['{@object.get}']
              originJobId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', originJobId]
                  - ['{@object.get}']
              expire:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', expire]
                  - ['{@object.get}']
              persistent:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', persistent]
                  - ['{@object.get}']
              signalIn:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', signalIn]
                  - ['{@object.get}']
              parentWorkflowId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', parentWorkflowId]
                  - ['{@object.get}']
              workflowId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowId]
                  - ['{@object.get}']
              workflowName:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowName]
                  - ['{@object.get}']
              workflowTopic:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowTopic]
                  - ['{@object.get}']
              backoffCoefficient:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', backoffCoefficient]
                  - ['{@object.get}']
              maximumAttempts:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', maximumAttempts]
                  - ['{@object.get}']
              maximumInterval:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', maximumInterval]
                  - ['{@object.get}']
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
                $error:
                  type: object
                  properties:
                    code:
                      type: number
                    message:
                      type: string
                    stack:
                      type: string
                done:
                  type: boolean
                workflowId:
                  type: string
                jc:
                  type: string
                ju:
                  type: string
          job:
            maps:
              response[25]:
                '@pipe':
                  - '@pipe':
                    - ['{collator_cycle_hook.output.data.cur_index}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.$error}']
                    - '@pipe':
                      - '@pipe':
                        - [type]
                      - '@pipe':
                        - [child]
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data.$error}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$job.metadata.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{@date.toISOXString}']
                      - '@pipe':
                        - [job_id]
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - ['{$self.output.data.done}']
                      - '@pipe':
                        - '@pipe':
                          - [type]
                        - '@pipe':
                          - [child]
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.response}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$job.metadata.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{@date.toISOXString}']
                        - '@pipe':
                          - [job_id]
                        - '@pipe':
                          - ['{$self.output.data.workflowId}']
                        - ['{@object.create}']
                      - '@pipe':
                        - '@pipe':
                          - [type]
                        - '@pipe':
                          - [start]
                        - '@pipe':
                          - [data]
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - '@pipe':
                          - [ac]
                        - '@pipe':
                          - ['{$job.metadata.jc}']
                        - '@pipe':
                          - [au]
                        - '@pipe':
                          - ['{@date.toISOXString}']
                        - '@pipe':
                          - [job_id]
                        - '@pipe':
                          - ['{$self.output.data.job_id}']
                        - ['{@object.create}']
                      - ['{@conditional.ternary}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

        collator_proxyer:
          title: Invokes the activity flow and awaits the response
          type: await
          topic: ${app}.activity.execute
          input:
            schema:
              type: object
              properties:
                activityName:
                  type: string
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                originJobId:
                  type: string
                  description: used for dependency cleanup
                workflowId:
                  type: string
                arguments:
                  type: array
                  description: the arguments to pass to the activity
                  items:
                    type: string
                expire:
                  type: number
                backoffCoefficient:
                  type: number
                maximumAttempts:
                  type: number
                maximumInterval:
                  type: number
            maps:
              activityName:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', activityName]
                  - ['{@object.get}']
              arguments:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', arguments]
                  - ['{@object.get}']
              workflowDimension:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowDimension]
                  - ['{@object.get}']
              index:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', index]
                  - ['{@object.get}']
              originJobId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', originJobId]
                  - ['{@object.get}']
              parentWorkflowId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', parentWorkflowId]
                  - ['{@object.get}']
              workflowId:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowId]
                  - ['{@object.get}']
              workflowTopic:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', workflowTopic]
                  - ['{@object.get}']
              expire:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', expire]
                  - ['{@object.get}']
              backoffCoefficient:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', backoffCoefficient]
                  - ['{@object.get}']
              maximumAttempts:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', maximumAttempts]
                  - ['{@object.get}']
              maximumInterval:
                '@pipe':
                  - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                  - ['{@array.get}', maximumInterval]
                  - ['{@object.get}']
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
                $error:
                  type: object
                  properties:
                    code:
                      type: number
                    message:
                      type: string
                    stack:
                      type: string
                done:
                  type: boolean
                workflowId:
                  type: string
                jc:
                  type: string
                ju:
                  type: string
          job:
            maps:
              response[25]:
                '@pipe':
                  - '@pipe':
                    - ['{collator_cycle_hook.output.data.cur_index}']
                  - '@pipe':
                    - '@pipe':
                      - ['{$self.output.data.response}']
                    - '@pipe':
                      - '@pipe':
                        - [type]
                      - '@pipe':
                        - ['proxy']
                      - '@pipe':
                        - [data]
                      - '@pipe':
                        - ['{$self.output.data.response}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$job.metadata.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{@date.toISOXString}']
                      - '@pipe':
                        - [job_id]
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - '@pipe':
                      - '@pipe':
                        - [$error]
                      - '@pipe':
                        - ['{$self.output.data}']
                      - '@pipe':
                        - [ac]
                      - '@pipe':
                        - ['{$job.metadata.jc}']
                      - '@pipe':
                        - [au]
                      - '@pipe':
                        - ['{@date.toISOXString}']
                      - '@pipe':
                        - [job_id]
                      - '@pipe':
                        - ['{$self.output.data.workflowId}']
                      - ['{@object.create}']
                    - ['{@conditional.ternary}']
                  - ['{@object.create}']

      transitions:
        collator_trigger:
          - to: collator_cycle_hook
        collator_cycle_hook:
          - to: collator_cycler
            conditions:
              code: 200
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - '@pipe':
                        - ['{$job.data.cur_index}']
                      - '@pipe':
                        - ['{collator_trigger.output.data.items}']
                        - ['{@array.length}']
                      - ['{@conditional.less_than}']
          - to: collator_childer
            conditions:
              code: 200
              match:
                - expected: 590
                  actual: 
                    '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', code]
                      - ['{@object.get}']
          - to: collator_proxyer
            conditions:
              code: 200
              match:
                - expected: 591
                  actual: 
                    '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', code]
                      - ['{@object.get}']
          - to: collator_sleeper
            conditions:
              code: 200
              match:
                - expected: 588
                  actual: 
                    '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', code]
                      - ['{@object.get}']
          - to: collator_waiter
            conditions:
              code: 200
              match:
                - expected: 595
                  actual: 
                    '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', code]
                      - ['{@object.get}']

      hooks:
        ${app}.wfs.signal:
          - to: collator_waiter
            conditions:
              match:
                - expected:
                    '@pipe':
                      - ['{collator_trigger.output.data.items}', '{collator_cycle_hook.output.data.cur_index}']
                      - ['{@array.get}', signalId]
                      - ['{@object.get}']
                  actual: '{$self.hook.data.id}'



    ###################################################
    #          THE REENTRANT ACTIVITY FLOW            #
    #                                                 #
    - subscribes: ${app}.activity.execute
      publishes: ${app}.activity.executed

      expire:
        '@pipe':
          - ['{activity_trigger.output.data.expire}', 1]
          - ['{@conditional.nullish}']

      input:
        schema:
          type: object
          properties:
            parentWorkflowId:
              type: string
            originJobId:
              type: string
            workflowId:
              type: string
            workflowTopic:
              type: string
            activityName:
              type: string
            arguments:
              type: array
            backoffCoefficient:
              type: number
            maximumAttempts:
              type: number
            maximumInterval:
              type: number
            expire:
              type: number
      output:
        schema:
          type: object
          properties:
            response:
              type: any
            done:
              type: boolean
            workflowId:
              type: string
            jc:
              type: string
            ju:
              type: string

      activities:
        activity_trigger:
          title: Activity Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            parent: '{$self.input.data.originJobId}'
            adjacent: '{$self.input.data.parentWorkflowId}'
          job:
            maps:
              workflowId: '{$self.input.data.workflowId}'

        activity_cycle_hook:
          title: Activity Flow Pivot - Cycling Descendants Point Here
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                retryCount:
                  type: number
            maps:
              retryCount: 0

        activity_worker:
          title: Activity Worker - Calls Activity Functions
          type: worker
          topic: '{activity_trigger.output.data.workflowTopic}'
          input:
            schema:
              type: object
              properties:
                parentWorkflowId:
                  type: string
                workflowId:
                  type: string
                workflowTopic:
                  type: string
                activityName:
                  type: string
                arguments:
                  type: array
            maps:
              parentWorkflowId: '{activity_trigger.output.data.parentWorkflowId}'
              workflowId: '{activity_trigger.output.data.workflowId}'
              workflowTopic: '{activity_trigger.output.data.workflowTopic}'
              activityName: '{activity_trigger.output.data.activityName}'
              arguments: '{activity_trigger.output.data.arguments}'
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
          job:
            maps:
              response: '{$self.output.data.response}'

        activity_retryer:
          title: Pauses for an exponentially-throttled amount of time after a 599 (retryable) error
          type: hook
          sleep:
            '@pipe':
              - '@pipe':
                - ['{activity_trigger.output.data.backoffCoefficient}', 10]
                - ['{@logical.or}', '{activity_cycle_hook.output.data.retryCount}']
                - ['{@math.pow}']
              - '@pipe':
                - ['{activity_trigger.output.data.maximumInterval}', 120]
                - ['{@math.min}']
              - ['{@math.min}']

        activity_retry_cycler:
          title: Cycles back to the activity_cycle_hook pivot, incrementing the \`retryCount\` (the exponential)
          type: cycle
          ancestor: activity_cycle_hook
          input:
            maps:
              retryCount:
                '@pipe':
                  - ['{activity_cycle_hook.output.data.retryCount}', 1]
                  - ['{@math.add}']

        activity_closer:
          title: Marks the activity workflow as done
          type: hook
          job:
            maps:
              done: true
              $error: '{activity_worker.output.data.$error}'
              jc: '{$job.metadata.jc}'
              ju:
                '@pipe':
                  - ['{@date.toISOXString}']

        activity_stopper:
          title: Stops the activity after retry count has been maxed
          type: hook
          job:
            maps:
              done: true
              $error: '{activity_worker.output.data.$error}'
              jc: '{$job.metadata.jc}'
              ju:
                '@pipe':
                  - ['{@date.toISOXString}']

      transitions:
        activity_trigger:
          - to: activity_cycle_hook
        activity_cycle_hook:
          - to: activity_worker
        activity_worker:
          - to: activity_closer
            conditions:
              code: [200, 598, 597, 596]
          - to: activity_stopper
            conditions:
              code: 599
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - '@pipe':
                        - ['{activity_cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{activity_trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.greater_than_or_equal}']

          - to: activity_retryer
            conditions:
              code: 599
              match:
                - expected: true
                  actual: 
                    '@pipe':
                      - '@pipe':
                        - ['{activity_cycle_hook.output.data.retryCount}']
                      - '@pipe':
                        - ['{activity_trigger.output.data.maximumAttempts}', 5]
                        - ['{@conditional.nullish}']
                      - ['{@conditional.less_than}']
        activity_retryer:
          - to: activity_retry_cycler
`;
};

export { getWorkflowYAML, APP_VERSION, APP_ID };
