/**
 * NOTE: Using `maxSystemRetries = 3` and `backoffCoefficient = 10`, errant
 *       workflows will be retried on the following schedule (8 times in 27 hours):
 *       => 10ms, 100ms, 1000ms, 10s, 100s, 1_000s, 10_000s, 100_000s
 * TODO: Max Interval, Min Interval, Initial Interval
 *
 * ERROR CODES:
 *      594: waitforsignal
 *      595: sleep
 *      596, 597, 598: fatal
 *      599: retry
 */
const getWorkflowYAML = (app: string, version: string) => {
  return `app:
  id: ${app}
  version: '${version}'
  graphs:
    - subscribes: ${app}.execute
      publishes: ${app}.executed
      expire: 120
      input:
        schema:
          type: object
          properties:
            parentWorkflowId:
              type: string
            workflowId:
              type: string
            arguments:
              type: array
            workflowTopic:
              type: string
            backoffCoefficient:
              type: number
      output:
        schema:
          type: object
          properties:
            response:
              type: any
            done:
              type: boolean

      activities:
        t1:
          title: Main Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            granularity: infinity
            measures:
              - measure: index
                target: '{$self.input.data.parentWorkflowId}'
          job:
            maps:
              done: false

        a1:
          title: Main Flow Pivot - All Cycling Descendants Point Here
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                duration:
                  type: number
            maps:
              duration: '{t1.output.data.backoffCoefficient}'

        w1:
          title: Main Worker - Calls Workflow Functions
          type: worker
          topic: '{t1.output.data.workflowTopic}'
          emit: '{$job.data.done}'
          retry:
            '599': [2]
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
            594:
              schema:
                type: object
                properties:
                  index:
                    type: number
                    description: the index of the first signal in the array
                  signals:
                    type: array
                    description: remaining signal ids
                    items:
                      type: object
                      properties:
                        signal:
                          type: string
                        index:
                          type: number
              maps:
                index: '{$self.output.data.index}'
                signals: '{$self.output.data.signals}'
            595:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleep duration in seconds
                  index:
                    type: number
                    description: the current index
              maps:
                duration: '{$self.output.data.duration}'
                index: '{$self.output.data.index}'
          job:
            maps:
              response: '{$self.output.data.response}'
              done: '{$self.output.data.done}'

        a2:
          title: Wait for cleanup signal
          type: hook
          hook:
            type: object
            properties:
              done:
                type: boolean
          job:
            maps:
              workflowId: '{t1.output.data.workflowId}'

        sig:
          title: Signal In - Receive signals
          type: hook
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
              workflowId: '{t1.output.data.workflowId}'
    
        siga1:
          title: Signal In Flow Pivot - Cycling Descendants Point Here
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                duration:
                  type: number
            maps:
              duration: '{t1.output.data.backoffCoefficient}'
    
        sigw1:
          title: Signal In - Worker
          type: worker
          topic: '{sig.hook.data.workflowTopic}'
          retry:
            '599': [2]
          input:
            schema:
              type: object
              properties:
                workflowId:
                  type: string
                workflowDimension:
                  type: string
                arguments:
                  type: array
            maps:
              workflowId: '{t1.output.data.workflowId}'
              workflowDimension: '{sig.output.metadata.dad}'
              arguments: '{sig.hook.data.arguments}'
          output:
            schema:
              type: object
            594:
              schema:
                type: object
                properties:
                  index:
                    type: number
                    description: the index of the first signal in the array
                  signals:
                    type: array
                    description: remaining signal ids
                    items:
                      type: object
                      properties:
                        signal:
                          type: string
                        index:
                          type: number
              maps:
                index: '{$self.output.data.index}'
                signals: '{$self.output.data.signals}'
            595:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleep duration in seconds
                  index:
                    type: number
                    description: the current index
              maps:
                duration: '{$self.output.data.duration}'
                index: '{$self.output.data.index}'

        siga594:
          title: Signal In - Wait for signals
          type: await
          topic: ${app}.wfsc.execute
          input:
            schema:
              type: object
              properties:
                index:
                  type: number
                signals:
                  type: array
                  description: signal ids
                  items:
                    type: object
                    properties:
                      signal:
                        type: string
                      index:
                        type: number
                parentWorkflowId:
                  type: string
                cycleWorkflowId:
                  type: string
                baseWorkflowId:
                  type: string
                  description: index will be appended later
            maps:
              signals: '{sigw1.output.data.signals}'
              parentWorkflowId:
                '@pipe':
                  - ['{$job.metadata.jid}', '-w']
                  - ['{@string.concat}']
              cycleWorkflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$wfc', '{sig.output.metadata.dad}', '-', '{sigw1.output.data.index}']
                  - ['{@string.concat}']
              baseWorkflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$wfs', '{sig.output.metadata.dad}', '-']
                  - ['{@string.concat}']
          output:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: '{sigw1.output.data.done}'

        sigc594:
          title: Signal In - Goto Activity siga1
          type: cycle
          ancestor: siga1
          input:
            maps:
              duration: '{siga1.output.data.duration}'

        siga595:
          title: Signal In - Sleep before trying again
          type: await
          topic: ${app}.sleep.execute
          input:
            schema:
              type: object
              properties:
                duration:
                  type: number
                index:
                  type: number
                workflowId:
                  type: string
                parentWorkflowId:
                  type: string
            maps:
              duration: '{sigw1.output.data.duration}'
              index: '{sigw1.output.data.index}'
              parentWorkflowId:
                '@pipe':
                  - ['{$job.metadata.jid}', '-s']
                  - ['{@string.concat}']
              workflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$sleep', '{sig.output.metadata.dad}', '-', '{sigw1.output.data.index}']
                  - ['{@string.concat}']
          output:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: '{sigw1.output.data.done}'

        sigc595:
          title: Signal In - Goto Activity siga1
          type: cycle
          ancestor: siga1
          input:
            maps:
              duration: '{siga1.output.data.duration}'

        siga599:
          title: Signal In - Sleep exponentially longer and retry
          type: hook
          sleep: '{siga1.output.data.duration}'

        sigc599:
          title: Signal In - Goto Activity siga1
          type: cycle
          ancestor: siga1
          input:
            maps:
              duration:
                '@pipe':
                  - ['{siga1.output.data.duration}', '{t1.output.data.backoffCoefficient}']
                  - ['{@math.multiply}']

        a594:
          title: Wait for signals
          type: await
          topic: ${app}.wfsc.execute
          input:
            schema:
              type: object
              properties:
                index:
                  type: number
                signals:
                  type: array
                  description: signal ids
                  items:
                    type: object
                    properties:
                      signal:
                        type: string
                      index:
                        type: number
                parentWorkflowId:
                  type: string
                cycleWorkflowId:
                  type: string
                baseWorkflowId:
                  type: string
                  description: index will be appended later
            maps:
              signals: '{w1.output.data.signals}'
              parentWorkflowId:
                '@pipe':
                  - ['{$job.metadata.jid}', '-w']
                  - ['{@string.concat}']
              cycleWorkflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$wfc-', '{w1.output.data.index}']
                  - ['{@string.concat}']
              baseWorkflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$wfs-']
                  - ['{@string.concat}']
          output:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: '{w1.output.data.done}'

        c594:
          title: Goto Activity a1
          type: cycle
          ancestor: a1
          input:
            maps:
              duration: '{a1.output.data.duration}'

        a595:
          title: Sleep before trying again
          type: await
          topic: ${app}.sleep.execute
          input:
            schema:
              type: object
              properties:
                duration:
                  type: number
                index:
                  type: number
                workflowId:
                  type: string
                parentWorkflowId:
                  type: string
            maps:
              duration: '{w1.output.data.duration}'
              index: '{w1.output.data.index}'
              parentWorkflowId:
                '@pipe':
                  - ['{$job.metadata.jid}', '-s']
                  - ['{@string.concat}']
              workflowId:
                '@pipe':
                  - ['-', '{$job.metadata.jid}', '-$sleep-', '{w1.output.data.index}']
                  - ['{@string.concat}']
          output:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: '{w1.output.data.done}'

        c595:
          title: Goto Activity a1
          type: cycle
          ancestor: a1
          input:
            maps:
              duration: '{a1.output.data.duration}'

        a599:
          title: Sleep exponentially longer before retrying
          type: hook
          sleep: '{a1.output.data.duration}'

        c599:
          title: Goto Activity a1
          type: cycle
          ancestor: a1
          input:
            maps:
              duration:
                '@pipe':
                  - ['{a1.output.data.duration}', '{t1.output.data.backoffCoefficient}']
                  - ['{@math.multiply}']

        s1:
          title: Awaken activity flows so they end and self-clean
          type: signal
          subtype: all
          key_name: parentWorkflowId
          key_value:
            '@pipe':
              - ['{$job.metadata.jid}', '-a']
              - ['{@string.concat}']
          topic: ${app}.activity.awaken
          resolver:
            schema:
              type: object
              properties:
                data:
                  type: object
                  properties:
                    parentWorkflowId:
                      type: string
                scrub:
                  type: boolean
            maps:
              data:
                parentWorkflowId:
                  '@pipe':
                    - ['{$job.metadata.jid}', '-a']
                    - ['{@string.concat}']

              scrub: true
          signal:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: true

        s2:
          title: Awaken sleeping flows so they end and self-clean
          type: signal
          subtype: all
          key_name: parentWorkflowId
          key_value:
            '@pipe':
              - ['{$job.metadata.jid}', '-s']
              - ['{@string.concat}']
          topic: ${app}.sleep.awaken
          resolver:
            schema:
              type: object
              properties:
                data:
                  type: object
                  properties:
                    parentWorkflowId:
                      type: string
                scrub:
                  type: boolean
            maps:
              data:
                parentWorkflowId:
                  '@pipe':
                    - ['{$job.metadata.jid}', '-s']
                    - ['{@string.concat}']
              scrub: true
          signal:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: true

        s3:
          title: Awaken WFS flows so they end and self-clean
          type: signal
          subtype: all
          key_name: parentWorkflowId
          key_value:
            '@pipe':
              - ['{$job.metadata.jid}', '-w']
              - ['{@string.concat}']
          topic: ${app}.wfs.awaken
          resolver:
            schema:
              type: object
              properties:
                data:
                  type: object
                  properties:
                    parentWorkflowId:
                      type: string
                scrub:
                  type: boolean
            maps:
              data:
                parentWorkflowId:
                  '@pipe':
                    - ['{$job.metadata.jid}', '-w']
                    - ['{@string.concat}']
              scrub: true
          signal:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: true

        s4:
          title: Awaken child flows so they end and self-clean
          type: signal
          subtype: all
          key_name: parentWorkflowId
          key_value:
            '@pipe':
              - ['{$job.metadata.jid}', '-f']
              - ['{@string.concat}']
          topic: ${app}.childflow.awaken
          resolver:
            schema:
              type: object
              properties:
                data:
                  type: object
                  properties:
                    parentWorkflowId:
                      type: string
                scrub:
                  type: boolean
            maps:
              data:
                parentWorkflowId:
                  '@pipe':
                    - ['{$job.metadata.jid}', '-f']
                    - ['{@string.concat}']
              scrub: true
          signal:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: true
        s5:
          title: Close Signal In Channel
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
    
      transitions:
        t1:
          - to: a1
          - to: a2
            conditions:
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - ['{$job.metadata.key}', true, false]
                      - ['{@conditional.ternary}']
          - to: sig
        sig:
          - to: siga1
            conditions:
              code: 202
        siga1:
          - to: sigw1
        sigw1:
          - to: siga594
            conditions:
              code: 594
          - to: siga595
            conditions:
              code: 595
          - to: siga599
            conditions:
              code: 599
        siga594:
          - to: sigc594
            conditions:
              code: 202
        siga595:
          - to: sigc595
            conditions:
              code: 202
        siga599:
          - to: sigc599
        a1:
          - to: w1
        w1:
          - to: a594
            conditions:
              code: 594
          - to: a595
            conditions:
              code: 595
          - to: a599
            conditions:
              code: 599
          - to: s3
            conditions:
              code: [200, 598, 597, 596]
          - to: s1
            conditions:
              code: [200, 598, 597, 596]
          - to: s2
            conditions:
              code: [200, 598, 597, 596]
          - to: s4
            conditions:
              code: [200, 598, 597, 596]
          - to: s5
            conditions:
              code: [200, 598, 597, 596]
        a594:
          - to: c594
            conditions:
              code: 202
        a595:
          - to: c595
            conditions:
              code: 202
        a599:
          - to: c599

      hooks:
        ${app}.childflow.awaken:
          - to: a2
            conditions:
              match:
                - expected: '{t1.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'

        ${app}.flow.signal:
          - to: sig
            conditions:
              match:
                - expected: '{t1.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'

    - subscribes: ${app}.activity.execute
      publishes: ${app}.activity.executed

      expire: 120

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
      output:
        schema:
          type: object
          properties:
            response:
              type: any
            done:
              type: boolean

      activities:
        t1a:
          title: Activity Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            granularity: infinity
            measures:
              - measure: index
                target: '{$self.input.data.parentWorkflowId}'

        w1a:
          title: Activity Worker - Calls Activity Functions
          type: worker
          topic: '{t1a.output.data.workflowTopic}'
          emit: true
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
              parentWorkflowId: '{t1a.output.data.parentWorkflowId}'
              workflowId: '{t1a.output.data.workflowId}'
              workflowTopic: '{t1a.output.data.workflowTopic}'
              activityName: '{t1a.output.data.activityName}'
              arguments: '{t1a.output.data.arguments}'
          output:
            schema:
              type: object
              properties:
                response:
                  type: any
          job:
            maps:
              response: '{$self.output.data.response}'
              done: true

        s1a:
          title: Awaken activity flows so they end and self-clean
          type: hook
          hook:
            type: object
            properties:
              done:
                type: boolean
          job:
            maps:
              workflowId: '{t1a.output.data.workflowId}'

      transitions:
        t1a:
          - to: w1a
        w1a:
          - to: s1a

      hooks:
        ${app}.activity.awaken:
          - to: s1a
            conditions:
              match:
                - expected: '{t1a.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'

    - subscribes: ${app}.sleep.execute
      publishes: ${app}.sleep.executed

      expire: 120

      input:
        schema:
          type: object
          properties:
            parentWorkflowId:
              type: string
            workflowId:
              type: string
            duration:
              type: number
              description: in seconds
            index:
              type: number
      output:
        schema:
          type: object
          properties:
            done:
              type: boolean
            duration:
              type: number
            index:
              type: number

      activities:
        t1s:
          title: Sleep Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            granularity: infinity
            measures:
              - measure: index
                target: '{$self.input.data.parentWorkflowId}'

        a1s:
          title: Sleep for a duration
          type: hook
          sleep: '{t1s.output.data.duration}'
          emit: true

        a2s:
          title: Awaken sleep flows so they end and self-clean
          type: hook
          hook:
            type: object
            properties:
              done:
                type: boolean
          job:
            maps:
              done: true
              duration: '{t1s.output.data.duration}'
              index: '{t1s.output.data.index}'
              workflowId: '{t1s.output.data.workflowId}'
  
      transitions:
        t1s:
          - to: a1s
        a1s:
          - to: a2s

      hooks:
        ${app}.sleep.awaken:
          - to: a2s
            conditions:
              match:
                - expected: '{t1s.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'

    - subscribes: ${app}.wfsc.execute
      publishes: ${app}.wfsc.executed

      expire: 120

      input:
        schema:
          type: object
          properties:
            index:
              type: number
            signals:
              type: array
              description: signal ids
              items:
                type: object
                properties:
                  signal:
                    type: string
                  index:
                    type: number
            parentWorkflowId:
              type: string
            cycleWorkflowId:
              type: string
            baseWorkflowId:
              type: string
              description: index will be appended later
      output:
        schema:
          type: object
          properties:
            done:
              type: boolean

      activities:
        t1wc:
          title: Cycler workflow that creates signal workflows
          type: trigger
          stats:
            id: '{$self.input.data.cycleWorkflowId}'

        a1wc:
          title: Pivot - All Cycling Descendants Point Here
          type: hook
          cycle: true
          output:
            schema:
              type: object
              properties:
                targetLength:
                  type: number
                targetSignal:
                  type: object
                  properties:
                    signal:
                      type: string
                    index:
                      type: number
                signals:
                  type: array
                  items:
                    type: object
                    properties:
                      signal:
                        type: string
                      index:
                        type: number
            maps:
              targetLength:
                '@pipe':
                  - ['{t1wc.output.data.signals}']
                  - ['{@array.length}']        
              targetSignal:
                '@pipe':
                  - ['{t1wc.output.data.signals}', 0]
                  - ['{@array.get}']
              signals:
                '@pipe':
                  - ['{t1wc.output.data.signals}', 1]
                  - ['{@array.slice}']
        a2wc:
          title: Precalculate targetLength
          type: hook
          output:
            schema:
              type: object
              properties:
                targetLength:
                  type: number
            maps:
              targetLength: '{a1wc.output.data.targetLength}'
 
        c1wc:
          title: Goto Activity a1wc - Spawn Signal children
          type: cycle
          ancestor: a1wc
          input:
            maps:
              targetLength:
                '@pipe':
                  - ['{a1wc.output.data.signals}']
                  - ['{@array.length}']
              targetSignal:
                '@pipe':
                  - ['{a1wc.output.data.signals}', 0]
                  - ['{@array.get}']
              signals:
                '@pipe':
                  - ['{a1wc.output.data.signals}', 1]
                  - ['{@array.slice}']

        a3wc:
          title: Call WFS workflow
          type: await
          topic: ${app}.wfs.execute
          emit: true
          input:
            schema:
              type: object
              properties:
                parentWorkflowId:
                  type: string
                  description: used to forge the cleanup key
                signalId:
                  type: string
                  description: used to forge the custom hookid
                workflowId:
                  type: string
                  description: the baseId + index
            maps:
              parentWorkflowId: '{t1wc.output.data.parentWorkflowId}'
              signalId: '{a1wc.output.data.targetSignal.signal}'
              workflowId:
                '@pipe':
                  - ['{t1wc.output.data.baseWorkflowId}', '{a1wc.output.data.targetSignal.index}']
                  - ['{@string.concat}']
          output:
            schema:
              type: object
              properties:
                done:
                  type: boolean
            maps:
              done: '{w1.output.data.done}'
    
      transitions:
        t1wc:
          - to: a1wc
        a1wc:
          - to: a2wc
        a2wc:
          - to: c1wc
            conditions:
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - ['{a1wc.output.data.targetLength}', 0]
                      - ['{@conditional.greater_than}']
          - to: a3wc
            conditions:
              match:
                - expected: true
                  actual:
                    '@pipe':
                      - ['{a1wc.output.data.targetLength}', 0]
                      - ['{@conditional.greater_than}']

    - subscribes: ${app}.wfs.execute
      publishes: ${app}.wfs.executed

      expire: 120

      input:
        schema:
          type: object
          properties:
            parentWorkflowId:
              type: string
              description: used to forge the cleanup key
            workflowId:
              type: string
              description: used to forge the cleanup hookid
            signalId:
              type: string
              description: used to forge the custom hookid
      output:
        schema:
          type: object
          properties:
            done:
              type: boolean
            workflowId:
              type: string
            signalData:
              type: object

      activities:
        t1ww:
          title: WFS - Wait For Signal Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            granularity: infinity
            measures:
              - measure: index
                target: '{$self.input.data.parentWorkflowId}'

        a1ww:
          title: WFS - signal entry point
          type: hook
          emit: true
          hook:
            type: object
            properties:
              signalData:
                type: object
          job:
            maps:
              signalData: '{$self.hook.data}'
              workflowId: '{t1ww.output.data.workflowId}'
              signalId: '{t1ww.output.data.signalId}'

        a2ww:
          title: WFS - cleanup signal entry point
          type: hook
          hook:
            type: object
            properties:
              done:
                type: boolean
          job:
            maps:
              done: true
              workflowId: '{t1ww.output.data.workflowId}'

      transitions:
        t1ww:
          - to: a1ww
          - to: a2ww

      hooks:
        ${app}.wfs.signal:
          - to: a1ww
            conditions:
              match:
                - expected: '{t1ww.output.data.signalId}'
                  actual: '{$self.hook.data.id}'
        ${app}.wfs.awaken:
          - to: a2ww
            conditions:
              match:
                - expected: '{t1ww.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'
                
`;
};

const APP_VERSION = '1';
const APP_ID = 'durable';
const DEFAULT_COEFFICIENT = 10;

export {
  getWorkflowYAML,
  APP_VERSION,
  APP_ID,
  DEFAULT_COEFFICIENT,
};
