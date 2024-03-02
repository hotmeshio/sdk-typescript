/**
 * NOTE: Using `maxSystemRetries = 3` and `backoffCoefficient = 10`, errant
 *       workflows will be retried on the following schedule (8 times in 27 hours):
 *       => 10ms, 100ms, 1000ms, 10s, 100s, 1_000s, 10_000s, 100_000s
 * TODO: Max Interval, Min Interval, Initial Interval
 *
 * ERROR CODES:
 *      594: waitforsignal
 *      592: sleepFor
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
      expire:
        '@pipe':
          - ['{t1.output.data.originJobId}', 0, '{t1.output.data.expire}']
          - ['{@conditional.ternary}']

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
            arguments:
              type: array
            workflowTopic:
              type: string
            backoffCoefficient:
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

      activities:
        t1:
          title: Main Flow Trigger
          type: trigger
          stats:
            id: '{$self.input.data.workflowId}'
            key: '{$self.input.data.parentWorkflowId}'
            parent: '{$self.input.data.originJobId}'
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
                originJobId:
                  type: string
                arguments:
                  type: array
            maps:
              workflowId: '{t1.output.data.workflowId}'
              originJobId: '{t1.output.data.originJobId}'
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
            592:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleepFor duration in seconds
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
                originJobId:
                  type: string
                workflowDimension:
                  type: string
                arguments:
                  type: array
            maps:
              workflowId: '{t1.output.data.workflowId}'
              originJobId: '{t1.output.data.originJobId}'
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
            592:
              schema:
                type: object
                properties:
                  duration:
                    type: number
                    description: sleepFor duration in seconds
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
                originJobId:
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
              originJobId:
                '@pipe':
                  - ['{t1.output.data.originJobId}', '{t1.output.data.originJobId}', '{$job.metadata.jid}']
                  - ['{@conditional.ternary}']
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

        siga592:
          title: Signal In - Sleep For a duration and then cycle/goto
          type: hook
          sleep: '{sigw1.output.data.duration}'

        sigc592:
          title: Signal In - Goto Activity a1 after sleeping for a duration
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
                originJobId:
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
              originJobId:
                '@pipe':
                  - ['{t1.output.data.originJobId}', '{t1.output.data.originJobId}', '{$job.metadata.jid}']
                  - ['{@conditional.ternary}']
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

        a592:
          title: Sleep For a duration and then cycle/goto
          type: hook
          sleep: '{w1.output.data.duration}'

        c592:
          title: Goto Activity a1 after sleeping for a duration
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
          - to: siga592
            conditions:
              code: 592
          - to: siga599
            conditions:
              code: 599
        siga594:
          - to: sigc594
        siga592:
          - to: sigc592
        siga599:
          - to: sigc599
        a1:
          - to: w1
        w1:
          - to: a594
            conditions:
              code: 594
          - to: a592
            conditions:
              code: 592
          - to: a599
            conditions:
              code: 599
          - to: s5
            conditions:
              code: [200, 598, 597, 596]
        a594:
          - to: c594
        a592:
          - to: c592
        a599:
          - to: c599

      hooks:
        ${app}.flow.signal:
          - to: sig
            conditions:
              match:
                - expected: '{t1.output.data.workflowId}'
                  actual: '{$self.hook.data.id}'

    - subscribes: ${app}.activity.execute
      publishes: ${app}.activity.executed

      expire: 0

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
            parent: '{$self.input.data.originJobId}'

        w1a:
          title: Activity Worker - Calls Activity Functions
          type: worker
          topic: '{t1a.output.data.workflowTopic}'
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

      transitions:
        t1a:
          - to: w1a

    - subscribes: ${app}.sleep.execute
      publishes: ${app}.sleep.executed

      expire: 0

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
            parent: '{$self.input.data.originJobId}'

        a1s:
          title: Sleep for a duration
          type: hook
          sleep: '{t1s.output.data.duration}'
          job:
            maps:
              done: true
              duration: '{t1s.output.data.duration}'
              index: '{t1s.output.data.index}'
              workflowId: '{t1s.output.data.workflowId}'

      transitions:
        t1s:
          - to: a1s

    - subscribes: ${app}.wfsc.execute
      publishes: ${app}.wfsc.executed

      expire: 0

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
            originJobId:
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
            parent: '{$self.input.data.originJobId}'

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
                signalId:
                  type: string
                  description: used to forge the custom hookid
                workflowId:
                  type: string
                  description: the baseId + index
            maps:
              parentWorkflowId: '{t1wc.output.data.parentWorkflowId}'
              originJobId: '{t1wc.output.data.originJobId}'
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

      expire: 0

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
            parent: '{$self.input.data.originJobId}'

        a1ww:
          title: WFS - signal entry point
          type: hook
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
              done: true

      transitions:
        t1ww:
          - to: a1ww

      hooks:
        ${app}.wfs.signal:
          - to: a1ww
            conditions:
              match:
                - expected: '{t1ww.output.data.signalId}'
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
