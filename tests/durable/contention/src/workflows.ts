/**
 * Assembly-line contention workflows — reproduces the collation poison
 * loop from the long-tail repo using only SDK primitives.
 *
 * stepIterator: parent that loops over data-driven steps, spawns a
 * child per step via startChild, waits via condition(signalId).
 *
 * workstation: child that runs an activity, then signals the parent
 * via a cross-workflow activity (signalParent). This is the exact
 * pattern that triggers the collation race under parallel Postgres
 * batch processing at scale.
 */

import { Durable } from '../../../../services/durable';
import * as activities from './activities';

type ActivitiesType = typeof activities;

const { processStation, signalParent } =
  Durable.workflow.proxyActivities<ActivitiesType>({ activities });

interface StepConfig {
  stationName: string;
  role: string;
}

interface StationResult {
  stationName: string;
  completedAt: string;
}

const TASK_QUEUE = 'contention-test';

export async function stepIterator(
  name: string,
  steps: StepConfig[],
): Promise<{ name: string; results: StationResult[] }> {
  const ctx = Durable.workflow.workflowInfo();
  const results: StationResult[] = [];

  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const signalId = `step-${i}-${ctx.workflowId}`;
    const childWorkflowId = `ws-${ctx.workflowId}-${i}`;

    await Durable.workflow.startChild({
      workflowName: 'workstation',
      args: [
        step.stationName,
        step.role,
        signalId,
        TASK_QUEUE,
        'stepIterator',
        ctx.workflowId,
      ],
      taskQueue: TASK_QUEUE,
      workflowId: childWorkflowId,
      expire: 600,
    });

    const result = await Durable.workflow.condition<StationResult>(
      signalId,
    );
    if (result === false) {
      throw new Error(`Timed out waiting for step ${i} (${step.stationName})`);
    }
    results.push(result);
  }

  return { name, results };
}

export async function workstation(
  stationName: string,
  role: string,
  parentSignalId: string,
  parentTaskQueue: string,
  parentWorkflowName: string,
  parentWorkflowId: string,
): Promise<StationResult> {
  // Run the station activity
  const result = await processStation({ stationName, role });

  // Signal the parent that this step is complete
  await signalParent({
    parentTaskQueue,
    parentWorkflowName,
    parentWorkflowId,
    signalId: parentSignalId,
    data: result,
  });

  return result;
}
