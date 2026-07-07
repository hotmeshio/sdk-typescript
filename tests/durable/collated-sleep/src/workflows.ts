import { Durable } from '../../../../services/durable';

export async function castMember(name: string): Promise<string> {
  return `cast:${name}`;
}

/**
 * The field topology: a sleep as one branch of a wide parallel await
 * (N executeChild + a sleeping branch in one Promise.all). The sleep's
 * timehook must be minted for the collated sleeper leg and the converge
 * must settle once the timer fires and the children complete.
 */
export async function convergeWithSleep(runId: string): Promise<string> {
  const children = [1, 2, 3, 4].map((i) =>
    Durable.workflow.executeChild<string>({
      workflowName: 'castMember',
      args: [`${runId}-c${i}`],
      taskQueue: 'converge-queue',
    }),
  );
  const runAndRetire = async (): Promise<string> => {
    await Durable.workflow.sleep('15 seconds');
    return 'retired';
  };
  const results = await Promise.all([...children, runAndRetire()]);
  return results.join('|');
}
