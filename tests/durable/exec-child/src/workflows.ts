import { Durable } from '../../../../services/durable';

/**
 * Child that returns void (works).
 */
export async function childVoid(name: string): Promise<void> {
  return;
}

/**
 * Child that returns a string value.
 */
export async function childString(name: string): Promise<string> {
  return `hello ${name}`;
}

/**
 * Parent that calls a void child.
 */
export async function parentVoidChild(name: string): Promise<string> {
  await Durable.workflow.execChild<void>({
    workflowName: 'childVoid',
    args: [name],
    taskQueue: 'ec-queue',
  });
  return 'void-done';
}

/**
 * Parent that calls a string-returning child and uses the value.
 */
export async function parentStringChild(name: string): Promise<string> {
  const result = await Durable.workflow.execChild<string>({
    workflowName: 'childString',
    args: [name],
    taskQueue: 'ec-queue',
  });
  return `got: ${result}`;
}
