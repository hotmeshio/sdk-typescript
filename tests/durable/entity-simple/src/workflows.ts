import { Durable } from '../../../../services/durable';

/**
 * Simplest entity test: set a value and return it.
 */
export async function entitySetGet(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  await entity.set({ greeting: `hello ${name}` });
  const result = await entity.get('greeting');
  return result;
}

/**
 * Entity with merge: set then merge then return.
 */
export async function entityMerge(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  await entity.set({ user: { name } });
  await entity.merge({ user: { age: 30 } });
  const user = await entity.get('user');
  return user;
}

/**
 * Simple child that returns a value.
 */
export async function simpleChild(name: string): Promise<string> {
  return `child-${name}`;
}

/**
 * Parent that uses entity AND execChild together.
 */
export async function entityWithChild(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  await entity.set({ status: 'started' });

  const childResult = await Durable.workflow.execChild<string>({
    workflowName: 'simpleChild',
    args: [name],
    taskQueue: 'entity-q',
  });

  await entity.merge({ status: 'done', childResult });
  return await entity.get();
}

/**
 * Child that uses entity internally and returns structured data.
 */
export async function entityChild(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  await entity.set({ product: name, created: true });
  return await entity.get();
}

/**
 * Parent that calls execChild with entity param (like the failing test).
 */
export async function entityExecChildWithEntity(name: string): Promise<any> {
  const entity = await Durable.workflow.entity();
  await entity.set({ user: name });

  const childResult = await Durable.workflow.execChild<any>({
    entity: 'product',
    args: [name],
    taskQueue: 'entity-q',
    workflowName: 'entityChild',
  });

  await entity.merge({ childResult });
  return await entity.get();
}
