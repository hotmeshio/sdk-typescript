/**
 * No-op workflow stubs.
 *
 * These are registered by the READONLY container via Worker.create
 * with a readonly connection. They have the same function names and
 * same task queue as the real implementations on the worker container.
 *
 * Because the connection is readonly, the engine's router never
 * consumes stream messages — so these functions never actually
 * execute. They exist solely to prove that registering a worker
 * on a readonly connection does NOT cause the readonly server to
 * process jobs.
 *
 * If one of these ever runs, the test will fail because the return
 * value is a distinctive sentinel that the assertions check for.
 */

export async function readonlyExample(_name: string): Promise<string> {
  return 'NOOP_SHOULD_NEVER_APPEAR';
}

export async function robustWorkflow(
  _orderId: string,
): Promise<Record<string, any>> {
  return {
    status: 'NOOP_SHOULD_NEVER_APPEAR',
    steps: [],
    processed: 'NOOP',
    notification: 'NOOP',
  };
}

export async function metadataWorkflow(
  _name: string,
): Promise<Record<string, any>> {
  return {
    status: 'NOOP_SHOULD_NEVER_APPEAR',
    steps: [],
    activityResult: { data: 'NOOP', headers: {} },
  };
}
