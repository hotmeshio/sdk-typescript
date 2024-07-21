import { MeshFlowFatalError } from '../../../../modules/errors';

export async function myFatalActivity(name: string): Promise<void> {
  throw new MeshFlowFatalError(`stop-retrying-please-${name}`);
}
