import { MemFlowFatalError } from '../../../../modules/errors';

export async function myFatalActivity(name: string): Promise<void> {
  throw new MemFlowFatalError(`stop-retrying-please-${name}`);
}
