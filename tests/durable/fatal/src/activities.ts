import { DurableFatalError } from '../../../../modules/errors';

export async function myFatalActivity(name: string): Promise<void> {
  throw new DurableFatalError(`stop-retrying-please-${name}`);
}
