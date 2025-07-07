import { MemFlow } from '../../../../services/memflow';

import * as activities from './activities';

const { myFatalActivity } = MemFlow.workflow.proxyActivities<
  typeof activities
>({ activities });

async function example({ name }: Record<'name', string>): Promise<void> {
  try {
    return await myFatalActivity(name);
  } catch (error) {
    //this error is thrown to reveal the error / stack trace feature
    // when activity execution fails on a remote host
    console.error('rethrowing error >', error);
    throw error;
  }
}

export default { example };
