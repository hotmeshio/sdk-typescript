import { MeshFlow } from '../../../../services/meshflow';

import * as activities from './activities';

const { count } = MeshFlow.workflow.proxyActivities<typeof activities>({
  activities,
  retryPolicy: {
    maximumAttempts: 2, //the succesful test retries twice.
    maximumInterval: '1s', //keep short for testing
    backoffCoefficient: 1, //keep short for testing
  },
});

async function example({ amount }): Promise<number> {
  return await count(amount);
}

export default { example };
