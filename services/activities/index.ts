import { Activity } from './activity';
import { Await } from './await';
import { Worker } from './worker';
import { Iterate } from './iterate';
import { Emit } from './emit';
import { Trigger } from './trigger';

export default { 
  activity: Activity,
  await: Await,
  iterate: Iterate,
  emit: Emit,
  trigger: Trigger,
  worker: Worker,
};
