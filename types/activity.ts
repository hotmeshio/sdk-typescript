import { MetricTypes } from './stats';
import { StreamRetryPolicy } from './stream';

type ActivityExecutionType =
  | 'trigger'
  | 'await'
  | 'worker'
  | 'activity'
  | 'emit'
  | 'interrupt'
  | 'cycle'
  | 'signal'
  | 'hook';

type Consumes = Record<string, string[]>;

interface BaseActivity {
  title?: string;
  type?: ActivityExecutionType;
  subtype?: string;
  statusThreshold?: number; //default is 0; set to 1 to ensure not last standing; message will be ignored as if too late to process
  statusThresholdType?: 'stop' | 'throw' | 'stall'; //default is stop; must explicitly set to throw to throw an error or stall to stall
  input?: Record<string, any>;
  entity?: string; //for triggers the type
  output?: Record<string, any>;
  settings?: Record<string, any>;
  job?: Record<string, any>;
  hook?: Record<string, any>;
  telemetry?: Record<string, any>;
  emit?: boolean; //if true, the activity will emit a message to the `publishes` topic immediately before transitioning to adjacent activities
  sleep?: number; //@pipe /in seconds
  expire?: number; //-1 forever; 0 persists the flow until the parent flow that expired it is dismissed; 15 seconds is the default (copied from the YAML at compile time)
  persistent?: boolean; //if true, the job will persist beyond completion (copied from the YAML at compile time)
  persist?: boolean; //when true, the activity will emit the job completed event and start the `persistence countdown`, using the 'expire' value
  retry?: StreamRetryPolicy;
  cycle?: boolean; //if true, the `notary` will leave leg 2 open, so it can be re/cycled
  collationInt?: number; //compiler
  consumes?: Consumes; //compiler
  PRODUCES?: string[]; //compiler
  produces?: string[]; //compiler
  publishes?: string; //compiler
  subscribes?: string; //compiler
  trigger?: string; //compiler
  parent?: string; //compiler
  ancestors?: string[]; //compiler
}

interface Measure {
  measure: MetricTypes;
  target: string;
}

interface TriggerActivityStats {
  /**
   * dependent parent job id; including this allows the parent's
   * expiration/interruption events to cascade; set
   * `expire` in the YAML for the dependent graph
   * to 0 and provide the parent for dependent,
   * cascading interruption and cleanup
   */
  parent?: string;
  /**
   * adjacent parent job id; this is the actual adjacent
   * parent in the graph, but it is not used for cascading expiration
   */
  adjacent?: string;
  id?: { [key: string]: unknown } | string;
  key?: { [key: string]: unknown } | string;
  /**
   * @deprecated
   * return 'infinity' to disable
   */
  granularity?: string;
  /**
   * @deprecated
   * what to capture
   */
  measures?: Measure[];
}

interface TriggerActivity extends BaseActivity {
  type: 'trigger';
  stats?: TriggerActivityStats;
  entity?: string;
}

interface AwaitActivity extends BaseActivity {
  type: 'await';
  eventName: string;
  timeout: number;
  await?: boolean; //if exlicitly false do not await the response
}

interface WorkerActivity extends BaseActivity {
  type: 'worker';
  topic: string;
  timeout: number;
}

interface CycleActivity extends BaseActivity {
  type: 'cycle';
  ancestor: string; //ancestor activity id
}

interface HookActivity extends BaseActivity {
  type: 'hook';
}

interface SignalActivity extends BaseActivity {
  type: 'signal'; //signal activities call hook/hookAll
  subtype: 'one' | 'all'; //trigger: hook(One) or hookAll
  topic: string; //e.g., 'hook.resume'
  key_name?: string; //e.g., 'parent_job_id'
  key_value?: string; //e.g., '1234567890'
  scrub?: boolean; //if true, the index will be deleted after use
  signal?: Record<string, any>; //used to define/map the signal input data (what to send/singnal into the job(s))
  resolver?: Record<string, any>; //used to define/map the signal key resolver (the key used to lookup the job(s that are assigned to the key)
  status?: string; //pending, success (default), error
  code?: number; //202, 200 (default)
}

interface InterruptActivity extends BaseActivity {
  type: 'interrupt';

  /**
   * Optional Reason; will be used as the error `message` when thrown
   * @default 'Job Interrupted'
   */
  reason?: string;

  /**
   * throw JobInterrupted error upon interrupting
   * @default true
   */
  throw?: boolean;

  /**
   * Interrupt child/descendant jobs
   * @default false
   */
  descend?: boolean;

  /**
   * Target job id (if not present the current job will be targeted)
   */
  target?: string;

  /**
   * Optional topic to publish the interrupt message (if not present the current job topic will be used
   */
  topic?: string;

  /**
   * Optional Error Code; will be used as the error `code` when thrown
   * @default 410
   */
  code?: number;

  /**
   * Optional stack trace
   */
  stack?: string;
}

type ActivityType =
  | BaseActivity
  | TriggerActivity
  | AwaitActivity
  | WorkerActivity
  | InterruptActivity
  | HookActivity
  | SignalActivity
  | CycleActivity;

type ActivityData = Record<string, any>;

/**
 * Type definition for activity metadata.
 */
type ActivityMetadata = {
  /**
   * Unique identifier for the activity.
   */
  aid: string;

  /**
   * Type of the activity.
   */
  atp: string;

  /**
   * Subtype of the activity.
   */
  stp: string;

  /**
   * Timestamp when the activity was created, in GMT.
   */
  ac: string;

  /**
   * Timestamp when the activity was last updated, in GMT.
   */
  au: string;

  /**
   * Optional stringified JSON containing error details such as message, code, and an optional error object.
   */
  err?: string;

  /**
   * OpenTelemetry span context for the first leg of the activity.
   */
  l1s?: string;

  /**
   * OpenTelemetry span context for the second leg of the activity.
   */
  l2s?: string;

  /**
   * Dimensional address, used for additional metadata.
   */
  dad?: string;

  /**
   * Status of the activity, could be codes representing different states.
   */
  as?: string;
};

type ActivityContext = {
  data?: ActivityData | null;
  metadata: ActivityMetadata;
  hook?: ActivityData;
};

type ActivityDuplex = 1 | 2;

type ActivityDataType = {
  data?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  hook?: Record<string, unknown>;
};

type ActivityLeg = 1 | 2;

export {
  ActivityContext,
  ActivityData,
  ActivityDataType,
  ActivityDuplex,
  ActivityLeg,
  ActivityMetadata,
  ActivityType,
  Consumes,
  TriggerActivityStats,
  AwaitActivity,
  CycleActivity,
  HookActivity,
  SignalActivity,
  BaseActivity,
  InterruptActivity,
  TriggerActivity,
  WorkerActivity,
};
