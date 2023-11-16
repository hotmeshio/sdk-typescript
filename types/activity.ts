import { MetricTypes } from "./stats";
import { StreamRetryPolicy } from "./stream";

type ActivityExecutionType = 'trigger' | 'await' | 'worker' | 'activity' | 'emit' | 'iterate' | 'cycle' | 'signal' | 'hook';

type Consumes = Record<string, string[]>;

interface BaseActivity {
  title?: string;
  type?: ActivityExecutionType;
  subtype?: string;
  input?: Record<string, any>;
  output?: Record<string, any>;
  settings?: Record<string, any>;
  job?: Record<string, any>;
  hook?: Record<string, any>;
  telemetry?: Record<string, any>;
  emit?: boolean;                      //if true, the activity will emit a message to the `publishes` topic immediately before transitioning to adjacent activities
  sleep?: number;                      //@pipe /in seconds
  expire?: number;                     //-1 forever (15 seconds default); todo: make globally configurable
  retry?: StreamRetryPolicy
  cycle?: boolean;                     //if true, the `notary` will leave leg 2 open, so it can be re/cycled
  collationInt?: number;               //compiler
  consumes?: Consumes;                 //compiler
  PRODUCES?: string[];                 //compiler
  produces?: string[];                 //compiler
  publishes?: string;                  //compiler 
  subscribes?: string;                 //compiler
  trigger?: string;                    //compiler
  parent?: string;                     //compiler
  ancestors?: string[];                //compiler
}

interface Measure {
  measure: MetricTypes;
  target: string;
}

interface TriggerActivityStats {
  id?: { [key: string]: unknown } | string;
  key?: { [key: string]: unknown } | string;
  granularity?: string; //return 'infinity' to disable; default behavior is to always segment keys by time to ensure indexes (Redis LIST) never grow unbounded as a default behavior; for now, 5m is default and infinity can be set to override
  measures?: Measure[]; //what to capture
}

interface TriggerActivity extends BaseActivity {
  type: 'trigger';
  stats?: TriggerActivityStats;
}

interface AwaitActivity extends BaseActivity {
  type: 'await';
  eventName: string;
  timeout: number;
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
  type: 'signal';         //signal activities call hook/hookAll
  subtype: 'one' | 'all'; //trigger: hook(One) or hookAll
  topic: string;          //e.g., 'hook.resume'
  key_name: string;       //e.g., 'parent_job_id'
  key_value: string;      //e.g., '1234567890'
  scrub: boolean;         //if true, the index will be deleted after use
  signal?: Record<string, any>;   //used to define/map the signal input data
  resolver?: Record<string, any>; //used to define/map the signal key resolver
}

interface IterateActivity extends BaseActivity {
  type: 'iterate';
}

type ActivityType = BaseActivity | TriggerActivity | AwaitActivity | WorkerActivity | IterateActivity | HookActivity;

type ActivityData = Record<string, any>;
type ActivityMetadata = {
  aid: string;  //activity_id
  atp: string;  //activity_type
  stp: string;  //activity_subtype
  ac: string;   //GMT created //activity_created
  au: string;   //GMT updated //activity_updated
  err?: string; //stringified error json: {message: string, code: number, error?}
  l1s?: string; //open telemetry span context (leg 1)
  l2s?: string; //open telemetry span context (leg 2)
  dad?: string; //dimensional address
  as?: string;  //activity status (e.g., 889000001000001)
};

type ActivityContext = {
  data?: ActivityData | null;
  metadata: ActivityMetadata;
  hook?: ActivityData
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
  IterateActivity,
  TriggerActivity,
  WorkerActivity
};
