import { MetricTypes } from "./stats";
import { StreamRetryPolicy } from "./stream";

type ActivityExecutionType = 'trigger' | 'await' | 'worker' | 'activity' | 'emit' | 'iterate' | 'cycle';

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

interface EmitActivity extends BaseActivity {
  type: 'emit';
}

interface CycleActivity extends BaseActivity {
  type: 'cycle';
  ancestor: string; //ancestor activity id
}

interface IterateActivity extends BaseActivity {
  type: 'iterate';
}

type ActivityType = BaseActivity | TriggerActivity | AwaitActivity | WorkerActivity | EmitActivity | IterateActivity;

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
  BaseActivity,
  EmitActivity,
  IterateActivity,
  TriggerActivity,
  WorkerActivity
};
