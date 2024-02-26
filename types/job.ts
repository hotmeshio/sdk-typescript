type JobData = Record<string, unknown|Record<string, unknown>>;
type JobsData = Record<string, unknown>;

type ActivityData = {
  data: Record<string, unknown>;
  metadata?: Record<string, unknown>;
};

type JobMetadata = {
  key?: string; //job_key
  jid: string;  //job_id (jid+dad+aid) is composite key for activity
  dad: string;  //dimensional address for the activity (,0,0,1)
  aid: string;  //activity_id as in the YAML file
  pj?: string;  //parent_job_id (pj+pd+pa) is composite key for parent activity
  pd?: string;  //parent_dimensional_address
  pa?: string;  //parent_activity_id
  ngn?: string; //engine guid (one time subscriptions)
  app: string;  //app_id
  vrs: string;  //app version
  tpc: string;  //subscription topic
  ts: string    //201203120005 (slice of time) //time series
  jc: string;   //GMT created //job_created
  ju: string;   //GMT updated //job_updated
  js: JobStatus;
  atp: string;  //activity_type
  stp: string;  //activity_subtype
  spn: string;  //open telemetry span context
  trc: string;  //open telemetry trace context
  err?: string; //stringified job error json: {message: string, code: number, error?}
  expire?: number; //process data expire policy
};

type JobStatus = number;       //job_status semaphore 

type JobState = {
  metadata: JobMetadata;
  data: JobData;
  [activityId: symbol]: {
    input: ActivityData;
    output: ActivityData;
    hook: ActivityData;
    settings: ActivityData;
    errors: ActivityData;
  };
};

type JobInterruptOptions = {
  /** Optional reason when throwing the error  */
  reason?: string;
  /** default is `true` when `undefined` (throw JobInterrupted/410 error) */
  throw?: boolean;
  /** default behavior is `false` when `undefined` (do NOT interrupt child jobs) */
  descend?: boolean;
  /** default is false; if true, errors related to inactivation (like overage...already inactive) are suppressed/ignored */
  suppress?: boolean;
  /** how long to wait in seconds before fully expiring/removing the hash from Redis; the job is inactive, but can remain in the cache indefinitely. minimum 1 second.*/
  expire?: number;
};

//format when publishing job meta/data on the wire when it completes
type JobOutput = {
  metadata: JobMetadata;
  data: JobData;
};

//jid+dad+aid is a composite guid; signal in and restore the full job context
type PartialJobState = {
  metadata: JobMetadata | Pick<JobMetadata, 'jid' | 'dad' | 'aid'>;
  data: JobData;
};

type JobCompletionOptions = {
  emit?: boolean;      //default false
  interrupt?: boolean;  //default undefined
  expire?: number;      // in seconds to wait before deleting/expiring job hash
}

export {
  JobCompletionOptions,
  JobInterruptOptions,
  JobData,
  JobsData,
  JobMetadata,
  JobOutput,
  JobState,
  JobStatus,
  PartialJobState,
};
