type JobData = Record<string, unknown|Record<string, unknown>>;
type JobsData = Record<string, unknown>;

type ActivityData = {
  data: Record<string, unknown>;
  metadata?: Record<string, unknown>;
};

type JobMetadata = {
  /** job_key */
  key?: string;
  
  /** system assigned guid that corresponds to the transition message guid that spawned reentry */
  guid?: string;
  
  /** system assigned guid; ensured created/deleted/created jobs are unique */
  gid: string;
  
  /** job_id (jid+dad+aid) is composite key for activity */
  jid: string;
  
  /** dimensional address for the activity (,0,0,1) */
  dad: string;
  
  /** activity_id as in the YAML file */
  aid: string;
  
  /** parent_job_id (pj+pd+pa) is composite key for parent activity */
  pj?: string;
  
  /** parent_generational_id (system assigned at trigger inception); pg is the parent job's gid (just in case user created/deleted/created a job with same jid) */
  pg?: string;
  
  /** parent_dimensional_address */
  pd?: string;
  
  /** parent_activity_id */
  pa?: string;
  
  /** sever the dependency chain if true (startChild/vs/executeChild) */
  px?: boolean;
  
  /** engine guid (one time subscriptions) */
  ngn?: string;
  
  /** app_id */
  app: string;
  
  /** app version */
  vrs: string;
  
  /** subscription topic */
  tpc: string;
  
  /** 201203120005 (slice of time) //time series */
  ts: string;
  
  /** GMT created //job_created */
  jc: string;
  
  /** GMT updated //job_updated */
  ju: string;
  
  js: JobStatus;
  
  /** activity_type */
  atp: string;
  
  /** activity_subtype */
  stp: string;
  
  /** open telemetry span context */
  spn: string;
  
  /** open telemetry trace context */
  trc: string;
  
  /** stringified job error json: {message: string, code: number, error?} */
  err?: string;
  
  /** process data expire policy */
  expire?: number;
};

/**
 * job_status semaphore
 */
type JobStatus = number;

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
  /** default false */
  emit?: boolean;
  
  /** default undefined */
  interrupt?: boolean;
  
  /** in seconds to wait before deleting/expiring job hash */
  expire?: number;
};

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
