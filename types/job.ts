import { StringStringType } from './serializer';

type JobData = Record<string, unknown | Record<string, unknown>>;
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

  /** sever the dependency chain if true (startChild/vs/execChild) */
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

  /** job status semaphore */
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

  /** job status threshold (if non-0) */
  threshold?: number;
};

/**
 * User-defined (extended) types for job data. Users may interleave
 * data into the job hash safely by using the `ExtensionType` interface.
 * The data will be prefixed as necessary using an underscore or
 * dash to ensure it is not confused with system process data.
 */
type ExtensionType = {
  /**
   * Custom search data field (name/value pairs) to seed the Hash.
   * Every field will be prefixed with an underscore before being
   * stored with the initial Hash data set along side system
   * process data.
   */
  search?: StringStringType;

  /**
   * Custom marker data field used for adding a searchable marker to the job.
   * markers always begin with a dash (-). Any field that does not
   * begin with a dash will be removed and will not be inserted with
   * the initial data set.
   */
  marker?: StringStringType;

  /**
   * Workflows that are 'pending' init with a status of `-1`.
   *
   * If provided, the job will initialize in a pending state, reserving
   * only the job ID (HSETNX) and persisting search and marker (if provided).
   * If a `resume` signal is sent before the specified number of seconds,
   * the job will resume as normal, transitioning to the adjacent children
   * of the trigger.
   *
   * If the job is not resumed within the number
   * of seconds specified, the job will be scrubbed. No dependencies
   * are added for a job in a pending state; however, dependencies
   * will be added after the job is resumed if relevant.
   */
  pending?: number;
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
  /**
   * optional Reason; will be used as the error `message` when thrown
   * @default 'Job Interrupted'
   */
  reason?: string;

  /**
   * throw JobInterrupted error upon interrupting
   * @default true
   */
  throw?: boolean;

  /**
   * interrupt child/descendant jobs
   * @default false
   */
  descend?: boolean;

  /**
   * if true, errors related to inactivation (like overage...already inactive) are suppressed/ignored
   * @default false
   */
  suppress?: boolean;

  /**
   * how long to wait in seconds before fully expiring/removing the hash from Redis;
   * the job is inactive, but can remain in the cache indefinitely;
   * @default 1 second.
   */
  expire?: number;

  /**
   * Optional Error Code; will be used as the error `code` when thrown
   * @default 410
   */
  code?: number;

  /**
   * Optional stack trace
   */
  stack?: string;
};

/**
 * format when publishing job meta/data on the wire when it completes
 */
type JobOutput = {
  metadata: JobMetadata;
  data: JobData;
};

/**
 * jid+dad+aid is a composite guid; signal in and restore the full job context
 */
type PartialJobState = {
  metadata: JobMetadata | Pick<JobMetadata, 'jid' | 'dad' | 'aid'>;
  data: JobData;
};

type JobCompletionOptions = {
  /** default false */
  emit?: boolean;

  /** default undefined */
  interrupt?: boolean;

  /**
   * in seconds to wait before deleting/expiring job hash
   * @default 1 second
   */
  expire?: number;
};

export {
  JobCompletionOptions,
  JobData,
  JobsData,
  JobInterruptOptions,
  JobMetadata,
  JobOutput,
  JobState,
  JobStatus,
  PartialJobState,
  ExtensionType,
};
