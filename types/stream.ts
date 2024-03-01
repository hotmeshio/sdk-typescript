export interface StreamRetryPolicy {
  [key: string]: [number, 'x']; //key is err code, val is the retry profile [(max retry count),(type (x:exponential (default)) (only 10, 100, 1000, 10000 allowed))
}

export type StreamCode = number; //3-digit status code

export type StreamError = {
  message: string;
  code: number;
  job_id?: string; //used when communicating errors externally
  stack?: string;  //unhandled errors will have a stack
  name?: string;   //unhandled errors will have a name
  error?: Record<string, unknown>; //custom user-defined error details go here
}

export enum StreamStatus {
  SUCCESS = 'success',
  ERROR = 'error',
  PENDING = 'pending',
}

export enum StreamDataType {
  TIMEHOOK = 'timehook',
  WEBHOOK = 'webhook',
  AWAIT = 'await',
  RESULT = 'result', //await result
  WORKER = 'worker',
  RESPONSE = 'response', //worker response
  TRANSITION = 'transition',
  SIGNAL = 'signal',
  INTERRUPT = 'interrupt',
}

export interface StreamData {
  metadata: {
    guid: string;   //every message is minted with a guid to distinguish retries from new messages
    topic?: string;
    jid?: string;   //is optional if type is WEBHOOK (system assigned or user assigned)
    gid?: string;   //is optional if type is WEBHOOK (system assigned job guid)
    dad?: string;   //dimensional address
    aid: string;
    trc?: string;   //trace id
    spn?: string;   //span id
    try?: number;   //current try count
  };
  type?: StreamDataType;
  data: Record<string, unknown>;
  policies?: {
    retry?: StreamRetryPolicy;
  };
  status?: StreamStatus; //assume success
  code?: number;         //assume 200
}

export interface StreamDataResponse extends StreamData {}

export enum StreamRole {
  WORKER = 'worker',
  ENGINE = 'engine',
  SYSTEM = 'system', //reserved for system use (i.e, if worker or engine fails)
}

export type ReclaimedMessageType = [
  messageId: string, //stream id (e.g.,`<timestamp>-<count>`)
  details: [
    key: string,     //`key' is always 'message'
    value: string    //`value` is stringified StreamData (StreamDataType)
  ]
][];                 //wrapped in an outer array

export type StreamConfig = {
  namespace: string;
  appId: string;
  guid: string;
  role: StreamRole;
  topic?: string;
  reclaimDelay?: number; //default 60_000
  reclaimCount?: number; //default 3 (any value greater throws an error)
}
