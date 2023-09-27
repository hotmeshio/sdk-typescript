import { ClientService } from './client';
import { ConnectionService } from './connection';
import { NativeConnectionService } from './native';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { ContextType } from '../../types/durable';

/**
 * As a durable integration platform, HotMesh
 * can model and emulate other durable systems
 * (like Temporal). As you review the code in
 * this file, note the following:
 * 
 * 1) There is no central governing server.
 * HotMesh is a client-side SDK that connects to Redis
 * using CQRS principles to implicitly drive
 * orchestrations using a headless quorum. Stream
 * semantics guarantee that all events are
 * processed by the quorum of connected clients.
 * 
 * 2) Every developer-defined `workflow` function
 * is assigned a HotMesh workflow (which runs in
 * the background) to support it.
 * 
 * If the HotMesh workflow is not yet defined,
 * it will be deployed and activated on-the-fly.
 * (The generated DAG will have one Trigger Activity
 * and one Worker Activity.) The Worker Activity
 * is configured to catch execution errors and
 * return them, using a 'pending' status to
 * indicate that the workflow is still running.
 * It is possible for workflow activities to throw
 * errors that will force the entire workflow to
 * fail. This is not the case here. The workflow will
 * continue to run until it is completed or cancelled.
 * 
 * 2) Every developer-defined `activity` function
 * is assigned a HotMesh workflow (which runs in
 * the background) to support it.
 * 
 * (The generated DAG will have one Trigger Activity and one
 * Worker Activity.) The JOB ID for Worker Activity executions
 * is derived from the containing JOB ID,
 * allowing Activity state to be 'replayed' when the workflow
 * is run again. The Activity Function Runner (activity proxy)
 * is configured similar to how the workflow worker is and will
 * catch execution errors and return them to the caller, using a
 * 'pending' status to indicate that the activity is still running.
 * This allows the activity to be retried until it succeeds.
 */

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  NativeConnection: NativeConnectionService,
  Worker: WorkerService,
  workflow: WorkflowService,
};

export type { ContextType };
