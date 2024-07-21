import { HotMesh } from '../hotmesh';
import { MeshFlowJobExport, ExportOptions } from '../../types/exporter';
import { JobInterruptOptions, JobOutput } from '../../types/job';
import { StreamError } from '../../types/stream';

import { ExporterService } from './exporter';

/**
 * The WorkflowHandleService provides methods to interact with a running
 * workflow. This includes exporting the workflow, sending signals, and
 * querying the state of the workflow. It is instanced/accessed via the
 * MeshFlow.Client class.
 * 
 * @example
 * ```typescript
 * import { MeshFlow } from '@hotmeshio/hotmesh';
 * 
 * const client = new MeshFlow.Client({ connection: { class: Redis, options } });
 * const handle = await client.workflow.start({
 *  args: ['HotMesh'],
 * taskQueue: 'hello-world',
 * });
 */
export class WorkflowHandleService {
  exporter: ExporterService;
  hotMesh: HotMesh;
  workflowTopic: string;
  workflowId: string;

  /**
   * @private
   */
  constructor(hotMesh: HotMesh, workflowTopic: string, workflowId: string) {
    this.workflowTopic = workflowTopic;
    this.workflowId = workflowId;
    this.hotMesh = hotMesh;
    this.exporter = new ExporterService(
      this.hotMesh.appId,
      this.hotMesh.engine.store,
      this.hotMesh.engine.logger,
    );
  }

  /**
   * Exports the workflow state to a JSON object.
   */
  async export(options?: ExportOptions): Promise<MeshFlowJobExport> {
    return this.exporter.export(this.workflowId, options);
  }

  /**
   * Sends a signal to the workflow. This is a way to send
   * a message to a workflow that is paused due to having
   * executed `MeshFlow.workflow.waitFor`. The workflow
   * will awaken if no other signals are pending.
   */
  async signal(signalId: string, data: Record<any, any>): Promise<void> {
    await this.hotMesh.hook(`${this.hotMesh.appId}.wfs.signal`, {
      id: signalId,
      data,
    });
  }

  /**
   * Returns the job state of the workflow. If the workflow has completed
   * this is also the job output. If the workflow is still running, this
   * is the current state of the job, but it may change depending upon
   * the activities that remain.
   */
  async state(metadata = false): Promise<Record<string, any>> {
    const state = await this.hotMesh.getState(
      `${this.hotMesh.appId}.execute`,
      this.workflowId,
    );
    if (!state.data && state.metadata.err) {
      throw new Error(JSON.parse(state.metadata.err));
    }
    return metadata ? state : state.data;
  }

  /**
   * Returns the current search state of the workflow. This is
   * different than the job state or individual activity state.
   * Search state represents name/value pairs that were added
   * to the workflow. As the workflow is stored in a Redis hash,
   * this is a way to store additional data that is indexed
   * and searchable using the RediSearch module.
   */
  async queryState(fields: string[]): Promise<Record<string, any>> {
    return await this.hotMesh.getQueryState(this.workflowId, fields);
  }

  /**
   * Returns the current status of the workflow. This is a semaphore
   * value that represents the current state of the workflow, where
   * 0 is complete and a negative value represents that the flow was
   * interrupted.
   */
  async status(): Promise<number> {
    return await this.hotMesh.getStatus(this.workflowId);
  }

  /**
   * Interrupts a running workflow. Standard Job Completion tasks will
   * run. Subscribers will be notified and the job hash will be expired.
   */
  async interrupt(options?: JobInterruptOptions): Promise<string> {
    return await this.hotMesh.interrupt(
      `${this.hotMesh.appId}.execute`,
      this.workflowId,
      options,
    );
  }

  /**
   * Waits for the workflow to complete and returns the result. If
   * the workflow response includes an error, this method will rethrow
   * the error, including the stack trace if available.
   * Wrap calls in a try/catch as necessary to avoid unhandled exceptions.
   */
  async result<T>(config?: {
    state?: boolean;
    throwOnError?: boolean;
  }): Promise<T | StreamError> {
    const topic = `${this.hotMesh.appId}.executed.${this.workflowId}`;
    let isResolved = false;

    return new Promise(async (resolve, reject) => {
      /**
       * rejects/resolves the promise based on the `throwOnError`
       * default behavior is to throw if error
       */
      const safeReject = (err: StreamError) => {
        if (config?.throwOnError === false) {
          return resolve(err);
        }
        reject(err);
      };

      /**
       * Common completion function that unsubscribes from the topic/returns
       */
      const complete = async (response?: T, err?: StreamError) => {
        if (isResolved) return;
        isResolved = true;

        if (err) {
          return safeReject(err as StreamError);
        } else if (!response) {
          const state = await this.hotMesh.getState(
            `${this.hotMesh.appId}.execute`,
            this.workflowId,
          );
          if (state.data?.done && !state.data?.$error) {
            return resolve(state.data.response as T);
          } else if (state.data?.$error) {
            return safeReject(state.data.$error as StreamError);
          } else if (state.metadata.err) {
            return safeReject(JSON.parse(state.metadata.err) as StreamError);
          }
          response = state.data?.response as T;
        }
        resolve(response as T);
      };

      //more expensive; fetches the entire job, not just the `status`
      if (config?.state) {
        const state = await this.hotMesh.getState(
          `${this.hotMesh.appId}.execute`,
          this.workflowId,
        );
        if (state?.data?.done && !state.data?.$error) {
          return complete(state.data.response as T);
        } else if (state.data?.$error) {
          return complete(null, state.data.$error as StreamError);
        } else if (state.metadata.err) {
          return complete(null, JSON.parse(state.metadata.err) as StreamError);
        }
      }

      //subscribe to 'done' topic
      this.hotMesh.sub(topic, async (_topic: string, state: JobOutput) => {
        this.hotMesh.unsub(topic);
        if (state.data.done && !state.data?.$error) {
          await complete(state.data?.response as T);
        } else if (state.data?.$error) {
          return complete(null, state.data.$error as StreamError);
        } else if (state.metadata.err) {
          const error = JSON.parse(state.metadata.err) as StreamError;
          return await complete(null, error);
        }
      });

      //check state in case completed during wiring
      const status = await this.hotMesh.getStatus(this.workflowId);
      if (status <= 0) {
        await complete();
      }
    });
  }
}
