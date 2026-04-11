import { HotMesh } from '../hotmesh';
import {
  DurableJobExport,
  ExportOptions,
  ExecutionExportOptions,
  WorkflowExecution,
} from '../../types/exporter';
import { JobInterruptOptions, JobOutput } from '../../types/job';
import { StreamError } from '../../types/stream';

import { ExporterService } from './exporter';

/**
 * Handle to a running or completed workflow execution. Returned by
 * `client.workflow.start()` and `client.workflow.getHandle()`.
 *
 * @example
 * ```typescript
 * const handle = await client.workflow.start({
 *   args: ['order-123'],
 *   taskQueue: 'orders',
 *   workflowName: 'orderWorkflow',
 *   workflowId: Durable.guid(),
 * });
 *
 * // Await the final result
 * const result = await handle.result();
 *
 * // Or interact while running
 * await handle.signal('approval', { approved: true });
 * await handle.cancel();
 * ```
 */
export class WorkflowHandleService {
  /**
   * @private
   */
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
   * Exports the full workflow state (job hash, metadata, activity
   * results) as a JSON object.
   */
  async export(options?: ExportOptions): Promise<DurableJobExport> {
    return this.exporter.export(this.workflowId, options);
  }

  /**
   * Exports the workflow as an execution event history.
   *
   * **Sparse mode** (default): transforms the main workflow's timeline
   * into a flat event list with workflow lifecycle, activity, child workflow,
   * timer, and signal events.
   *
   * **Verbose mode**: recursively fetches child workflow jobs and attaches
   * their full execution histories as nested `children`.
   */
  async exportExecution(
    options?: ExecutionExportOptions,
  ): Promise<WorkflowExecution> {
    return this.exporter.exportExecution(
      this.workflowId,
      this.workflowTopic,
      options,
    );
  }

  /**
   * Delivers a named signal to the workflow. If the workflow is paused
   * on `Durable.workflow.condition(signalId)`, it resumes with the
   * provided data.
   *
   * @param signalId - Matches the `signalId` passed to `condition()`.
   * @param data - Payload delivered to the waiting workflow.
   */
  async signal(signalId: string, data: Record<any, any>): Promise<void> {
    await this.hotMesh.signal(`${this.hotMesh.appId}.wfs.signal`, {
      id: signalId,
      data,
    });
  }

  /**
   * Returns the current workflow state. For a completed workflow this
   * is the final output; for a running workflow it reflects the latest
   * persisted state (may change as activities complete).
   *
   * @param metadata - If `true`, returns the full job envelope including
   *   internal metadata alongside the data.
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
   * Returns key-value pairs previously written via
   * `Durable.workflow.search()` or `Durable.workflow.enrich()`.
   *
   * @param fields - The field names to retrieve.
   */
  async queryState(fields: string[]): Promise<Record<string, any>> {
    return await this.hotMesh.getQueryState(this.workflowId, fields);
  }

  /**
   * Returns the workflow's numeric status code: `0` = completed,
   * positive = still running, negative = interrupted/errored.
   */
  async status(): Promise<number> {
    return await this.hotMesh.getStatus(this.workflowId);
  }

  /**
   * Immediately terminates the workflow. The job is marked as interrupted,
   * subscribers are notified, and the job hash is expired. Unlike
   * {@link cancel}, this does **not** give the workflow a chance to
   * run cleanup code.
   */
  async terminate(options?: JobInterruptOptions): Promise<string> {
    return await this.hotMesh.interrupt(
      `${this.hotMesh.appId}.execute`,
      this.workflowId,
      options,
    );
  }

  /**
   * Requests cooperative cancellation of the workflow. Unlike
   * `terminate()` (which terminates immediately), `cancel()` sets
   * a durable flag that the workflow detects at its next durable
   * operation (`sleep`, `proxyActivities`, `executeChild`, etc.).
   * The workflow receives a `CancelledFailure` error that it can
   * catch to perform cleanup before exiting.
   *
   * ```typescript
   * const handle = await client.workflow.start({ ... });
   * await handle.cancel();
   * // Workflow will throw CancelledFailure at its next durable operation
   * ```
   */
  async cancel(): Promise<void> {
    await this.hotMesh.cancel(this.workflowId);
  }

  /**
   * Blocks until the workflow completes and returns the result. If the
   * workflow failed, the error is rethrown (with stack trace) unless
   * `throwOnError: false` is set, in which case the error object is
   * returned directly.
   *
   * @template T - The workflow's return type.
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
