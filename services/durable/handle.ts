import { ExporterService } from './exporter';
import { HotMeshService as HotMesh } from '../hotmesh';
import { DurableJobExport } from '../../types/exporter';
import { JobInterruptOptions, JobOutput } from '../../types/job';
import { StreamError } from '../../types/stream';

export class WorkflowHandleService {
  exporter: ExporterService
  hotMesh: HotMesh;
  workflowTopic: string;
  workflowId: string;

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

  async export(): Promise<DurableJobExport>  {
    return this.exporter.export(this.workflowId);
  }

  /**
   * Sends a signal to the workflow. This is a way to send
   * a message to a workflow that is paused due to having
   * executed `Durable.workflow.waitFor`. The workflow
   * will awaken if no other signals are pending.
   */
  async signal(signalId: string, data: Record<any, any>): Promise<void> {
    await this.hotMesh.hook(`${this.hotMesh.appId}.wfs.signal`, { id: signalId, data });
  }

  /**
   * Returns the job state of the workflow. If the workflow has completed
   * this is also the job output. If the workflow is still running, this
   * is the current state of the job, but it may change depending upon
   * the activities that remain.
   */
  async state(metadata = false): Promise<Record<string, any>> {
    const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
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
    return await this.hotMesh.interrupt(`${this.hotMesh.appId}.execute`, this.workflowId, options);
  }

  /**
   * Awaits for the workflow to complete and returns the result. If
   * the workflow response includes an error, this method will rethrow
   * the error, including the stack trace if available. 
   * Wrap calls in a try/catch as necessary to avoid unhandled exceptions.
   */
  async result(loadState?: boolean): Promise<any> {
    const topic = `${this.hotMesh.appId}.executed.${this.workflowId}`;
    let isResolved = false;

    return new Promise(async (resolve, reject) => {

      /**
       * Common completion function that unsubscribes from the topic/returns
       */
      const complete = async (response?: any, err?: Record<string, any>) => {
        if (isResolved) return;
        isResolved = true;

        if (err) {
          return reject(err);
        } else if (!response) {
          const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
          if (state.data?.$error) {
            return reject(state.data.$error)
          } else if (state.metadata.err) {
            return reject(JSON.parse(state.metadata.err));
          }
          response = state.data?.response;
        }
        resolve(response);
      };

      //loadState is more expensive; fetches the entire job, not just the `status`
      if (loadState) {
        const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
        if (state.data?.$error) {
          return complete(null, state.data.$error)
        } else if (state.metadata.err) {
          return complete(null, JSON.parse(state.metadata.err));
        } else if (state?.data?.done) {
          return complete(state.data.response);
        }
      }

      //subscribe to 'done' topic
      this.hotMesh.sub(topic, async (_topic: string, state: JobOutput) => {
        this.hotMesh.unsub(topic);

        if (state.data?.$error) {
          return complete(null, state.data.$error as StreamError)
        } else if (state.metadata.err) {
          const error = JSON.parse(state.metadata.err) as StreamError;
          return await complete(null, error);
        }
        await complete(state.data?.response);
      });

      //check state in case completed during wiring
      const status = await this.hotMesh.getStatus(this.workflowId);
      if (status <= 0) {
        await complete();
      }
    });
  }
}
