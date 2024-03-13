import { HMSH_CODE_INTERRUPT } from '../../modules/enums';
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
   * executed a `waitForSignal` workflow extension. Awakens
   * the workflow if no other signals are pending.
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
   * the workflow thows and error, this method will likewise throw
   * an error.
   */
  async result(loadState?: boolean): Promise<any> {
    if (loadState) {
      const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
      if (!state.data && state.metadata.err) {
        throw new Error(JSON.parse(state.metadata.err));
      }
      if (state?.data?.done) {
        //child flows are never 'done'; they use a hook
        //that only closes upon parent flow completion.
        return state.data.response;
      }
    }
    let status = await this.hotMesh.getStatus(this.workflowId);
    const topic = `${this.hotMesh.appId}.executed.${this.workflowId}`;
  
    return new Promise((resolve, reject) => {
      let isResolved = false;
      //common fulfill/unsubscribe
      const complete = async (response?: any, err?: string) => {
        if (isResolved) return;
        isResolved = true;
        this.hotMesh.unsub(topic);
        if (err) {
          return reject(JSON.parse(err));
        } else if (!response) {
          const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
          if (state.metadata.err) {
            const error = JSON.parse(state.metadata.err) as StreamError;
            if (error.code === HMSH_CODE_INTERRUPT || !state.data) {
              return reject({ ...error, job_id: this.workflowId });
            }
          }
          response = state.data?.response;
        }
        resolve(response);
      };
      //check for done
      if (status <= 0) {
        return complete();
      }
      //subscribe to topic
      this.hotMesh.sub(topic, async (topic: string, state: JobOutput) => {
        if (state.metadata.err) {
          const error = JSON.parse(state.metadata.err) as StreamError;
          if (error.code === HMSH_CODE_INTERRUPT || !state.data) {
            return await complete(null, state.metadata.err);
          }
        }
        await complete(state.data?.response);
      });
      //resolve for race condition
      setTimeout(async () => {
        status = await this.hotMesh.getStatus(this.workflowId);
        if (status <= 0) {
          await complete();
        }
      }, 0);
    });
  }
}
