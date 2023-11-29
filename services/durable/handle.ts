import { JobOutput } from '../../types/job';
import { HotMeshService as HotMesh } from '../hotmesh';

export class WorkflowHandleService {
  hotMesh: HotMesh;
  workflowTopic: string;
  workflowId: string;

  constructor(hotMesh: HotMesh, workflowTopic: string, workflowId: string) {
    this.workflowTopic = workflowTopic;
    this.workflowId = workflowId;
    this.hotMesh = hotMesh;
  }

  async signal(signalId: string, data: Record<any, any>): Promise<void> {
    await this.hotMesh.hook(`${this.hotMesh.appId}.wfs.signal`, { id: signalId, data });
  }

  async result(loadState?: boolean): Promise<any> {
    if (loadState) {
      const state = await this.hotMesh.getState(`${this.hotMesh.appId}.execute`, this.workflowId);
      if (!state.data && state.metadata.err) {
        throw new Error(JSON.parse(state.metadata.err));
      }
      if (state?.data?.done) {
        //child flows are never technically 'done' as they have an open hook
        //that is tied to the parent flow's completion. so, we need to check
        //the 'done' flag on the child flow's payload (not the 'js' metadata field
        //which is typically used); the loadState parameter ensures this
        //check happens early
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
          if (!state.data && state.metadata.err) {
            return reject(JSON.parse(state.metadata.err));
          }
          response = state.data?.response;
        }
        resolve(response);
      };
      //check for done
      if (status == 0) {
        return complete();
      }
      //subscribe to topic
      this.hotMesh.sub(topic, async (topic: string, state: JobOutput) => {
        if (!state.data && state.metadata.err) {
          await complete(null, state.metadata.err);
        } else {
          await complete(state.data?.response);
        }
      });
      //resolve for race condition
      setTimeout(async () => {
        status = await this.hotMesh.getStatus(this.workflowId);
        if (status == 0) {
          await complete();
        }
      }, 0);
    });
  }
}
