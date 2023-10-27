import { JobOutput } from '../../types/job';
import { HotMeshService as HotMesh } from '../hotmesh';
import { PUBLISHES_TOPIC } from './factory';

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
    await this.hotMesh.hook('durable.wfs.signal', { id: signalId, data });
  }

  async result(): Promise<any> {
    let status = await this.hotMesh.getStatus(this.workflowId);
    const topic = `${PUBLISHES_TOPIC}.${this.workflowId}`;
  
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
          const state = await this.hotMesh.getState(this.workflowTopic, this.workflowId);
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
