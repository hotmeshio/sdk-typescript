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

  async result(): Promise<any> {
    let status = await this.hotMesh.getStatus(this.workflowId);
    const topic = `${this.workflowTopic}.${this.workflowId}`;
  
    if (status == 0) {
      return (await this.hotMesh.getState(this.workflowTopic, this.workflowId)).data?.response;
    }
  
    return new Promise((resolve, reject) => {
      let isResolved = false;
      //common fulfill/unsubscribe
      const complete = async (response?: any) => {
        if (isResolved) return;
        isResolved = true;
        this.hotMesh.unsub(topic);
        resolve(response || (await this.hotMesh.getState(this.workflowTopic, this.workflowId)).data?.response);
      };
      this.hotMesh.sub(topic, async (topic: string, message: JobOutput) => {
        await complete(message.data?.response);
      });
      setTimeout(async () => {
        status = await this.hotMesh.getStatus(this.workflowId);
        if (status == 0) {
          await complete();
        }
      }, 0);
    });
  }
}
