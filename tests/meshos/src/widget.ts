import { MeshOS } from '../../../services/meshos';
import * as HotMeshTypes from '../../../types';

export class Widget extends MeshOS {
  //Subclass the `connect` method to connect workers and
  // hooks (optional) when the container starts
  //NOTE: if not subclassed, the super method would start
  // an engine client (since no bound function would be available)
  // This is useful for dashboads that simply need to connect
  async connect() {
    await this.meshData.connect({
      entity: this.getEntity(),
      //this function runs transactionally
      target: async function (input: {
        id: string;
        $entity: string;
        active: 'y' | 'n';
      }) {
        return { hello: input.id };
      },
      options: {
        namespace: this.getNamespace(),
        taskQueue: this.getTaskQueue(),
      },
    });
  }

  // subclass the `create` method to start a transactional
  // workflow use the options/search field to set default
  // record data `{ ...input}` and invoke the `createWidget`
  // workflow.
  async create(
    input: HotMeshTypes.StringAnyType,
  ): Promise<HotMeshTypes.StringStringType> {
    return await this.meshData.exec<HotMeshTypes.StringStringType>({
      entity: this.getEntity(),
      args: [{ ...input }],
      options: {
        id: input.id,
        ttl: '5 minutes',
        namespace: this.getNamespace(),
        taskQueue: this.getTaskQueue(),
        search: { data: { ...input } },
      },
    });
  }
}
