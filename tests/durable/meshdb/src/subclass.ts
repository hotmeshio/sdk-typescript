import { Durable } from '../../../../services/durable';
import { MeshDBTest } from './meshdb';

export class MeshDBTestSubClass extends MeshDBTest {
  constructor(id?: string, taskQueue?: string) {
    super(id, taskQueue);
  }

  /**
   * main method: create; initializes the entity
   * @returns {Promise<string>}
   */
  async create(val: number): Promise<string> {
    const search = await Durable.workflow.search();
    await search.set('quantity', val.toString()); //100
    await Durable.workflow.sleep('1 second');
    return await search.get('quantity'); //89
  }

  /**
   * testing regular calls and proxy calls
   */
  async decrement(val: number): Promise<void> {
    const search = await Durable.workflow.search();
    await search.incr('quantity', -val); //subtract 11
  }
}
