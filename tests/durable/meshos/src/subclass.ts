import { MeshOSTest } from './meshostest';
import { WorkflowSearchOptions } from '../../../../types/durable';

export class MyClass extends MeshOSTest {

  namespace = 'staging';
  //taskQueue = 'priority'; //a worker with this taskQueue will service

  workflowFunctions = ['create', 'stringDoubler'];
  hookFunctions = ['decrement', 'updateStatus'];
  proxyFunctions = ['greet'];

  search: WorkflowSearchOptions = {
    index: 'inventory-orders',
    prefix: ['ord_'],
    schema: {
      quantity: {
        type: 'NUMERIC', // | TEXT
        sortable: true
      },
      status: {
        type: 'TAG',
        sortable: true
      }
    }
  };

  /**
   * main method: start the workflow.
   */
  async create(val: number): Promise<string> {
    //set a custom value that's indexed / searchable
    const search = await MyClass.MeshOS.search();
    await search.set('quantity', val.toString()); //100

    //call both a proxied activity and a vanilla activity
    const greeting = await this.greet('world');
    const salud = await this.saludar('world');
    console.log(greeting, salud);

    //sleep for 1 second (or week, or year, or whatever)
    await MyClass.MeshOS.sleep('1 second');

    const receipt = await this.updateStatus('ordered', val);
    console.log('updateStatus receipt=>', receipt);

    //wait for the hook method to finish and signal completion
    console.log('waiting for signal!!!');
    const [hookResult] = await MyClass.MeshOS.waitForSignal(['abc']);
    console.log('wait for signal result=>', hookResult);

    return await search.get('quantity'); //89
  }

  /**
   * hook method: update workflow status (and quantity)
   */
  async updateStatus(status: string, quantity: number): Promise<void> {
    //update the workflow status
    console.log('updating status to=>', status, Date.now());

    const search = await MyClass.MeshOS.search();
    const current = await search.get('status');
    await search.set('status', status);

    if (current === 'delivered' && status === 'depleted') {
      await search.set('quantity', quantity.toString());
    } else if (current === 'shipped' && status === 'delivered') {
      await search.set('quantity', quantity.toString());
    }

    await MyClass.MeshOS.sleep('2 seconds');

    console.log('sending the abc signal');
    await MyClass.MeshOS.signal('abc', { status, quantity });
  }

  /**
   * hook method: update workflow state
   */
  async decrement(val: number): Promise<void> {
    //update a custom value that's indexed / searchable
    const search = await MyClass.MeshOS.search();
    await search.incr('quantity', -val); //subtract 11

    //call another hook method if quantity is 0
    if (Number(await search.get('quantity')) < 1) {
      await this.updateStatus('depleted', 0);
    }
  }

  /**
   * proxied activity method: return a greeting
   */
  async greet(name: string): Promise<string> {
    console.log('calling greet with input=>', name);
    return `Hello ${name}!`;
  }

  /**
   * vanilla activity method: return a greeting
   */
  async saludar(name: string): Promise<string> {
    console.log('calling saludar with input=>', name);
    return `Hola ${name}!`;
  }

  /**
   * test awaiting a result
   */
  async stringDoubler(a: string): Promise<string> {
    return `${a}${a}`;
  }
}
