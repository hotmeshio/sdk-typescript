import { Durable } from '../../../../services/durable';
import { WorkflowSearchOptions } from '../../../../types/durable';
import { RedisOSTest } from './redisostest';

export class RedisOSTestSubClass extends RedisOSTest {

  namespace = 'staging';
  taskQueue = 'inventory'; //OR inventory-priority

  workflowFunctions = ['create'];
  hookFunctions = ['decrement', 'updateStatus'];
  proxyFunctions = ['greet'];
  
  model = {
    quantity: {type: 'numeric', indexed: true, sortable: true},
    status: {type: 'tag', indexed: true, items: [
      'ordered',
      'cancelled',
      'shipped',
      'delivered',
      'depleted'
    ]},
  };

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
    const search = await Durable.workflow.search();
    await search.set('quantity', val.toString()); //100

    //call both a proxied activity and a vanilla activity
    const greeting = await this.greet('world');
    const salud = await this.saludar('world');
    console.log(greeting, salud);

    //sleep for 1 second (or week, or year, or whatever)
    await Durable.workflow.sleep('1 second');

    const receipt = await this.updateStatus('ordered', val);
    console.log('updateStatus receipt=>', receipt);

    //wait for the hook method to finish and signal completion
    console.log('waiting for signal!!!');
    const [hookResult] = await Durable.workflow.waitForSignal(['abc']);
    console.log('wait for signal result=>', hookResult);

    return await search.get('quantity'); //89
  }

  /**
   * hook method: update workflow status (and quantity)
   */
  async updateStatus(status: string, quantity: number): Promise<void> {
    //update the workflow status
    console.log('updating status to=>', status, Date.now());

    const search = await Durable.workflow.search();
    const current = await search.get('status');
    await search.set('status', status);

    if (current === 'delivered' && status === 'depleted') {
      await search.set('quantity', quantity.toString());
    } else if (current === 'shipped' && status === 'delivered') {
      await search.set('quantity', quantity.toString());
    }

    await Durable.workflow.sleep('2 seconds');

    console.log('sending the abc signal');
    //NOTE: this test is non-deterministic: signals can fire so
    //      quickly that the subscriber (the method containing
    //      the 'waitForSignal' call) can miss the signal.
    //      If timing is absolutely critical, and the main thread
    //      needs to pause and await a response call
    //      `await Durable.workflow.executeChild()` instead.
    await Durable.workflow.signal('abc', { status, quantity });
  }

  /**
   * hook method: update workflow state
   */
  async decrement(val: number): Promise<void> {
    //update a custom value that's indexed / searchable
    const search = await Durable.workflow.search();
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
}
