import { nanoid } from 'nanoid';

import { ClientService as Client } from './client';
import { WorkflowHandleService } from './handle';
import { Search } from './search';
import { WorkerService as Worker } from './worker';
import { WorkflowSearchOptions } from '../../types/durable';
import { RedisOptions, RedisClass } from '../../types/redis';

/**
 * A base class for configuration and setup of
 * a reentrant process database. Entities modeled as
 * subclasses of this class will execute as reentrant
 * processes with a 'main' execution thread and 'n'
 * parallel hook threads.
 * 
 * @example
 * //RUN (start a workflow)
 * const myInstance = new MeshDB('someIdempotentGuid');
 * const handle = await myInstance.create(100);
 * await handle.result(); //100
 *
 * //UPDATE (update a workflow)
 * const result = await myInstance.decrement(11);
 */

export class MeshDBService {

  /**
   * The name of the main method. When this method
   * is invoked/proxied, it is assumed that a new
   * workflow instance is being created. In all other
   * cases, the call is assumed to be a hook/update
   */
  main = 'create';

  /**
   * The GUID for the workflow (assigned when created). This
   * value should be idempotent and will be rejected if an
   * instance is already running with the same id.
   */
  id: string;

  /**
   * test value
   */
  value: number;

  /**
   * The top-level Redis isolation. All workflow data is
   * isolated within this namespace. Values should be
   * lower-case with no spaces (e.g, 'staging', 'prod', 'test',
   * 'routing-stagig', 'reporting-prod', etc.). 
   *  1) only url-safe values are allowed;
   *  2) the 'a' symbol is reserved by HotMesh for indexing apps
   */
  namespace = 'durable';

  /**
   * The second-level isolation. Data is routed to workers
   * that specify this task queue. Setting the task queue
   * when the worker is created will ensure that the worker
   * only receives messages destined for the queue. This
   * allows callers to specify specific workers/containers
   * for specific tasks. Only url-safe values are allowed.
   */
  taskQueue = 'default';

  /**
   * The Redis connection options. NOTE: Redis and IORedis
   * use different formats for their connection config.
   */
  redisOptions: RedisOptions = {
    host: 'localhost',
    port: 6379,
    password: '',
    db: 0,
  };

  /**
   * The Redis connection class. Import as follows
   * within the base subclass as follows:
   * 
   * @example
   * import Redis from 'ioredis';
   * import * as Redis from 'redis';
   */
  redisClass: RedisClass | null = null;

  /**
   * Configuration for the the Redis FT search index.
   */
  search: WorkflowSearchOptions;

  static async getHotMeshClient (redisClass: RedisClass, redisOptions: RedisOptions, namespace: string, taskQueue: string) {
    const client = new Client({
      connection: {
        class: redisClass,
        options: redisOptions,
      }
    });
    return await client.getHotMeshClient(taskQueue, namespace);
  }

  /**
   * mints a new key, using the provided search prefix, ensuring
   * new workflows are properly indexed
   * @returns {string}
   */
  static mintGuid(): string {
    const my = new this();
    return `${my.search?.prefix?.[0]}${nanoid()}}`;
  }

  /**
   * Creates an FT search index
   */
  static async createIndex() {
    const my = new this();
    const hmClient = await MeshDBService.getHotMeshClient(my.redisClass, my.redisOptions, my.namespace, my.taskQueue);
    Search.configureSearchIndex(hmClient, my.search)
  }

  /**
   * Initialize the worker(s) for the entity. This is a static
   * method that allows for optional task Queue targeting.
   * NOTE: Allow List may be optionally used 
   * @param {string} taskQueue 
   * @param {string[]} allowList 
   */
  static async doWork(taskQueue?: string, allowList?: string[]) {
    const my = new this();
    let prototype = Object.getPrototypeOf(my);
    const durablePromises = [];
    const found = [];

    while (prototype !== null && !Object.getOwnPropertyNames(prototype).includes('__proto__')) {
      const promises = Object.getOwnPropertyNames(prototype).map((prop) => {
        if (found.includes(prop) || ['constructor'].includes(prop) || (allowList && !allowList.includes(prop))) {
          return;
        }
        const originalMethod = my[prop];
        if (typeof originalMethod === 'function') {
          found.push(prop);
          return Worker.create({
            namespace: my.namespace,
            connection: {
              class: my.redisClass,
              options: my.redisOptions,
            },
            taskQueue: taskQueue ?? my.taskQueue,
            workflow: originalMethod,
          });
        }
      }).filter(p => p !== undefined); // filter out undefined values
      durablePromises.push(...promises);
      prototype = Object.getPrototypeOf(prototype);
    }
    await Promise.all(durablePromises);
  }

  /**
   * executes the redis FT search query
   * @example '@_quantity:[89 89]'
   * @param {any[]} args
   * @returns {string}
   */
  static async find(...args: string[]): Promise<string[] | [number]> {
    const my = new this();
    const client = new Client({ connection: {
      class: my.redisClass,
      options: my.redisOptions 
    }});
    return await client.workflow.search(
      my.taskQueue,
      my.main,
      my.namespace,
      my.search.index,
      ...args,
    );
    //[count, [id, fields[], id, fields[], id, fields[], ...]]
  }

  /**
   * returns the workflow handle (use the handle to call:
   * `state`, `status`, `queryStatus`, and `result`)
   * @param {string} id 
   * @returns {Promise<WorkflowHandleService>}
   */
  static async get(id: string): Promise<WorkflowHandleService> {
    const my = new this();
    const client = new Client({ connection: {
      class: my.redisClass,
      options: my.redisOptions 
    }});
    return await client.workflow.getHandle(
      my.taskQueue,
      my.main,
      id,
      my.namespace,
    );
  }

  /**
   * Initialize with an idempotent workflow identifier.
   * Optionally include a target taskQueue to send
   * events to a specific worker.
   */
  constructor(id?: string, taskQueue?: string) {
    this.id = id;
    if (taskQueue) {
      this.taskQueue = taskQueue;
    } else if (!id && !taskQueue) {
      return this;
    }

    return new Proxy(this, {
      get: (target, prop, receiver) => {
        if (typeof target[prop] === 'function') {
          return (...args: any[]) => {

            return new Promise(async (resolve, reject) => {
              const client = new Client({ connection: {
                class: this.redisClass,
                options: this.redisOptions 
              }});
              if (prop === this.main) {
                //start a new workflow (main method was called)
                return client.workflow.start({
                  namespace: this.namespace,
                  args,
                  taskQueue: this.taskQueue,
                  workflowName: prop,
                  workflowId: this.id,
                }).then(resolve).catch(reject);
              } else if (prop !== 'constructor') {
                //update an existing workflow (hook/signal-in)
                return client.workflow.hook({
                  namespace: this.namespace,
                  taskQueue: this.taskQueue,
                  workflowName: prop as string,
                  workflowId: this.id,
                  args,
                }).then(resolve).catch(reject);
              }
              target[prop].apply(this, args).then(resolve).catch(reject);
            });
          };
        }
        return Reflect.get(target, prop, receiver);
      },
    });
  }
}
