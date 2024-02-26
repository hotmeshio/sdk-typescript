import { Durable } from '.';
import { asyncLocalStorage } from './asyncLocalStorage';
import { ClientService as Client } from './client';
import { WorkflowHandleService } from './handle';
import { Search } from './search';
import { WorkerService as Worker } from './worker';
import { WorkflowService } from './workflow';
import {
  FindOptions,
  FindWhereOptions,
  FindWhereQuery,
  MeshOSActivityOptions,
  MeshOSConfig,
  MeshOSOptions,
  MeshOSWorkerOptions,
  WorkflowSearchOptions } from '../../types/durable';
import { RedisOptions, RedisClass } from '../../types/redis';
import { StringAnyType } from '../../types/serializer';
import { guid } from '../../modules/utils';

/**
 * The base class for running MeshOS workflows.
 * Extend this class, add your Redis config, and add functions to
 * execute as durable `hooks`, `workflows`, and `activities`.
 */

export class MeshOSService {

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
   * Data is routed to workers that specify this task queue.
   * Setting the task queue when the worker is created will
   * ensure that the worker only receives messages destined
   * for the queue. Callers can specify the taskQue to when
   * starting a job to call those workers.
   */
  taskQueue = 'default';

  /**
   * These methods run as durable workflows
   */
  workflowFunctions: Array<MeshOSOptions | string> = [];

  /**
   * These methods run as hooks (hook into a running workflow)
   */
  hookFunctions: Array<MeshOSOptions | string> = [];

  /**
   * These methods run as proxied activities (and are safely memoized)
   */
  proxyFunctions: Array<MeshOSActivityOptions | string> = [];

  /**
   * The workflow GUID. Workflows will be persisted to
   * Redis using the pattern hmsh:<namespace>:j:<id>.
   */
  id: string;

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
   * The Redis connection class.
   * 
   * @example
   * import Redis from 'ioredis';
   * import * as Redis from 'redis';
   */
  redisClass: RedisClass;

  /**
   * Optional model declaration (custom workflow state)
   */
  model: StringAnyType;

  /**
   * Optional configuration for Redis FT search
   */
  search: WorkflowSearchOptions;

  static MeshOS = WorkflowService

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
   * mints a workflow ID, using the search prefix.
   * NOTE: The prefix is necesary when indexing
   *       HASHes when FT search is enabled.
   * @returns {string}
   */
  static mintGuid(): string {
    const my = new this();
    return `${my.search?.prefix?.[0]}${guid()}`;
  }

  /**
   * Creates an FT search index
   */
  static async createIndex() {
    const my = new this();
    const hmClient = await MeshOSService.getHotMeshClient(my.redisClass, my.redisOptions, my.namespace, my.taskQueue);
    Search.configureSearchIndex(hmClient, my.search)
  }

  /**
   * stop the workers
   * @returns {Promise<void>}
   */
  static async stopWorkers(): Promise<void> {
    await Durable.shutdown();
  }

  /**
   * Initializes the worker(s). This is a static
   * method that allows for optional task Queue targeting.
   * An `allowList` may be optionally provided to start
   * specific `worker` and `hook` methods. 
   * @param {MeshOSWorkerOptions} [options]
   */
  static async startWorkers(options?: MeshOSWorkerOptions) {
    const taskQueue = options && options.taskQueue;
    const allowList = options && options.allowList || [];
    const my = new this();

    //helper functions
    const resolveFunctionNames = (arr: any[]) => arr.map(item => typeof item === 'string' ? item : item.name);
    const belongsTo = (name: string, target: Array<MeshOSOptions | MeshOSActivityOptions | string>): boolean => {
      const isWorkflow = target.find((item: MeshOSOptions | string) => {
        return typeof item === 'string' ? item === name : item.name === name;
      });
      return isWorkflow !== undefined;
    };

    // proxy registered activities
    const proxyFunctionNames = resolveFunctionNames([...my.proxyFunctions]);
    if (proxyFunctionNames.length) {
      const proxyActivities = proxyFunctionNames.reduce((acc, funcName) => {
        let originalMethod = my[funcName];
        if (typeof originalMethod === 'function') {
          acc[funcName] = async (...args: any[]) => {
            return await originalMethod.apply(my, args);
          }
        }
        return acc;
      }, {});
      const proxiedActivities = Durable.workflow.proxyActivities({
        activities: proxyActivities
      });
      Object.assign(my, proxiedActivities);
    }

    const functionsToIterate = allowList.length ? resolveFunctionNames(allowList) : resolveFunctionNames([...my.workflowFunctions, ...my.hookFunctions]);

    // Iterating through the functions sequentially
    for (const funcName of functionsToIterate) {
      const originalMethod = my[funcName];
      if (typeof originalMethod === 'function') {

        //wrap the function to return
        const wrappedFunction = {
          [funcName]: async (...args: any[]) => {
            const store = asyncLocalStorage.getStore();
            const workflowId = store.get('workflowId');

            //use a Proxy to wrap hook methods
            const context = new Proxy(my, {
              get: (target, prop, receiver) => {
                if (prop === 'id') {
                  return workflowId;
                } else if (typeof target[prop] === 'function') {
                  return (...args: any[]) => {
                    return new Promise(async (resolve, reject) => {
                      if (belongsTo(prop as string, my.hookFunctions)) {
                        return WorkflowService.hook({
                          namespace: my.namespace,
                          taskQueue: my.taskQueue,
                          workflowName: prop as string,
                          workflowId,
                          args,
                        }).then(resolve).catch(reject);
                      }
                      //otherwise, call the method as a standard instance method.
                      target[prop].apply(this, args).then(resolve).catch(reject);
                    });
                  }
                }
                return Reflect.get(target, prop, receiver);
              },
            });
            return await originalMethod.apply(context, args);
          }
        };

        //start the worker
        await Worker.create({
          namespace: my.namespace,
          connection: {
            class: my.redisClass,
            options: my.redisOptions,
          },
          taskQueue: taskQueue ?? my.taskQueue,
          workflow: wrappedFunction,
        });
      }
    }
  }

  /**
   * executes the redis FT search query; optionally specify other commands
   * @example '@_quantity:[89 89]'
   * @example '@_quantity:[89 89] @_name:"John"'
   * @example 'FT.search my-index @_quantity:[89 89]'
   * @param {FindOptions} options
   * @param {any[]} args
   * @returns {Promise<string[] | [number] | Array<number, string | number | string[]>>}
   */
  static async find(options: FindOptions, ...args: string[]): Promise<string[] | [number] | Array<string | number | string[]>> {
    const my = new this();
    const client = new Client({ connection: {
      class: my.redisClass,
      options: my.redisOptions 
    }});

    let workflowName: string;
    if (options.workflowName) {
      workflowName = options?.workflowName
    } else if(my.workflowFunctions?.length) {
      let target = my.workflowFunctions[0];
      if (typeof target === 'string') {
        workflowName = target;
      } else {
        workflowName = target.name;
      }
    }
    return await client.workflow.search(
      options.taskQueue ?? my.taskQueue,
      workflowName,
      options.namespace ?? my.namespace,
      options.index ?? my.search.index,
      ...args,
    ); //[count, [id, fields[]], [id, fields[]], [id, fields[]], ...]]
  }

  /**
   * Provides a JSON abstraction for the Redis FT.search command
   * (e.g, `count`, `query`, `return`, `limit`)
   * @param {FindWhereOptions} options 
   * @returns {Promise<string[] | [number] | Array<string | number | string[]>>}
   */
  static async findWhere(options: FindWhereOptions): Promise<string[] | [number] | Array<string | number | string[]>> {
    const args: string[] = [this.generateSearchQuery(options.query)];
    if (options.count) {
      args.push('LIMIT', '0', '0');
    } else {
      //limit which hash fields to return
      if (options.return?.length) {
        args.push('RETURN');
        args.push(options.return.length.toString());
        options.return.forEach(returnField => {
          args.push(`_${returnField}`);
        });
      }
      //paginate
      if (options.limit) {
        args.push('LIMIT', options.limit.start.toString(), options.limit.size.toString());
      }
    } 
    return await this.find(options.options ?? {}, ...args);
  }

  static generateSearchQuery(query: FindWhereQuery[]) {
    const my = new this();
    let queryString = query.map(q => {
      const { field, is, value, type } = q;
      const prefixedFieldName = my.search?.schema && field in my.search.schema ? `@_${field}` : `@${field}`;
      const fieldType = my.search?.schema[field]?.type ?? type ?? 'TEXT';

      switch (fieldType) {
        case 'TAG':
          return `${prefixedFieldName}:{${value}}`;
        case 'TEXT':
          return `${prefixedFieldName}:"${value}"`;
        case 'NUMERIC':
          let range = '';
          if (is.startsWith('=')) {        //equal
            range = `[${value} ${value}]`;
          } else if (is.startsWith('<')) { //less than or equal
            range = `[-inf ${value}]`;
          } else if (is.startsWith('>')) { //greater than or equal
            range = `[${value} +inf]`;
          } else if (is === '[]') {        //between
            range = `[${value[0]} ${value[1]}]`
          }
          return `${prefixedFieldName}:${range}`;
        default:
          return '';
      }
    }).join(' ');
    return queryString;
  }

  /**
   * returns the workflow handle. The handle can then be
   * used to query for status, state, custom state, etc.
   * @param {string} id 
   * @returns {Promise<WorkflowHandleService>}
   */
  static async get(id: string): Promise<WorkflowHandleService> {
    const my = new this();
    const client = new Client({ connection: {
      class: my.redisClass,
      options: my.redisOptions 
    }});
    let workflowName: string;
    let target = my.workflowFunctions[0];
    if (typeof target === 'string') {
      workflowName = target;
    } else {
      workflowName = target.name;
    }
    return await client.workflow.getHandle(
      my.taskQueue,
      workflowName,
      id,
      my.namespace,
    );
  }

  /**
   * Optionally include a target taskQueue to exec the
   * workflow's call on a specific worker queue.
   */
  constructor(id?: string | MeshOSConfig, options?: MeshOSConfig) {
    if (typeof(id) === 'string') {
      this.id = id;
    } else if (id?.id) {
      this.id = id.id;
      options = id;
      id = undefined;
    };
    if (options?.taskQueue) {
      this.taskQueue = options.taskQueue;
    } else if (!id && !options?.taskQueue) {
      return this;
    }

    function belongsTo(name: string, target: Array<MeshOSOptions | MeshOSActivityOptions | string>): boolean {
      const isWorkflow = target.find((item: MeshOSOptions | string) => {
        return typeof item === 'string' ? item === name : item.name === name;
      });
      return isWorkflow !== undefined;
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
              if (belongsTo(prop as string, this.workflowFunctions)) {
                //start a new workflow
                const handle = await client.workflow.start({
                  namespace: this.namespace,
                  args,
                  taskQueue: this.taskQueue,
                  workflowName: prop as string,
                  workflowId: this.id,
                });
                if (options?.await) {
                  //wait for the workflow to complete
                  const result = await handle.result();
                  return resolve(result);
                } else {
                  //return the workflow handle
                  return resolve(handle);
                }
              } else if (belongsTo(prop as string, this.hookFunctions)) {
                //hook into a running workflow
                return client.workflow.hook({
                  namespace: this.namespace,
                  taskQueue: this.taskQueue,
                  workflowName: prop as string,
                  workflowId: this.id,
                  args,
                }).then(resolve).catch(reject);
              }
              //otherwise, call the method as a standard instance method.
              target[prop].apply(this, args).then(resolve).catch(reject);
            });
          };
        }
        return Reflect.get(target, prop, receiver);
      },
    });
  }
}
