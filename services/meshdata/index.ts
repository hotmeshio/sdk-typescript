import { s } from '../../modules/utils';
import { MeshFlow } from '../meshflow';
import { HotMesh } from '../hotmesh';
import {
  WorkflowOptions,
  WorkflowSearchOptions,
  WorkerOptions,
  FindJobsOptions,
  FindOptions,
  FindWhereOptions,
  SearchResults,
  FindWhereQuery,
} from '../../types/meshflow';
import {
  CallOptions,
  ConnectionInput,
  ExecInput,
  HookInput,
} from '../../types/meshdata';
import { RedisClass, RedisOptions } from '../../types/redis';
import { StringAnyType, StringStringType } from '../../types/serializer';
import { JobInterruptOptions, JobOutput } from '../../types/job';
import { KeyType } from '../../types/hotmesh';
import {
  QuorumMessage,
  QuorumMessageCallback,
  QuorumProfile,
  RollCallOptions,
  SubscriptionOptions,
  ThrottleOptions,
} from '../../types/quorum';
import { MeshFlowJobExport, ExportOptions } from '../../types/exporter';
import { MAX_DELAY } from '../../modules/enums';

/**
 * The `MeshData` service wraps the `MeshFlow` service.
 * It serves to unify both data record concepts and
 * transactional workflow principles into a single
 * *Operational Data Layer*. Deployments with
 * the Redis FT.SEARCH module enabled can deliver
 * both OLTP (transactions) and OLAP (analytics)
 * with no additional infrastructure.
 *
 * The following example depicts the full end-to-end
 * lifecycle of a `MeshData` app, including the
 * connection of a worker function, the execution of
 * a remote function, the retrieval of data from Redis,
 * the creation of a search index, and the execution
 * of a full-text search query.
 * @example
 * ```typescript
 * import { MeshData, Types } from '@hotmeshio/hotmesh';
 * import * as Redis from 'redis';
 *
 * //1) Define a search schema
 * const schema = {
 *   schema: {
 *     id: { type: 'TAG', sortable: true },
 *     plan: { type: 'TAG', sortable: true },
 *     active: { type: 'TEXT', sortable: false },
 *   },
 *   index: 'user',
 *   prefix: ['user'], //index items with keys starting with 'user'
 * } as unknown as Types.WorkflowSearchOptions;
 *
 * //2) Initialize MeshData and Redis
 * const meshData = new MeshData(
 *   Redis,
 *   { url: 'redis://:key_admin@redis:6379' },
 *   schema,
 * );
 *
 * //3) Connect a 'user' worker function
 * await meshData.connect({
 *   entity: 'user',
 *   target: async function(userID: string): Promise<string> {
 *     //used the `search` extension to add searchable data
 *     const search = await MeshData.workflow.search();
 *     await search.set('active', 'yes');
 *     return `Welcome, ${userID}.`;
 *   },
 *   options: { namespace: 'meshdata' },
 * });
 *
 * const userID = 'someTestUser123';
 *
 * //4) Call the 'user' worker function; include search data
 * const response = await meshData.exec({
 *   entity: 'user',
 *   args: [userID],
 *   options: {
 *     ttl: 'infinity',
 *     id: userID,
 *     search: {
 *       data: { id: userID, plan: 'pro' }
 *     },
 *     namespace: 'meshdata',
 *   },
 * });
 *
 * //5) Read data (by field name) directly from Redis
 * const data = await meshData.get(
 *   'user',
 *   userID,
 *   {
 *     fields: ['plan', 'id', 'active'],
 *     namespace: 'meshdata'
 *   },
 * );
 *
 * //6) Create a search index
 * await meshData.createSearchIndex('user', { namespace: 'meshdata' }, schema);
 *
 * //7) Perform Full Text Search on the indexed dataset
 * const results = await meshData.findWhere('user', {
 *   query: [{ field: 'id', is: '=', value: userID }],
 *   limit: { start: 0, size: 100 },
 *   return: ['plan', 'id', 'active']
 * });
 *
 * //8) Shutdown MeshData
 * await MeshData.shutdown();
 * ```
 *
 */
class MeshData {
  /**
   * @private
   */
  connectionSignatures: StringStringType = {};

  /**
   * The Redis connection options. NOTE: Redis and IORedis
   * use different formats for their connection config.
   * @private
   * @example
   * // Instantiate MeshData with `ioredis`
   * import Redis from 'ioredis';
   *
   * const meshData = new MeshData(Redis, {
   *   host: 'localhost',
   *   port: 6379,
   *   password: 'shhh123',
   *   db: 0,
   * });
   *
   * // Instantiate MeshData with `redis`
   * import * as Redis from 'redis';
   *
   * const meshData = new MeshData(Redis, { url: 'redis://:shhh123@localhost:6379' });
   */
  redisOptions: RedisOptions;

  /**
   * The Redis connection class.
   * @private
   * @example
   * import Redis from 'ioredis';
   * import * as Redis from 'redis';
   */
  redisClass: RedisClass;

  /**
   * Cached local instances (map) of HotMesh organized by namespace
   * @private
   */
  instances: Map<string, Promise<HotMesh> | HotMesh> = new Map();

  /**
   * Redis FT search configuration (indexed/searchable fields and types)
   */
  search: WorkflowSearchOptions;

  /**
   * Provides a set of static extensions that can be invoked by
   * your linked workflow functions during their execution.
   * @example
   *
   * function greet (email: string, user: { first: string}) {
   *   //persist the user's email and newsletter preferences
   *   const search = await MeshData.workflow.search();
   *   await search.set('email', email, 'newsletter', 'yes');
   *
   *   //hook a function to send a newsletter
   *   await MeshData.workflow.hook({
   *     entity: 'user.newsletter',
   *     args: [email]
   *   });
   *
   *   return `Hello, ${user.first}. Your email is [${email}].`;
   * }
   */
  static workflow = {
    sleep: MeshFlow.workflow.sleepFor,
    sleepFor: MeshFlow.workflow.sleepFor,
    signal: MeshFlow.workflow.signal,
    hook: MeshFlow.workflow.hook,
    waitForSignal: MeshFlow.workflow.waitFor,
    waitFor: MeshFlow.workflow.waitFor,
    getHotMesh: MeshFlow.workflow.getHotMesh,
    random: MeshFlow.workflow.random,
    search: MeshFlow.workflow.search,
    getContext: MeshFlow.workflow.getContext,
    once: MeshFlow.workflow.once,

    /**
     * Interrupts a job by its entity and id.
     */
    interrupt: async (
      entity: string,
      id: string,
      options: JobInterruptOptions = {},
    ) => {
      const jobId = MeshData.mintGuid(entity, id);
      await MeshFlow.workflow.interrupt(jobId, options);
    },

    /**
     * Starts a new, subordinated workflow/job execution. NOTE: The child workflow's
     * lifecycle is bound to the parent workflow, and it will be terminated/scrubbed
     * when the parent workflow is terminated/scrubbed.
     *
     * @template T The expected return type of the target function.
     */
    execChild: async <T>(
      options: Partial<WorkflowOptions> = {},
    ): Promise<T> => {
      const pluckOptions = {
        ...options,
        args: [...options.args, { $type: 'exec' }],
      };
      return MeshFlow.workflow.execChild(pluckOptions as WorkflowOptions);
    },

    /**
     * Starts a new, subordinated workflow/job execution. NOTE: The child workflow's
     * lifecycle is bound to the parent workflow, and it will be terminated/scrubbed
     * when the parent workflow is terminated/scrubbed.
     *
     * @template T The expected return type of the target function.
     */
    executeChild: async <T>(
      options: Partial<WorkflowOptions> = {},
    ): Promise<T> => {
      const pluckOptions = {
        ...options,
        args: [...options.args, { $type: 'exec' }],
      };
      return MeshFlow.workflow.execChild(pluckOptions as WorkflowOptions);
    },

    /**
     * Starts a new, subordinated workflow/job execution, awaiting only the jobId, namely,
     * the confirmation that the suboridinated job has begun. NOTE: The child workflow's
     * lifecycle is bound to the parent workflow, and it will be terminated/scrubbed
     * when the parent workflow is terminated/scrubbed.
     */
    startChild: async (
      options: Partial<WorkflowOptions> = {},
    ): Promise<string> => {
      const pluckOptions = {
        ...options,
        args: [...options.args, { $type: 'exec' }],
      };
      return MeshFlow.workflow.startChild(pluckOptions as WorkflowOptions);
    },
  };

  /**
   *
   * @param {RedisClass} redisClass - the Redis class/import (e.g, `ioredis`, `redis`)
   * @param {StringAnyType} redisOptions - the Redis connection options. These are specific to the package (refer to their docs!). Each uses different property names and structures.
   * @param {WorkflowSearchOptions} search - the Redis search options for JSON-based configuration of the Redis FT.Search module index
   * @example
   * // Instantiate MeshData with `ioredis`
   * import Redis from 'ioredis';
   *
   * const meshData = new MeshData(Redis, {
   *   host: 'localhost',
   *   port: 6379,
   *   password: 'shhh123',
   *   db: 0,
   * });
   *
   * // Instantiate MeshData with `redis`
   * import * as Redis from 'redis';
   *
   * const meshData = new MeshData(Redis, { url: 'redis://:shhh123@localhost:6379' });
   */
  constructor(
    redisClass: Partial<RedisClass>,
    redisOptions: Partial<RedisOptions>,
    search?: WorkflowSearchOptions,
  ) {
    this.redisClass = redisClass as RedisClass;
    this.redisOptions = redisOptions as RedisOptions;
    if (search) {
      this.search = search;
    }
  }

  /**
   * @private
   */
  validate(entity: string) {
    if (entity.includes(':') || entity.includes('$') || entity.includes(' ')) {
      throw "Invalid string [':','$',' ' not allowed]";
    }
  }

  /**
   * @private
   */
  async getConnection() {
    return await MeshFlow.Connection.connect({
      class: this.redisClass,
      options: this.redisOptions,
    });
  }

  /**
   * Return a MeshFlow client
   * @private
   */
  getClient() {
    return new MeshFlow.Client({
      connection: {
        class: this.redisClass,
        options: this.redisOptions,
      },
    });
  }

  /**
   * @private
   */
  safeKey(key: string): string {
    return `_${key}`;
  }

  /**
   * @private
   */
  arrayToHash(
    input: [number, ...Array<string | string[]>],
  ): StringStringType[] {
    const max = input.length;
    const hashes: StringStringType[] = [];

    for (let i = 1; i < max; i++) {
      const fields = input[i] as string[];
      if (Array.isArray(fields)) {
        const hash: StringStringType = {};
        const hashId = input[i - 1];
        for (let j = 0; j < fields.length; j += 2) {
          const fieldKey = fields[j].replace(/^_/, '');
          const fieldValue = fields[j + 1];
          hash[fieldKey] = fieldValue;
        }
        //redis uses '$' as a special field for the key
        if (typeof hashId === 'string') {
          hash['$'] = hashId;
        }
        hashes.push(hash);
      }
    }
    return hashes;
  }

  /**
   * serialize using the HotMesh `toString` format
   * @private
   */
  toString(value: any): string | undefined {
    switch (typeof value) {
      case 'string':
        break;
      case 'boolean':
        value = value ? '/t' : '/f';
        break;
      case 'number':
        value = '/d' + value.toString();
        break;
      case 'undefined':
        return undefined;
      case 'object':
        if (value === null) {
          value = '/n';
        } else {
          value = '/s' + JSON.stringify(value);
        }
        break;
    }
    return value;
  }

  /**
   * returns an entity-namespaced guid
   * @param {string|null} entity - entity namespace
   * @param {string} [id] - workflow id (allowed to be namespaced)
   * @returns {string}
   * @private
   */
  static mintGuid(entity: string, id?: string): string {
    if (!id && !entity) {
      throw 'Invalid arguments [entity and id are both null]';
    } else if (!id) {
      id = HotMesh.guid();
    } else if (entity) {
      entity = `${entity}-`;
    } else {
      entity = '';
    }
    return `${entity}${id}`;
  }

  /**
   * Returns a HotMesh client
   * @param {string} [namespace='durable'] - the namespace for the client
   */
  async getHotMesh(namespace = 'durable'): Promise<HotMesh> {
    //try to reuse an existing client
    let hotMesh: HotMesh | Promise<HotMesh> | undefined =
      await this.instances.get(namespace);
    if (!hotMesh) {
      hotMesh = HotMesh.init({
        appId: namespace,
        engine: {
          redis: {
            class: this.redisClass,
            options: this.redisOptions,
          },
        },
      });
      this.instances.set(namespace, hotMesh);
      hotMesh = await hotMesh;
      this.instances.set(namespace, hotMesh);
    }
    return hotMesh;
  }

  /**
   * Returns the Redis HASH key given an `entity` name and workflow/job. The
   * item identified by this key is a HASH record with multidimensional process
   * data interleaved with the function state data.
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} workflowId - the workflow/job id
   * @param {string} [namespace='durable'] - the namespace for the client
   * @returns {Promise<string>}
   * @example
   * // mint a key
   * const key = await meshData.mintKey('greeting', 'jsmith123');
   *
   * // returns 'hmsh:durable:j:greeting-jsmith123'
   */
  async mintKey(
    entity: string,
    workflowId: string,
    namespace?: string,
  ): Promise<string> {
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      namespace,
    );
    const store = handle.hotMesh.engine?.store;
    return store?.mintKey(KeyType.JOB_STATE, {
      jobId: workflowId,
      appId: handle.hotMesh.engine?.appId,
    }) as string;
  }

  /**
   * Exposes the the service mesh control plane through the
   * mesh 'events' (pub/sub) system. This is useful for
   * monitoring and managing the operational data layer.
   */
  mesh = {
    /**
     * subscribes to the mesh control plane
     * @param {QuorumMessageCallback} callback - the callback function
     * @param {SubscriptionOptions} options - connection options
     * @returns {Promise<void>}
     */
    sub: async (
      callback: QuorumMessageCallback,
      options: SubscriptionOptions = {},
    ): Promise<void> => {
      const hotMesh = await this.getHotMesh(options.namespace || 'durable');
      const callbackWrapper: QuorumMessageCallback = (topic, message) => {
        if (message.type === 'pong' && !message.originator) {
          if (message.profile?.worker_topic) {
            const [entity] = message.profile.worker_topic.split('-');
            if (entity) {
              message.profile.entity = message.entity = entity;
              if (this.connectionSignatures[entity]) {
                message.profile.signature = this.connectionSignatures[entity];
              }
            }
          }
        } else if (message?.topic) {
          const [entity] = message.topic.split('-');
          if (entity) {
            message.entity = entity;
          }
        }
        callback(topic, message);
      };
      await hotMesh.quorum?.sub(callbackWrapper);
    },

    /**
     * publishes a message to the mesh control plane
     * @param {QuorumMessage} message - the message payload
     * @param {SubscriptionOptions} options - connection options
     * @returns {Promise<void>}
     */
    pub: async (
      message: QuorumMessage,
      options: SubscriptionOptions = {},
    ): Promise<void> => {
      const hotMesh = await this.getHotMesh(options.namespace || 'durable');
      await hotMesh.quorum?.pub(message);
    },

    /**
     * unsubscribes from the mesh control plane
     * @param {QuorumMessageCallback} callback - the callback function
     * @param {SubscriptionOptions} options - connection options
     * @returns {Promise<void>}
     */
    unsub: async (
      callback: QuorumMessageCallback,
      options: SubscriptionOptions = {},
    ): Promise<void> => {
      const hotMesh = await this.getHotMesh(options.namespace || 'durable');
      await hotMesh.quorum?.unsub(callback);
    },
  };

  /**
   * Connects a function to the operational data layer.
   *
   * @template T The expected return type of the target function.
   *
   * @param {object} connection - The options for connecting a function.
   * @param {string} connection.entity - The global entity identifier for the function (e.g, 'user', 'order', 'product').
   * @param {(...args: any[]) => T} connection.target - Function to connect, returns type T.
   * @param {ConnectOptions} connection.options={} - Extended connection options (e.g., ttl, taskQueue). A
   *                                                 ttl of 'infinity' will cache the function indefinitely.
   *
   * @returns {Promise<boolean>} True if connection is successfully established.
   *
   * @example
   * // Instantiate MeshData with Redis configuration.
   * const meshData = new MeshData(Redis, { host: 'localhost', port: 6379 });
   *
   * // Define and connect a function with the 'greeting' entity.
   * // The function will be cached indefinitely (infinite TTL).
   * meshData.connect({
   *   entity: 'greeting',
   *   target: (email, user) => `Hello, ${user.first}.`,
   *   options: { ttl: 'infinity' }
   * });
   */
  async connect<T>({
    entity,
    target,
    options = {},
  }: ConnectionInput<T>): Promise<boolean> {
    this.validate(entity);

    this.connectionSignatures[entity] = target.toString();
    const targetFunction = {
      [entity]: async (...args: any[]): Promise<T> => {
        const { callOptions } = this.bindCallOptions(args);
        const result = (await target.apply(target, args)) as T;
        //increase status by 1, set 'done' flag and emit the 'job done' signal
        await this.pauseForTTL(result, callOptions);
        return result as T;
      },
    };

    await MeshFlow.Worker.create({
      namespace: options.namespace,
      options: options.options as WorkerOptions,
      connection: await this.getConnection(),
      taskQueue: options.taskQueue ?? entity,
      workflow: targetFunction,
      search: options.search,
    });

    return true;
  }

  /**
   * During remote execution, an argument is injected (the last argument)
   * this is then used by the 'connect' function to determine if the call
   * is a hook or a exec call. If it is an exec, the connected function has
   * precedence and can say that all calls are cached indefinitely.
   *
   * @param {any[]} args
   * @param {StringAnyType} options
   * @param {StringAnyType} callOptions
   * @returns {StringAnyType}
   * @private
   */
  bindCallOptions(args: any[], callOptions: CallOptions = {}): StringAnyType {
    if (args.length) {
      const lastArg = args[args.length - 1];
      if (lastArg instanceof Object && lastArg?.$type === 'exec') {
        //override the caller and force indefinite caching
        callOptions = args.pop() as CallOptions;
      } else if (lastArg instanceof Object && lastArg.$type === 'hook') {
        callOptions = args.pop() as CallOptions;
        //hooks may not affect `ttl` (it is set at invocation)
        delete callOptions.ttl;
      }
    }
    return { callOptions };
  }

  /**
   * Sleeps/WaitsForSignal to keep the function open
   * and remain part of the operational data layer
   *
   * @template T The expected return type of the remote function
   *
   * @param {string} result - the result to emit before going to sleep
   * @param {CallOptions} options - call options
   * @private
   */
  async pauseForTTL<T>(result: T, options: CallOptions) {
    if (options?.ttl && options.$type === 'exec') {
      //exit early if the outer function wrapper has already run
      const { counter, replay, workflowDimension, workflowId } =
        MeshData.workflow.getContext();
      const prefix = options.ttl === 'infinity' ? 'wait' : 'sleep';
      if (`-${prefix}${workflowDimension}-${counter + 1}-` in replay) {
        return;
      }

      //manually set job state since leaving open/running
      await new Promise((resolve) => setImmediate(resolve));
      options.$guid = options.$guid ?? workflowId;
      const hotMesh = await MeshData.workflow.getHotMesh();
      const store = hotMesh.engine?.store;
      const jobKey = store?.mintKey(KeyType.JOB_STATE, {
        jobId: options.$guid,
        appId: hotMesh.engine?.appId,
      });
      const jobResponse = ['aAa', '/t', 'aBa', this.toString(result)];
      await store?.exec('HSET', jobKey, ...jobResponse);
    }
  }

  /**
   * Publishes the job result, because pausing the job (in support of
   * the 'ttl' option) interrupts the response.
   *
   * @template T The expected return type of the remote function
   *
   * @param {string} result - the result to emit before going to sleep
   * @param {HotMesh} hotMesh - call options
   * @param {CallOptions} options - call options
   *
   * @returns {Promise<void>}
   * @private
   */
  async publishDone<T>(
    result: T,
    hotMesh: HotMesh,
    options: CallOptions,
  ): Promise<void> {
    await hotMesh.engine?.store?.publish(
      KeyType.QUORUM,
      {
        type: 'job',
        topic: `${hotMesh.engine.appId}.executed`,
        job: {
          metadata: {
            tpc: `${hotMesh.engine.appId}.execute`,
            app: hotMesh.engine.appId,
            vrs: '1',
            jid: options.$guid,
            aid: 't1',
            ts: '0',
            js: 0,
          },
          data: {
            done: true, //aAa
            response: result, //aBa
            workflowId: options.$guid, //aCa
          },
        },
      },
      hotMesh.engine.appId,
      `${hotMesh.engine.appId}.executed.${options.$guid}`,
    );
  }

  /**
   * Flushes a function with a `ttl` of 'infinity'. These entities were
   * created by a connect method that was configured with a
   * `ttl` of 'infinity'. It can take several seconds for the function
   * to be removed from the cache as it might be actively orchestrating
   * sub-workflows.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - The workflow/job id
   * @param {string} [namespace='durable'] - the namespace for the client
   *
   * @example
   * // Flush a function
   * await meshData.flush('greeting', 'jsmith123');
   */
  async flush(
    entity: string,
    id: string,
    namespace?: string,
  ): Promise<string | void> {
    const workflowId = MeshData.mintGuid(entity, id);
    //resolve the system signal (this forces the main wrapper function to end)
    await this.getClient().workflow.signal(
      `flush-${workflowId}`,
      {},
      namespace,
    );
    await new Promise((resolve) => setTimeout(resolve, 1000));
    //other activities may still be running; call `interrupt` to stop all threads
    await this.interrupt(
      entity,
      id,
      {
        descend: true,
        suppress: true,
        expire: 1,
      },
      namespace,
    );
  }

  /**
   * Interrupts a job by its entity and id. It is best not to call this
   * method directly for entries with a ttl of `infinity` (call `flush` instead).
   * For those entities that are cached for a specified duration (e.g., '15 minutes'),
   * this method will interrupt the job and start the cascaded cleanup/expire/delete.
   * As jobs are asynchronous, there is no way to stop descendant flows immediately.
   * Use an `expire` option to keep the interrupted job in the cache for a specified
   * duration before it is fully removed.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - The workflow/job id
   * @param {JobInterruptOptions} [options={}] - call options
   * @param {string} [namespace='durable'] - the namespace for the client
   *
   * @example
   * // Interrupt a function
   * await meshData.interrupt('greeting', 'jsmith123');
   */
  async interrupt(
    entity: string,
    id: string,
    options: JobInterruptOptions = {},
    namespace?: string,
  ): Promise<void> {
    const workflowId = MeshData.mintGuid(entity, id);
    try {
      const handle = await this.getClient().workflow.getHandle(
        entity,
        entity,
        workflowId,
        namespace,
      );
      const hotMesh = handle.hotMesh;
      await hotMesh.interrupt(`${hotMesh.appId}.execute`, workflowId, options);
    } catch (e) {
      //no-op; interrup throws an error
    }
  }

  /**
   * Signals a Hook Function or Main Function to awaken that
   * is paused and registered to awaken upon receiving the signal
   * matching @guid.
   *
   * @param {string} guid - The global identifier for the signal
   * @param {StringAnyType} payload - The payload to send with the signal
   * @param {string} [namespace='durable'] - the namespace for the client
   * @returns {Promise<string>} - the signal id
   * @example
   * // Signal a function with a payload
   * await meshData.signal('signal123', { message: 'hi!' });
   *
   * // returns '123456732345-0' (redis stream message receipt)
   */
  async signal(
    guid: string,
    payload: StringAnyType,
    namespace?: string,
  ): Promise<string> {
    return await this.getClient().workflow.signal(guid, payload, namespace);
  }

  /**
   * Sends a signal to the backend Service Mesh (workers and engines)
   * to announce their presence, including message counts, target
   * functions, topics, etc. This is useful for establishing
   * the network profile and overall message throughput
   * of the operational data layer as a unified quorum.
   * @param {RollCallOptions} options
   * @returns {Promise<QuorumProfile[]>}
   */
  async rollCall(options: RollCallOptions = {}): Promise<QuorumProfile[]> {
    return (await this.getHotMesh(options.namespace || 'durable')).rollCall(
      options.delay || 1000,
    );
  }

  /**
   * Throttles a worker or engine in the backend Service Mesh, using either
   * a 'guid' to target a specific worker or engine, or a 'topic' to target
   * a group of worker(s) connected to that topic. The throttle value is
   * specified in milliseconds and will cause the target(s) to delay consuming
   * the next message by this amount. By default, the value is set to `0`.
   * @param {ThrottleOptions} options
   * @returns {Promise<boolean>}
   *
   * @example
   * // Throttle a worker or engine
   * await meshData.throttle({ guid: '1234567890', throttle: 10_000 });
   */
  async throttle(options: ThrottleOptions): Promise<boolean> {
    return (await this.getHotMesh(options.namespace || 'durable')).throttle(
      options as ThrottleOptions,
    );
  }

  /**
   * Similar to `exec`, except it augments the workflow state without creating a new job.
   *
   * @param {object} input - The input parameters for hooking a function.
   * @param {string} input.entity - The target entity name (e.g., 'user', 'order', 'product').
   * @param {string} input.id - The target execution/workflow/job id.
   * @param {string} input.hookEntity - The hook entity name (e.g, 'user.notification').
   * @param {any[]} input.hookArgs - The arguments for the hook function; must be JSON serializable.
   * @param {HookOptions} input.options={} - Extended hook options (taskQueue, namespace, etc).
   * @returns {Promise<string>} The signal id.
   *
   * @example
   * // Hook a function
   * const signalId = await meshData.hook({
   *   entity: 'greeting',
   *   id: 'jsmith123',
   *   hookEntity: 'greeting.newsletter',
   *   hookArgs: ['xxxx@xxxxx'],
   *   options: {}
   * });
   */
  async hook({
    entity,
    id,
    hookEntity,
    hookArgs,
    options = {},
  }: HookInput): Promise<string> {
    const workflowId = MeshData.mintGuid(entity, id);
    this.validate(workflowId);
    const args = [
      ...hookArgs,
      { ...options, $guid: workflowId, $type: 'hook' },
    ];
    return await this.getClient().workflow.hook({
      namespace: options.namespace,
      args,
      taskQueue: options.taskQueue ?? hookEntity,
      workflowName: hookEntity,
      workflowId: options.workflowId ?? workflowId,
      config: options.config ?? undefined,
    });
  }

  /**
   * Executes a remote function by its global entity identifier with specified arguments.
   * If options.ttl is infinity, the function will be cached indefinitely and can only be
   * removed by calling `flush`. During this time, the function will remain active and
   * its state can be augmented by calling `set`, `incr`, `del`, etc OR by calling a
   * transactional 'hook' function.
   *
   * @template T The expected return type of the remote function.
   *
   * @param {object} input - The execution parameters.
   * @param {string} input.entity - The function entity name (e.g., 'user', 'order', 'user.bill').
   * @param {any[]} input.args - The arguments for the remote function.
   * @param {CallOptions} input.options={} - Extended configuration options for execution (e.g, taskQueue).
   *
   * @returns {Promise<T>} A promise that resolves with the result of the remote function execution. If
   *                     the input options include `await: false`, the promise will resolve with the
   *                     workflow ID (string) instead of the result. Make sure to pass string as the
   *                     return type if you are using `await: false`.
   *
   * @example
   * // Invoke a remote function with arguments and options
   * const response = await meshData.exec({
   *   entity: 'greeting',
   *   args: ['jsmith@hotmesh', { first: 'Jan' }],
   *   options: { ttl: '15 minutes', id: 'jsmith123' }
   * });
   */
  async exec<T>({ entity, args = [], options = {} }: ExecInput): Promise<T> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, options.id);
    this.validate(workflowId);

    const client = this.getClient();
    try {
      //check the cache
      const handle = await client.workflow.getHandle(
        entity,
        entity,
        workflowId,
        options.namespace,
      );
      const state = await handle.hotMesh.getState(
        `${handle.hotMesh.appId}.execute`,
        handle.workflowId,
      );
      if (state?.data?.done) {
        return state.data.response as unknown as T;
      }
      //202 `pending`; await the result
      return (await handle.result()) as unknown as T;
    } catch (e) {
      //create, since not found; then await the result
      const optionsClone = { ...options };
      let seconds: number;
      if (optionsClone.ttl) {
        //setting ttl requires the workflow to remain open
        if (optionsClone.signalIn !== false) {
          optionsClone.signalIn = true; //explicit 'true' forces open
        }
        if (optionsClone.ttl === 'infinity') {
          delete optionsClone.ttl;
          //max expire seconds in Redis (68 years)
          seconds = MAX_DELAY;
        } else {
          seconds = s(optionsClone.ttl);
        }
      }
      delete optionsClone.search;
      delete optionsClone.config;
      const handle = await client.workflow.start({
        args: [...args, { ...optionsClone, $guid: workflowId, $type: 'exec' }],
        taskQueue: options.taskQueue ?? entity,
        workflowName: entity,
        workflowId: options.workflowId ?? workflowId,
        config: options.config ?? undefined,
        search: options.search,
        workflowTrace: options.workflowTrace,
        workflowSpan: options.workflowSpan,
        namespace: options.namespace,
        await: options.await,
        marker: options.marker,
        pending: options.pending,
        expire: seconds ?? options.expire,
        //`persistent` flag keeps the job alive and accepting signals
        persistent: options.signalIn == false ? undefined : seconds && true,
        signalIn: optionsClone.signalIn,
      });
      if (options.await === false) {
        return handle.workflowId as unknown as T;
      }
      return (await handle.result()) as unknown as T;
    }
  }

  /**
   * Retrieves the job profile for the function execution, including metadata such as
   * execution status and result.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - identifier for the job
   * @param {CallOptions} [options={}] - Configuration options for the execution,
   *                                     including custom IDs, time-to-live (TTL) settings, etc.
   *                                     Defaults to an empty object if not provided.
   *
   * @returns {Promise<JobOutput>} A promise that resolves with the job's output, which
   *                               includes metadata about the job's execution status. The
   *                               structure of `JobOutput` should contain all relevant
   *                               information such as execution result, status, and any
   *                               error messages if the job failed.
   *
   * @example
   * // Retrieve information about a remote function's execution by job ID
   * const jobInfoById = await meshData.info('greeting', 'job-12345');
   *
   * // Response: JobOutput
   * {
   *   metadata: {
   *    tpc: 'durable.execute',
   *    app: 'durable',
   *    vrs: '1',
   *    jid: 'greeting-jsmith123',
   *    aid: 't1',
   *    ts: '0',
   *    jc: '20240208014803.980',
   *    ju: '20240208065017.762',
   *    js: 0
   *   },
   *   data: {
   *    done: true,
   *    response: 'Hello, Jan. Your email is [jsmith@hotmesh.com].',
   *    workflowId: 'greeting-jsmith123'
   *   }
   * }
   */
  async info(
    entity: string,
    id: string,
    options: CallOptions = {},
  ): Promise<JobOutput> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);

    const handle = await this.getClient().workflow.getHandle(
      options.taskQueue ?? entity,
      entity,
      workflowId,
      options.namespace,
    );
    return await handle.hotMesh.getState(
      `${handle.hotMesh.appId}.execute`,
      handle.workflowId,
    );
  }

  /**
   * Exports the job profile for the function execution, including
   * all state, process, and timeline data. The information in the export
   * is sufficient to capture the full state of the function in the moment
   * and over time.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - The workflow/job id
   * @param {ExportOptions} [options={}] - Configuration options for the export
   * @param {string} [namespace='durable'] - the namespace for the client
   *
   * @example
   * // Export a function
   * await meshData.export('greeting', 'jsmith123');
   */
  async export(
    entity: string,
    id: string,
    options?: ExportOptions,
    namespace?: string,
  ): Promise<MeshFlowJobExport> {
    const workflowId = MeshData.mintGuid(entity, id);
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      namespace,
    );
    return await handle.export(options);
  }

  /**
   * Returns the remote function state. this is different than the function response
   * returned by the `exec` method which represents the return value from the
   * function at the moment it completed. Instead, function state represents
   * mutable shared state that can be set:
   * 1) when the record is first created (provide `options.search.data` to `exec`)
   * 2) during function execution ((await MeshData.workflow.search()).set(...))
   * 3) during hook execution ((await MeshData.workflow.search()).set(...))
   * 4) via the meshData SDK (`meshData.set(...)`)
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the job id
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<StringAnyType>} - the function state
   *
   * @example
   * // get the state of a function
   * const state = await meshData.get('greeting', 'jsmith123', { fields: ['fred', 'barney'] });
   *
   * // returns { fred: 'flintstone', barney: 'rubble' }
   */
  async get(
    entity: string,
    id: string,
    options: CallOptions = {},
  ): Promise<StringAnyType> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);

    let prefixedFields: string[] = [];
    if (Array.isArray(options.fields)) {
      prefixedFields = options.fields.map((field) => `_${field}`);
    } else if (this.search?.schema) {
      prefixedFields = Object.entries(this.search.schema).map(
        ([key, value]: [string, StringAnyType]) => {
          return 'fieldName' in value ? value.fieldName.toString() : `_${key}`;
        },
      );
    } else {
      return await this.all(entity, id, options);
    }

    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      options.namespace,
    );
    const store = handle.hotMesh.engine?.store;
    const jobKey = await this.mintKey(entity, workflowId, options.namespace);
    const vals = await store?.exec('HMGET', jobKey, ...prefixedFields);
    const result = prefixedFields.reduce(
      (obj, field: string, index) => {
        obj[field.substring(1)] = vals?.[index];
        return obj;
      },
      {} as { [key: string]: any },
    );

    return result;
  }

  /**
   * Returns the remote function state for all fields. NOTE:
   * `all` can be less efficient than calling `get` as it returns all
   * fields (HGETALL), not just the ones requested (HMGET). Depending
   * upon the duration of the workflow, this could represent a large
   * amount of process/history data.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the workflow/job id
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<StringAnyType>} - the function state
   *
   * @example
   * // get the state of the job (this is not the response...this is job state)
   * const state = await meshData.all('greeting', 'jsmith123');
   *
   * // returns { fred: 'flintstone', barney: 'rubble', ...  }
   */
  async all(
    entity: string,
    id: string,
    options: CallOptions = {},
  ): Promise<StringAnyType> {
    const rawResponse = await this.raw(entity, id, options);
    const responseObj = {};
    for (const key in rawResponse) {
      if (key.startsWith('_')) {
        responseObj[key.substring(1)] = rawResponse[key];
      }
    }
    return responseObj;
  }

  /**
   * Returns all fields in the HASH record from Redis (HGETALL). Record
   * fields include the following:
   *
   * 1) `:`:                 workflow status (a semaphore where `0` is complete)
   * 2) `_*`:                function state (name/value pairs are prefixed with `_`)
   * 3) `-*`:                workflow cycle state (cycles are prefixed with `-`)
   * 4) `[a-zA-Z]{3}`:       mutable workflow job state
   * 5) `[a-zA-Z]{3}[,\d]+`: immutable workflow activity state
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the workflow/job id
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<StringAnyType>} - the function state
   *
   * @example
   * // get the state of a function
   * const state = await meshData.raw('greeting', 'jsmith123');
   *
   * // returns { : '0', _barney: 'rubble', aBa: 'Hello, John Doe. Your email is [jsmith@hotmesh].', ... }
   */
  async raw(
    entity: string,
    id: string,
    options: CallOptions = {},
  ): Promise<StringAnyType> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      options.namespace,
    );
    const store = handle.hotMesh.engine?.store;
    const jobKey = await this.mintKey(entity, workflowId, options.namespace);
    const rawResponse = (await store?.exec('HGETALL', jobKey)) as string[];
    const responseObj = {};
    for (let i = 0; i < rawResponse.length; i += 2) {
      responseObj[rawResponse[i] as string] = rawResponse[i + 1];
    }
    return responseObj;
  }

  /**
   * Sets the remote function state. this is different than the function response
   * returned by the exec method which represents the return value from the
   * function at the moment it completed. Instead, function state represents
   * mutable shared state that can be set
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the job id
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<number>} - count
   * @example
   * // set the state of a function
   * const count = await meshData.set('greeting', 'jsmith123', { search: { data: { fred: 'flintstone', barney: 'rubble' } } });
   */
  async set(
    entity: string,
    id: string,
    options: CallOptions = {},
  ): Promise<number> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      options.namespace,
    );
    const store = handle.hotMesh.engine?.store;
    const jobId = await this.mintKey(entity, workflowId, options.namespace);
    const safeArgs: string[] = [];
    for (const key in options.search?.data) {
      safeArgs.push(this.safeKey(key), options.search?.data[key].toString());
    }
    return (await store?.exec('HSET', jobId, ...safeArgs)) as unknown as number;
  }

  /**
   * Increments a field in the remote function state.
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the job id
   * @param {string} field - the field name
   * @param {number} amount - the amount to increment
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<number>} - the new value
   * @example
   * // increment a field in the function state
   * const count = await meshData.incr('greeting', 'jsmith123', 'counter', 1);
   */
  async incr(
    entity: string,
    id: string,
    field: string,
    amount: number,
    options: CallOptions = {},
  ): Promise<number> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      options.namespace,
    );
    const store = handle.hotMesh.engine?.store;
    const jobId = await this.mintKey(entity, workflowId, options.namespace);
    const result = await store?.exec(
      'HINCRBYFLOAT',
      jobId,
      this.safeKey(field),
      amount.toString(),
    );
    return Number(result as string);
  }

  /**
   * Deletes one or more fields from the remote function state.
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {string} id - the job id
   * @param {CallOptions} [options={}] - call options
   *
   * @returns {Promise<number>} - the count of fields deleted
   * @example
   * // remove two hash fields from the function state
   * const count = await meshData.del('greeting', 'jsmith123', { fields: ['fred', 'barney'] });
   */
  async del(entity: string, id: string, options: CallOptions): Promise<number> {
    const workflowId = MeshData.mintGuid(options.prefix ?? entity, id);
    this.validate(workflowId);
    if (!Array.isArray(options.fields)) {
      throw 'Invalid arguments [options.fields is not an array]';
    }
    const prefixedFields = options.fields.map((field) => `_${field}`);
    const handle = await this.getClient().workflow.getHandle(
      entity,
      entity,
      workflowId,
      options.namespace,
    );
    const store = handle.hotMesh.engine?.store;
    const jobKey = await this.mintKey(entity, workflowId, options.namespace);
    const count = await store?.exec('HDEL', jobKey, ...prefixedFields);
    return Number(count);
  }

  /**
   * For those Redis implementations without the FT module, this quasi-equivalent
   * method is provided that uses SCAN along with a custom match
   * string to view jobs. A cursor is likewise provided in support
   * of rudimentary pagination.
   * @param {FindJobsOptions} [options]
   * @returns {Promise<[string, string[]]>}
   * @example
   * // find jobs
   * const [cursor, jobs] = await meshData.findJobs({ match: 'greeting*' });
   *
   * // returns [ '0', [ 'hmsh:durable:j:greeting-jsmith123', 'hmsh:durable:j:greeting-jdoe456' ] ]
   */
  async findJobs(options: FindJobsOptions = {}): Promise<[string, string[]]> {
    const hotMesh = await this.getHotMesh(options.namespace);
    return (await hotMesh.engine?.store?.findJobs(
      options.match,
      options.limit,
      options.batch,
      options.cursor,
    )) as [string, string[]];
  }

  /**
   * Executes the redis FT search query; optionally specify other commands
   * @example '@_quantity:[89 89]'
   * @example '@_quantity:[89 89] @_name:"John"'
   * @example 'FT.search my-index @_quantity:[89 89]'
   * @param {FindOptions} options
   * @param {any[]} args
   * @returns {Promise<string[] | [number] | Array<number, string | number | string[]>>}
   */
  async find(
    entity: string,
    options: FindOptions,
    ...args: string[]
  ): Promise<string[] | [number] | Array<string | number | string[]>> {
    return await this.getClient().workflow.search(
      options.taskQueue ?? entity,
      options.workflowName ?? entity,
      options.namespace || 'durable',
      options.index ?? options.search?.index ?? this.search.index ?? '',
      ...args,
    ); //[count, [id, fields[]], [id, fields[]], [id, fields[]], ...]]
  }

  /**
   * Provides a JSON abstraction for the Redis FT.search command
   * (e.g, `count`, `query`, `return`, `limit`)
   * NOTE: If the type is TAG for an entity, `.`, `@`, and `-` must be escaped.
   *
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {FindWhereOptions} options - find options (the query). A custom search schema may be provided to target any index on the Redis backend.
   * @returns {Promise<SearchResults | number>} Returns a number if `count` is true, otherwise a SearchResults object.
   * @example
   * const results = await meshData.findWhere('greeting', {
   *  query: [
   *   { field: 'name', is: '=', value: 'John' },
   *   { field: 'age', is: '>', value: 2 },
   *   { field: 'quantity', is: '[]', value: [89, 89] }
   *  ],
   *  count: false,
   *  limit: { start: 0, size: 10 },
   *  return: ['name', 'quantity']
   * });
   *
   * // returns { count: 1, query: 'FT.SEARCH my-index @_name:"John" @_age:[2 +inf] @_quantity:[89 89] LIMIT 0 10', data: [ { name: 'John', quantity: '89' } ] }
   */
  async findWhere(
    entity: string,
    options: FindWhereOptions,
  ): Promise<SearchResults | number> {
    const targetSearch = options.options?.search ?? this.search;
    const args: string[] = [
      this.generateSearchQuery(options.query, targetSearch),
    ];
    if (options.count) {
      args.push('LIMIT', '0', '0');
    } else {
      //limit which hash fields to return
      args.push('RETURN');
      args.push(((options.return?.length ?? 0) + 1).toString());
      args.push('$');
      options.return?.forEach((returnField) => {
        if (returnField.startsWith('"')) {
          //request a literal hash value
          args.push(returnField.slice(1, -1));
        } else {
          //request a search hash value
          args.push(`_${returnField}`);
        }
      });
      //paginate
      if (options.limit) {
        args.push(
          'LIMIT',
          options.limit.start.toString(),
          options.limit.size.toString(),
        );
      }
    }
    const FTResults = await this.find(entity, options.options ?? {}, ...args);
    const count = FTResults[0] as number;
    const sargs = `FT.SEARCH ${options.options?.index ?? targetSearch?.index} ${args.join(' ')}`;
    if (options.count) {
      //always return number format if count is requested
      return !isNaN(count) || count > 0 ? count : 0;
    } else if (count === 0) {
      return { count, query: sargs, data: [] };
    }
    const hashes = this.arrayToHash(
      FTResults as [number, ...Array<string | string[]>],
    );
    return { count, query: sargs, data: hashes };
  }

  /**
   * Generates a search query from a FindWhereQuery array
   * @param {FindWhereQuery[] | string} [query]
   * @param {WorkflowSearchOptions} [search]
   * @returns {string}
   * @private
   */
  generateSearchQuery(
    query: FindWhereQuery[] | string,
    search?: WorkflowSearchOptions,
  ): string {
    if (!Array.isArray(query) || query.length === 0) {
      return typeof query === 'string' ? (query as string) : '*';
    }
    const queryString = query
      .map((q) => {
        const { field, is, value, type } = q;
        let prefixedFieldName: string;
        //insert the underscore prefix if requested field in query is not a literal
        if (search?.schema && field in search.schema) {
          if ('fieldName' in search.schema[field]) {
            prefixedFieldName = `@${search.schema[field].fieldName}`;
          } else {
            prefixedFieldName = `@_${field}`;
          }
        } else {
          prefixedFieldName = `@${field}`;
        }
        const fieldType = search?.schema?.[field]?.type ?? type ?? 'TEXT';

        switch (fieldType) {
          case 'TAG':
            return `${prefixedFieldName}:{${value}}`;
          case 'TEXT':
            return `${prefixedFieldName}:"${value}"`;
          case 'NUMERIC':
            let range = '';
            if (is.startsWith('=')) {
              //equal
              range = `[${value} ${value}]`;
            } else if (is.startsWith('<')) {
              //less than or equal
              range = `[-inf ${value}]`;
            } else if (is.startsWith('>')) {
              //greater than or equal
              range = `[${value} +inf]`;
            } else if (is === '[]') {
              //between
              range = `[${value[0]} ${value[1]}]`;
            }
            return `${prefixedFieldName}:${range}`;
          default:
            return '';
        }
      })
      .join(' ');
    return queryString;
  }

  /**
   * Creates a search index for the specified entity (FT.search). The index
   * must be removed by calling `FT.DROP_INDEX` directly in Redis.
   * @param {string} entity - the entity name (e.g, 'user', 'order', 'product')
   * @param {CallOptions} [options={}] - call options
   * @param {WorkflowSearchOptions} [searchOptions] - search options
   * @returns {Promise<string>} - the search index name
   * @example
   * // create a search index for the 'greeting' entity. pass in search options.
   * const index = await meshData.createSearchIndex('greeting', {}, { prefix: 'greeting', ... });
   *
   * // creates a search index for the 'greeting' entity, using the default search options.
   * const index = await meshData.createSearchIndex('greeting');
   */
  async createSearchIndex(
    entity: string,
    options: CallOptions = {},
    searchOptions?: WorkflowSearchOptions,
  ): Promise<void> {
    const workflowTopic = `${options.taskQueue ?? entity}-${entity}`;
    const hotMeshClient = await this.getClient().getHotMeshClient(
      workflowTopic,
      options.namespace,
    );
    return await MeshFlow.Search.configureSearchIndex(
      hotMeshClient,
      searchOptions ?? this.search,
    );
  }

  /**
   * Lists all search indexes in the operational data layer when the
   * targeted Redis backend supports the FT module.
   * @returns {Promise<string[]>}
   * @example
   * // list all search indexes
   * const indexes = await meshData.listSearchIndexes();
   *
   * // returns ['greeting', 'user', 'order', 'product']
   */
  async listSearchIndexes(): Promise<string[]> {
    const hotMeshClient = await this.getHotMesh();
    return await MeshFlow.Search.listSearchIndexes(hotMeshClient);
  }

  /**
   * Wrap activities in a proxy that will durably run them, once.
   */
  static proxyActivities = MeshFlow.workflow.proxyActivities;

  /**
   * shut down MeshData (typically on sigint or sigterm)
   */
  static async shutdown() {
    await MeshFlow.shutdown();
  }
}

export { MeshData };
