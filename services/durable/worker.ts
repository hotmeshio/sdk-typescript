import {
  HMSH_CODE_DURABLE_ALL,
  HMSH_CODE_DURABLE_RETRYABLE,
  HMSH_CODE_DURABLE_TIMEOUT,
  HMSH_CODE_DURABLE_MAXED,
  HMSH_CODE_DURABLE_FATAL,
  HMSH_DURABLE_EXP_BACKOFF,
  HMSH_DURABLE_MAX_INTERVAL,
  HMSH_DURABLE_MAX_ATTEMPTS,
  HMSH_LOGLEVEL,
} from '../../modules/enums';
import { CancelledFailure } from './workflow/cancellationScope';
import {
  DurableChildError,
  DurableContinueAsNewError,
  DurableFatalError,
  DurableMaxedError,
  DurableProxyError,
  DurableRetryError,
  DurableSleepError,
  DurableTimeoutError,
  DurableWaitForError,
} from '../../modules/errors';
import { asyncLocalStorage, activityAsyncLocalStorage } from '../../modules/storage';
import { formatISODate, guid, hashOptions, s } from '../../modules/utils';
import { HotMesh } from '../hotmesh';
import {
  ActivityWorkflowDataType,
  Connection,
  Registry,
  WorkerConfig,
  WorkerOptions,
  WorkflowDataType,
} from '../../types/durable';
import {
  StreamData,
  StreamDataResponse,
  StreamStatus,
} from '../../types/stream';

import { Search } from './search';
import { APP_ID, APP_VERSION, getWorkflowYAML } from './schemas/factory';
import { DurableTelemetryService } from './telemetry';
import { TelemetryService } from '../telemetry';
import { SpanStatusCode } from '../../types/telemetry';

import { Durable } from './index';

/**
 * Hosts workflow and activity functions, connecting them to Postgres
 * for durable execution, replay, and automatic retry.
 *
 * ## Connection Modes
 *
 * ### Standard (legacy) — full admin access
 *
 * The worker connects with the same Postgres credentials as the engine.
 * Simple to set up; all workers share the same connection pool.
 *
 * ```typescript
 * const worker = await Durable.Worker.create({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgres://user:pass@host:5432/hotmesh' },
 *   },
 *   taskQueue: 'orders',
 *   workflow: orderWorkflow,
 * });
 * ```
 *
 * ### Secured — scoped Postgres role (recommended for production)
 *
 * The worker connects as a restricted Postgres role that can only
 * dequeue/ack/respond on its assigned stream names. All data access
 * goes through SECURITY DEFINER stored procedures that validate the
 * role's `app.allowed_streams` session variable before executing.
 *
 * **Step 1**: Provision a scoped credential (run once, from the engine/admin):
 * ```typescript
 * const cred = await Durable.provisionWorkerRole({
 *   connection: { class: Postgres, options: adminPgOptions },
 *   namespace: 'durable',
 *   streamNames: ['orders-activity'],
 * });
 * // cred = { roleName: 'hmsh_wrk_durable_orders_activity', password: '...' }
 * ```
 *
 * **Step 2**: Pass the credential when creating the worker:
 * ```typescript
 * const worker = await Durable.Worker.create({
 *   connection: {
 *     class: Postgres,
 *     options: { host: 'pg.prod', port: 5432, database: 'hotmesh' },
 *   },
 *   taskQueue: 'orders',
 *   workflow: orderWorkflow,
 *   workerCredentials: { user: cred.roleName, password: cred.password },
 * });
 * ```
 *
 * The worker role **cannot**:
 * - SELECT/INSERT/UPDATE/DELETE any table directly
 * - Access `jobs`, `jobs_attributes`, or any engine tables
 * - Dequeue messages from other workers' streams
 * - LISTEN on other workers' notification channels
 *
 * See {@link Durable.provisionWorkerRole} for credential lifecycle management.
 *
 * ## Telemetry
 *
 * Workers automatically emit OpenTelemetry spans when an OTel SDK is
 * registered. Initialize the SDK **before** calling `create()`:
 *
 * ```typescript
 * import { NodeSDK } from '@opentelemetry/sdk-node';
 * import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
 * import { resourceFromAttributes } from '@opentelemetry/resources';
 * import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
 *
 * const sdk = new NodeSDK({
 *   resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: 'my-service' }),
 *   traceExporter: new OTLPTraceExporter({
 *     url: 'https://api.honeycomb.io/v1/traces',
 *     headers: { 'x-honeycomb-team': process.env.HONEYCOMB_API_KEY },
 *   }),
 * });
 * sdk.start();
 * ```
 *
 * | `HMSH_TELEMETRY` | Spans emitted |
 * |-------|---------------|
 * | `'info'` (default) | `WORKFLOW/START`, `WORKFLOW/COMPLETE`, `WORKFLOW/ERROR`, `ACTIVITY/{name}` |
 * | `'debug'` | All `info` spans + `DISPATCH/RETURN` per operation + engine internals |
 */
export class WorkerService {
  /**
   * @private
   */
  static activityRegistry: Registry = {}; //user's activities
  /**
   * @private
   */
  static instances = new Map<string, HotMesh | Promise<HotMesh>>();
  /**
   * @private
   */
  workflowRunner: HotMesh;
  /**
   * @private
   */
  activityRunner: HotMesh;

  /**
   * @private
   */
  static getHotMesh = async (
    workflowTopic: string,
    config?: Partial<WorkerConfig>,
    options?: WorkerOptions,
  ) => {
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${workflowTopic}`;

    if (WorkerService.instances.has(targetTopic)) {
      return await WorkerService.instances.get(targetTopic);
    }
    const hotMeshClient = HotMesh.init({
      logLevel: options?.logLevel ?? HMSH_LOGLEVEL,
      appId: targetNamespace,
      taskQueue: config.taskQueue,
      engine: {
        connection: { ...config?.connection },
      },
    });
    WorkerService.instances.set(targetTopic, hotMeshClient);
    await WorkerService.activateWorkflow(await hotMeshClient);
    return hotMeshClient;
  };

  static hashOptions(connection: Connection): string {
    if ('options' in connection) {
      //shorthand format
      return hashOptions(connection.options);
    } else {
      //longhand format (sub, store, stream, pub, search)
      const response = [];
      for (const p in connection) {
        if (connection[p].options) {
          response.push(hashOptions(connection[p].options));
        }
      }
      return response.join('');
    }
  }

  /**
   * @private
   */
  constructor() {}

  /**
   * @private
   */
  static async activateWorkflow(hotMesh: HotMesh) {
    const app = await hotMesh.engine.store.getApp(hotMesh.engine.appId);
    const appVersion = app?.version;
    if (!appVersion) {
      try {
        await hotMesh.deploy(
          getWorkflowYAML(hotMesh.engine.appId, APP_VERSION),
        );
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-deploy-activate-err', err);
        throw err;
      }
    } else if (app && !app.active) {
      try {
        await hotMesh.activate(APP_VERSION);
      } catch (err) {
        hotMesh.engine.logger.error('durable-worker-activate-err', err);
        throw err;
      }
    }
  }

  /**
   * @private
   */
  static registerActivities<ACT>(activities: ACT): Registry {
    if (
      typeof activities === 'function' &&
      typeof WorkerService.activityRegistry[activities.name] !== 'function'
    ) {
      WorkerService.activityRegistry[activities.name] = activities as Function;
    } else {
      Object.keys(activities).forEach((key) => {
        if (
          activities[key].name &&
          typeof WorkerService.activityRegistry[activities[key].name] !==
            'function'
        ) {
          WorkerService.activityRegistry[activities[key].name] = (
            activities as any
          )[key] as Function;
        } else if (typeof (activities as any)[key] === 'function') {
          WorkerService.activityRegistry[key] = (activities as any)[
            key
          ] as Function;
        }
      });
    }
    return WorkerService.activityRegistry;
  }

  /**
   * Register activity workers for a task queue. Activities are invoked via message queue,
   * so they can run on different servers from workflows.
   * 
   * The task queue name gets `-activity` appended automatically for the worker topic.
   * For example, `taskQueue: 'payment'` creates a worker listening on `payment-activity`.
   * 
   * @param config - Worker configuration (connection, namespace, taskQueue)
   * @param activities - Activity functions to register
   * @param activityTaskQueue - Task queue name (without `-activity` suffix).
   *                            Defaults to `config.taskQueue` if not provided.
   * 
   * @returns Promise<HotMesh> The initialized activity worker
   * 
   * @example
   * ```typescript
   * // Activity worker (can be on separate server)
   * import { Durable } from '@hotmeshio/hotmesh';
   * import { Client as Postgres } from 'pg';
   * 
   * const activities = {
   *   async processPayment(amount: number): Promise<string> {
   *     return `Processed $${amount}`;
   *   },
   *   async sendEmail(to: string, subject: string): Promise<void> {
   *     // Send email
   *   }
   * };
   * 
   * await Durable.registerActivityWorker({
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   taskQueue: 'payment'  // Listens on 'payment-activity'
   * }, activities, 'payment');
   * ```
   * 
   * @example
   * ```typescript
   * // Workflow worker (can be on different server)
   * async function orderWorkflow(orderId: string, amount: number) {
   *   const { processPayment, sendEmail } = Durable.workflow.proxyActivities<{
   *     processPayment: (amount: number) => Promise<string>;
   *     sendEmail: (to: string, subject: string) => Promise<void>;
   *   }>({
   *     taskQueue: 'payment',
   *     retry: { maximumAttempts: 3 }
   *   });
   * 
   *   const result = await processPayment(amount);
   *   await sendEmail('customer@example.com', 'Order confirmed');
   *   return result;
   * }
   * 
   * await Durable.Worker.create({
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   taskQueue: 'orders',
   *   workflow: orderWorkflow
   * });
   * ```
   * 
   * @example
   * ```typescript
   * // Shared activity pool for interceptors
   * await Durable.registerActivityWorker({
   *   connection: {
   *     class: Postgres,
   *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' }
   *   },
   *   taskQueue: 'shared'
   * }, { auditLog, collectMetrics }, 'shared');
   * 
   * const interceptor: WorkflowInboundCallsInterceptor = {
   *   async execute(ctx, next) {
   *     const { auditLog } = Durable.workflow.proxyActivities<{
   *       auditLog: (id: string, action: string) => Promise<void>;
   *     }>({
   *       taskQueue: 'shared',
   *       retry: { maximumAttempts: 3 }
   *     });
   *     await auditLog(ctx.get('workflowId'), 'started');
   *     return next();
   *   }
   * };
   * ```
   *
   * @example
   * ```typescript
   * // Secured worker with scoped Postgres credentials (VNF-style isolation)
   * // Step 1: Admin provisions a credential (one-time)
   * const cred = await Durable.provisionWorkerRole({
   *   connection: { class: Postgres, options: adminOptions },
   *   streamNames: ['payment-activity'],
   * });
   *
   * // Step 2: Worker connects with scoped role — can only access payment-activity
   * await Durable.registerActivityWorker({
   *   connection: { class: Postgres, options: { host: 'pg.prod', database: 'hotmesh' } },
   *   taskQueue: 'payment',
   *   workerCredentials: { user: cred.roleName, password: cred.password },
   * }, { processPayment, refundPayment });
   * ```
   */
  static async registerActivityWorker(
    config: Partial<WorkerConfig>,
    activities: any,
    activityTaskQueue?: string,
  ): Promise<HotMesh> {
    // Register as durable namespace so engine-layer spans are suppressed in 'info' mode
    const targetNamespace = config?.namespace ?? APP_ID;
    TelemetryService.durableNamespaces.add(targetNamespace);

    // Register activities globally in the registry
    WorkerService.registerActivities(activities);

    // Use provided activityTaskQueue or fall back to config.taskQueue
    const taskQueue = activityTaskQueue || config.taskQueue || 'durable-activities';

    // Append '-activity' suffix for the worker topic
    const activityTopic = `${taskQueue}-activity`;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${activityTopic}`;

    // Return existing worker if already initialized (idempotent)
    if (WorkerService.instances.has(targetTopic)) {
      return await WorkerService.instances.get(targetTopic);
    }

    // Create activity worker that listens on '{taskQueue}-activity' topic
    const workerEntry: any = {
      topic: activityTopic,
      connection: config.connection,
      callback: WorkerService.createActivityCallback(),
    };
    if (config.workerCredentials) {
      workerEntry.workerCredentials = config.workerCredentials;
    }
    const hotMeshWorker = await HotMesh.init({
      guid: config.guid ? `${config.guid}XA` : undefined,
      taskQueue,
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: targetNamespace,
      engine: { connection: config.connection },
      workers: [workerEntry],
    });

    WorkerService.instances.set(targetTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  /**
   * Create an activity callback function that can be used by activity workers
   * @private
   */
  static createActivityCallback(): (payload: StreamData) => Promise<StreamDataResponse> {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      let activitySpan: import('../../types/telemetry').Span | null = null;
      try {
        //always run the activity function when instructed; return the response
        const activityInput = data.data as unknown as ActivityWorkflowDataType;
        const activityName = activityInput.activityName;
        const activityFunction = WorkerService.activityRegistry[activityName];

        if (!activityFunction) {
          throw new Error(`Activity '${activityName}' not found in registry`);
        }

        const activityContext = new Map<string, any>();
        activityContext.set('activityName', activityName);
        activityContext.set('arguments', activityInput.arguments);
        activityContext.set('headers', activityInput.headers ?? {});
        activityContext.set('workflowId', activityInput.workflowId);
        activityContext.set('workflowTopic', activityInput.workflowTopic);

        // Start an ACTIVITY span if telemetry is enabled
        const trc = data.metadata.trc;
        const spn = data.metadata.spn;
        activitySpan =
          DurableTelemetryService.isEnabled() && trc && spn
            ? DurableTelemetryService.startSpan(
                trc,
                spn,
                `ACTIVITY/${activityName}`,
                {
                  'durable.activity.name': activityName,
                  'durable.workflow.id': activityInput.workflowId,
                },
              )
            : null;

        const interceptorService = Durable.getInterceptorService();
        const pojoResponse = await activityAsyncLocalStorage.run(
          activityContext,
          () => {
            const executeFn = () => activityFunction.apply(null, activityInput.arguments);
            if (interceptorService?.activityInbound?.length > 0) {
              return interceptorService.executeActivityInboundChain(
                activityName,
                activityInput.arguments,
                executeFn,
              );
            }
            return executeFn();
          },
        );

        activitySpan?.end();

        return {
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: pojoResponse },
        };
      } catch (err) {
        activitySpan?.setStatus({
          code: SpanStatusCode.ERROR,
          message: err.message,
        });
        activitySpan?.end();
        // Log error (note: we don't have access to this.activityRunner here)
        console.error('durable-worker-activity-err', {
          name: err.name,
          message: err.message,
          stack: err.stack,
        });
        
        if (
          !(err instanceof DurableTimeoutError) &&
          !(err instanceof DurableMaxedError) &&
          !(err instanceof DurableFatalError)
        ) {
          //use code 599 as a proxy for all retryable errors
          // (basically anything not 596, 597, 598)
          return {
            status: StreamStatus.SUCCESS,
            code: 599,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                code: HMSH_CODE_DURABLE_RETRYABLE,
              },
            },
          };
        } else if (err instanceof DurableTimeoutError) {
          return {
            status: StreamStatus.SUCCESS,
            code: 596,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                code: HMSH_CODE_DURABLE_TIMEOUT,
              },
            },
          };
        } else if (err instanceof DurableMaxedError) {
          return {
            status: StreamStatus.SUCCESS,
            code: 597,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                code: HMSH_CODE_DURABLE_MAXED,
              },
            },
          };
        } else if (err instanceof DurableFatalError) {
          return {
            status: StreamStatus.SUCCESS,
            code: 598,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                code: HMSH_CODE_DURABLE_FATAL,
              },
            },
          };
        }
      }
    };
  }

  /**
   * Creates and starts a workflow worker.
   *
   * @example
   * ```typescript
   * import { Durable } from '@hotmeshio/hotmesh';
   * import { Client as Postgres } from 'pg';
   * import * as workflows from './workflows';
   *
   * async function run() {
   *   const worker = await Durable.Worker.create({
   *     connection: {
   *       class: Postgres,
   *       options: {
   *         connectionString: 'postgres://user:password@localhost:5432/db'
   *       },
   *     },
   *     taskQueue: 'default',
   *     workflow: workflows.example,
   *   });
   *
   *   await worker.run();
   * }
   * ```
   */
  static async create(config: WorkerConfig): Promise<WorkerService> {
    // Register as durable namespace so engine-layer spans are suppressed in 'info' mode
    const targetNamespace = config?.namespace ?? APP_ID;
    TelemetryService.durableNamespaces.add(targetNamespace);

    const workflow = config.workflow;
    const [workflowFunctionName, workflowFunction] =
      WorkerService.resolveWorkflowTarget(workflow);
    // Separate taskQueue from workflowName - no concatenation for stream_name
    const taskQueue = config.taskQueue;
    const activityTopic = `${taskQueue}-activity`;
    // workflowTopic remains concatenated for engine-internal routing (graph.subscribes)
    const workflowTopic = `${taskQueue}-${workflowFunctionName}`;

    // Register activities passed via config
    if (config.activities) {
      WorkerService.registerActivities(config.activities);
    }

    //initialize supporting workflows
    const worker = new WorkerService();
    worker.activityRunner = await worker.initActivityWorker(
      config,
      activityTopic,
    );
    worker.workflowRunner = await worker.initWorkflowWorker(
      config,
      taskQueue,
      workflowFunctionName,
      workflowTopic,
      workflowFunction,
    );
    Search.configureSearchIndex(worker.workflowRunner, config.search);
    await WorkerService.activateWorkflow(worker.workflowRunner);
    return worker;
  }

  /**
   * @private
   */
  static resolveWorkflowTarget(
    workflow: object | Function,
    name?: string,
  ): [string, Function] {
    let workflowFunction: Function;
    if (typeof workflow === 'function') {
      workflowFunction = workflow;
      return [workflowFunction.name ?? name, workflowFunction];
    } else {
      const workflowFunctionNames = Object.keys(workflow);
      const lastFunctionName =
        workflowFunctionNames[workflowFunctionNames.length - 1];
      workflowFunction = workflow[lastFunctionName];
      return WorkerService.resolveWorkflowTarget(
        workflowFunction,
        lastFunctionName,
      );
    }
  }

  /**
   * Run the connected worker; no-op (unnecessary to call)
   */
  async run() {
    this.workflowRunner.engine.logger.info('durable-worker-running');
  }

  /**
   * @private
   */
  async initActivityWorker(
    config: WorkerConfig,
    activityTopic: string,
  ): Promise<HotMesh> {
    const providerConfig = config.connection;
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${activityTopic}`;
    
    // Return existing worker if already initialized
    if (WorkerService.instances.has(targetTopic)) {
      return await WorkerService.instances.get(targetTopic);
    }
    
    const workerEntry: any = {
      topic: activityTopic,
      connection: providerConfig,
      callback: this.wrapActivityFunctions().bind(this),
    };
    if (config.workerCredentials) {
      workerEntry.workerCredentials = config.workerCredentials;
    }
    const hotMeshWorker = await HotMesh.init({
      guid: config.guid ? `${config.guid}XA` : undefined,
      taskQueue: config.taskQueue,
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: targetNamespace,
      engine: { connection: providerConfig },
      workers: [workerEntry],
    });
    WorkerService.instances.set(targetTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  /**
   * @private
   */
  wrapActivityFunctions(): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      let activitySpan: import('../../types/telemetry').Span | null = null;
      try {
        //always run the activity function when instructed; return the response
        const activityInput = data.data as unknown as ActivityWorkflowDataType;
        const activityName = activityInput.activityName;
        const activityFunction = WorkerService.activityRegistry[activityName];

        const activityContext = new Map<string, any>();
        activityContext.set('activityName', activityName);
        activityContext.set('arguments', activityInput.arguments);
        activityContext.set('headers', activityInput.headers ?? {});
        activityContext.set('workflowId', activityInput.workflowId);
        activityContext.set('workflowTopic', activityInput.workflowTopic);

        const interceptorService = Durable.getInterceptorService();

        // Start an ACTIVITY span if telemetry is enabled
        const trc = data.metadata.trc;
        const spn = data.metadata.spn;
        activitySpan =
          DurableTelemetryService.isEnabled() && trc && spn
            ? DurableTelemetryService.startSpan(
                trc,
                spn,
                `ACTIVITY/${activityName}`,
                {
                  'durable.activity.name': activityName,
                  'durable.workflow.id': activityInput.workflowId,
                },
              )
            : null;

        const activityPromise = activityAsyncLocalStorage.run(
          activityContext,
          () => {
            const executeFn = () => activityFunction.apply(this, activityInput.arguments);
            if (interceptorService?.activityInbound?.length > 0) {
              return interceptorService.executeActivityInboundChain(
                activityName,
                activityInput.arguments,
                executeFn,
              );
            }
            return executeFn();
          },
        );

        let pojoResponse: any;
        if (activityInput.startToCloseTimeout && activityInput.startToCloseTimeout > 0) {
          const timeoutMs = activityInput.startToCloseTimeout * 1000;
          pojoResponse = await Promise.race([
            activityPromise,
            new Promise((_, reject) =>
              setTimeout(
                () => reject(new DurableTimeoutError(
                  `Activity '${activityName}' exceeded startToCloseTimeout of ${activityInput.startToCloseTimeout}s`,
                )),
                timeoutMs,
              ),
            ),
          ]);
        } else {
          pojoResponse = await activityPromise;
        }

        activitySpan?.end();

        return {
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: pojoResponse },
        };
      } catch (err) {
        activitySpan?.setStatus({
          code: SpanStatusCode.ERROR,
          message: err.message,
        });
        activitySpan?.end();
        this.activityRunner.engine.logger.error('durable-worker-activity-err', {
          name: err.name,
          message: err.message,
          stack: err.stack,
        });
        if (
          !(err instanceof DurableTimeoutError) &&
          !(err instanceof DurableMaxedError) &&
          !(err instanceof DurableFatalError)
        ) {
          //use code 599 as a proxy for all retryable errors
          // (basically anything not 596, 597, 598)
          return {
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_DURABLE_RETRYABLE,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                stack: err.stack,
                timestamp: formatISODate(new Date()),
              },
            },
          } as StreamDataResponse;
        }

        return {
          //always returrn success (the Durable module is just fine);
          //  it's the user's function that has failed
          status: StreamStatus.SUCCESS,
          code: err.code,
          stack: err.stack,
          metadata: { ...data.metadata },
          data: {
            $error: {
              message: err.message,
              stack: err.stack,
              timestamp: formatISODate(new Date()),
              code: err.code,
            },
          },
        } as StreamDataResponse;
      }
    };
  }

  /**
   * @private
   */
  async initWorkflowWorker(
    config: WorkerConfig,
    taskQueue: string,
    workflowFunctionName: string,
    workflowTopic: string,
    workflowFunction: Function,
  ): Promise<HotMesh> {
    const providerConfig = config.connection;
    const targetNamespace = config?.namespace ?? APP_ID;
    const optionsHash = WorkerService.hashOptions(config?.connection);
    const targetTopic = `${optionsHash}.${targetNamespace}.${workflowTopic}`;
    const workerEntry: any = {
      topic: taskQueue,
      workflowName: workflowFunctionName,
      connection: providerConfig,
      callback: this.wrapWorkflowFunction(
        workflowFunction,
        workflowTopic,
        workflowFunctionName,
        config,
      ).bind(this),
    };
    if (config.workerCredentials) {
      workerEntry.workerCredentials = config.workerCredentials;
    }
    const hotMeshWorker = await HotMesh.init({
      guid: config.guid,
      taskQueue: config.taskQueue,
      logLevel: config.options?.logLevel ?? HMSH_LOGLEVEL,
      appId: config.namespace ?? APP_ID,
      engine: { connection: providerConfig },
      workers: [workerEntry],
    });
    WorkerService.instances.set(targetTopic, hotMeshWorker);
    return hotMeshWorker;
  }

  /**
   * @private
   */
  static Context = {
    info: () => {
      return {
        workflowId: '',
        workflowTopic: '',
      };
    },
  };

  /**
   * @private
   */
  wrapWorkflowFunction(
    workflowFunction: Function,
    workflowTopic: string,
    workflowFunctionName: string,
    config: WorkerConfig,
  ): Function {
    return async (data: StreamData): Promise<StreamDataResponse> => {
      const counter = { counter: 0 };
      const interruptionRegistry: any[] = [];
      const patchMarkers: Record<string, string> = {};
      let isProcessing = false;

      // Injects accumulated patch markers into any worker response so the
      // engine can persist them via the YAML schema's job.maps mechanism.
      // Include patchMarkers only when non-empty. The schema's
      // job.maps patch-marker[-] is a no-op when the reference is absent.
      const withPatchMarkers = (
        response: StreamDataResponse,
      ): StreamDataResponse => {
        if (Object.keys(patchMarkers).length > 0) {
          response.data = response.data || {};
          (response.data as any).patchMarkers = patchMarkers;
        }
        return response;
      };
      try {
        //incoming data payload has arguments and workflowId
        const workflowInput = data.data as unknown as WorkflowDataType;
        const context = new Map();
        context.set('canRetry', workflowInput.canRetry);
        context.set('expire', workflowInput.expire);
        context.set('counter', counter);
        context.set('interruptionRegistry', interruptionRegistry);
        context.set('patchMarkers', patchMarkers);
        context.set('connection', config.connection);
        context.set('namespace', config.namespace ?? APP_ID);
        context.set('raw', data);
        context.set('workflowId', workflowInput.workflowId);
        if (workflowInput.originJobId) {
          //if present there is an origin job to which this job is subordinated;
          // garbage collect (expire) this job when originJobId is expired
          context.set('originJobId', workflowInput.originJobId);
        }
        //TODO: the query is provider-specific;
        //      refactor as an abstract interface the provider must implement
        let replayQuery = '';
        if (workflowInput.workflowDimension) {
          //every hook function runs in an isolated dimension controlled
          //by the index assigned when the signal was received; even if the
          //hook function re-runs, its scope will always remain constant
          context.set('workflowDimension', workflowInput.workflowDimension);
          replayQuery = `-*${workflowInput.workflowDimension}-*`;
        } else if (workflowInput.continueGeneration) {
          //continueAsNew: use generation prefix for replay isolation;
          //old generation keys remain but are invisible to the new execution
          const genPrefix = `$${workflowInput.continueGeneration}`;
          context.set('workflowDimension', genPrefix);
          replayQuery = `-*${genPrefix}-*`;
        } else {
          //last letter of words like 'hook', 'sleep', 'wait', 'signal', 'search', 'start', 'proxy', 'child', 'collator', 'trace', 'enrich', 'publish'
          replayQuery = '-*[ehklptydr]-*';
        }
        context.set('workflowTopic', workflowTopic);
        context.set('workflowName', workflowFunctionName);
        context.set('taskQueue', config.taskQueue);
        context.set('workflowTrace', data.metadata.trc);
        context.set('workflowSpan', data.metadata.spn);
        const store = this.workflowRunner.engine.store;
        const [cursor, replay] = await store.findJobFields(
          workflowInput.workflowId,
          replayQuery,
          50_000,
          5_000,
        );
        context.set('replay', replay);
        context.set('cursor', cursor); // if != 0, more remain
        context.set('activityInterceptorService', Durable.getInterceptorService());

        // Execute workflow with interceptors
        const workflowResponse = await asyncLocalStorage.run(
          context,
          async () => {
            // Emit WORKFLOW/START on first execution (not replay)
            if (
              DurableTelemetryService.isEnabled() &&
              Object.keys(replay).length === 0
            ) {
              DurableTelemetryService.emitPointSpan(
                data.metadata.trc,
                data.metadata.spn,
                `WORKFLOW/START/${workflowFunctionName}`,
                {
                  'durable.workflow.id': workflowInput.workflowId,
                  'durable.workflow.name': workflowFunctionName,
                  'durable.workflow.topic': workflowTopic,
                },
              );
            }

            // Get the interceptor service
            const interceptorService = Durable.getInterceptorService();

            // Create the workflow execution function
            const execWorkflow = async () => {
              return await workflowFunction.apply(
                this,
                workflowInput.arguments,
              );
            };

            // Execute the workflow through the interceptor chain
            return await interceptorService.executeInboundChain(context, execWorkflow);
          },
        );

        // Emit WORKFLOW/COMPLETE when the workflow finishes successfully
        if (DurableTelemetryService.isEnabled()) {
          DurableTelemetryService.emitPointSpan(
            data.metadata.trc,
            data.metadata.spn,
            `WORKFLOW/COMPLETE/${workflowFunctionName}`,
            {
              'durable.workflow.id': workflowInput.workflowId,
              'durable.workflow.name': workflowFunctionName,
              'durable.workflow.topic': workflowTopic,
            },
          );
        }

        //if the embedded function has a try/catch, it can interrup the throw
        // throw here to interrupt the workflow if the embedded function caught and suppressed
        if (interruptionRegistry.length > 0) {
          const payload = interruptionRegistry[0];
          switch (payload.type) {
            case 'DurableWaitForError':
              throw new DurableWaitForError(payload);
            case 'DurableProxyError':
              throw new DurableProxyError(payload);
            case 'DurableChildError':
              throw new DurableChildError(payload);
            case 'DurableSleepError':
              throw new DurableSleepError(payload);
            case 'DurableContinueAsNewError':
              throw new DurableContinueAsNewError(payload);
            case 'DurableTimeoutError':
              throw new DurableTimeoutError(payload.message, payload.stack);
            case 'DurableMaxedError':
              throw new DurableMaxedError(payload.message, payload.stack);
            case 'DurableFatalError':
              throw new DurableFatalError(payload.message, payload.stack);
            case 'DurableRetryError':
              throw new DurableRetryError(payload.message, payload.stack);
            default:
              throw new DurableRetryError(
                `Unknown interruption type: ${payload.type}`,
              );
          }
        }

        return withPatchMarkers({
          code: 200,
          status: StreamStatus.SUCCESS,
          metadata: { ...data.metadata },
          data: { response: workflowResponse, done: true },
        });
      } catch (err) {
        if (isProcessing) {
          return;
        }
        if (
          err instanceof DurableWaitForError ||
          interruptionRegistry.length > 1
        ) {
          isProcessing = true;

          //NOTE: this type is spawned when `Promise.all` is used OR if the interruption is a `condition`
          const workflowInput = data.data as unknown as WorkflowDataType;
          const execIndex = counter.counter - interruptionRegistry.length + 1;
          const {
            workflowId,
            workflowTopic,
            workflowDimension,
            originJobId,
            expire,
          } = workflowInput;
          const collatorFlowId = `${guid()}$C`;
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_DURABLE_ALL,
            metadata: { ...data.metadata },
            data: {
              code: HMSH_CODE_DURABLE_ALL,
              items: [...interruptionRegistry],
              size: interruptionRegistry.length,
              workflowDimension: workflowDimension || '',
              index: execIndex,
              originJobId: originJobId || workflowId,
              parentWorkflowId: workflowId,
              workflowId: collatorFlowId,
              workflowTopic: workflowTopic,
              expire,
            },
          } as StreamDataResponse);
        } else if (err instanceof DurableSleepError) {
          //return the sleep interruption
          isProcessing = true;
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({
                duration: err.duration,
                index: err.index,
                workflowDimension: err.workflowDimension,
              }),
              duration: err.duration,
              index: err.index,
              workflowDimension: err.workflowDimension,
            },
          } as StreamDataResponse);
        } else if (err instanceof DurableProxyError) {
          //return the proxyActivity interruption
          isProcessing = true;
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              message: JSON.stringify({
                message: err.message,
                workflowId: err.workflowId,
                activityName: err.activityName,
                dimension: err.workflowDimension,
              }),
              arguments: err.arguments,
              headers: err.headers,
              workflowDimension: err.workflowDimension,
              index: err.index,
              originJobId: err.originJobId,
              parentWorkflowId: err.parentWorkflowId,
              expire: err.expire,
              workflowId: err.workflowId,
              workflowTopic: err.workflowTopic,
              activityName: err.activityName,
              backoffCoefficient: err.backoffCoefficient,
              initialInterval: err.initialInterval,
              maximumAttempts: err.maximumAttempts,
              maximumInterval: err.maximumInterval,
              startToCloseTimeout: err.startToCloseTimeout,
            },
          } as StreamDataResponse);
        } else if (err instanceof DurableChildError) {
          //return the child interruption
          isProcessing = true;
          const msg = {
            message: err.message,
            workflowId: err.workflowId,
            dimension: err.workflowDimension,
          };
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              arguments: err.arguments,
              await: err.await,
              backoffCoefficient:
                err.backoffCoefficient || HMSH_DURABLE_EXP_BACKOFF,
              code: err.code,
              index: err.index,
              initialInterval: err.initialInterval,
              message: JSON.stringify(msg),
              maximumAttempts: err.maximumAttempts || HMSH_DURABLE_MAX_ATTEMPTS,
              maximumInterval:
                err.maximumInterval || s(HMSH_DURABLE_MAX_INTERVAL),
              originJobId: err.originJobId,
              entity: err.entity,
              parentWorkflowId: err.parentWorkflowId,
              expire: err.expire,
              persistent: err.persistent,
              signalIn: err.signalIn,
              workflowDimension: err.workflowDimension,
              workflowId: err.workflowId,
              workflowTopic: err.workflowTopic,
              taskQueue: err.taskQueue,
              workflowName: err.workflowName,
            },
          } as StreamDataResponse);
        } else if (err instanceof DurableContinueAsNewError) {
          //return the continueAsNew interruption
          isProcessing = true;
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: err.code,
            metadata: { ...data.metadata },
            data: {
              code: err.code,
              arguments: err.arguments,
              index: err.index,
              workflowDimension: err.workflowDimension,
            },
          } as StreamDataResponse);
        }

        if (err instanceof CancelledFailure) {
          // CancelledFailure that wasn't caught by the workflow:
          // treat as fatal (no retry) so the workflow terminates.
          isProcessing = true;
          return withPatchMarkers({
            status: StreamStatus.SUCCESS,
            code: HMSH_CODE_DURABLE_FATAL,
            metadata: { ...data.metadata },
            data: {
              $error: {
                message: err.message,
                type: 'CancelledFailure',
                name: 'CancelledFailure',
                stack: err.stack,
                code: HMSH_CODE_DURABLE_FATAL,
              },
            },
          } as StreamDataResponse);
        }

        // ALL other errors are actual fatal errors (598, 597, 596)
        //  OR will be retried (599)
        if (DurableTelemetryService.isEnabled()) {
          DurableTelemetryService.emitPointSpan(
            data.metadata.trc,
            data.metadata.spn,
            `WORKFLOW/ERROR/${workflowFunctionName}`,
            {
              'durable.workflow.id':
                (data.data as unknown as WorkflowDataType).workflowId,
              'durable.workflow.name': workflowFunctionName,
              'error.message': err.message,
              'error.type': err.name || 'Error',
            },
            SpanStatusCode.ERROR,
            err.message,
          );
        }
        isProcessing = true;
        return withPatchMarkers({
          status: StreamStatus.SUCCESS,
          code: err.code || new DurableRetryError(err.message).code,
          metadata: { ...data.metadata },
          data: {
            $error: {
              message: err.message,
              type: err.name,
              name: err.name,
              stack: err.stack,
              code: err.code || new DurableRetryError(err.message).code,
            },
          },
        } as StreamDataResponse);
      }
    };
  }

  /**
   * @private
   */
  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of WorkerService.instances) {
      (await hotMeshInstance).stop();
    }
  }
}
