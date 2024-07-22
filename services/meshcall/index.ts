import { HotMesh } from "../hotmesh";
import { HMSH_LOGLEVEL, HMSH_QUORUM_DELAY_MS } from "../../modules/enums";
import { sleepFor } from "../../modules/utils";
import {
  MeshCallConnectParams,
  MeshCallCronParams,
  MeshCallExecParams,
  MeshCallFlushParams,
  MeshCallInterruptParams
} from "../../types/meshcall";
import { RedisConfig } from "../../types";

/**
 * MeshCall connects your functions to the Redis-backed mesh,
 * exposing them as idempotent endpoints. Call functions
 * from anywhere on the network with a connection to Redis. Function
 * responses are cacheable and functions can even
 * run as cyclical cron jobs (this one runs once a day).
 * 
 * @example
 * ```typescript
 * MeshCall.cron({
 *   namespace: 'demo',
 *   topic: 'my.cron.function',
 *   redis: {
 *     class: Redis,
 *     options: { url: 'redis://:key_admin@redis:6379' }
 *   },
 *   callback: async () => {
 *     //your code here...
 *   },
 *   options: { id: 'myDailyCron123', interval: '1 day' }
 * });
 * ```
 */
class MeshCall {
  /**
   * @private
   */
  static instances = new Map<string, any>();

  /**
   * @private
   */
  static connectons = new Map<string, any>();

  /**
   * @private
   */
  constructor() {}

  /**
   * @private
   */
  static getHotMeshClient = async (topic: string, namespace: string, connection: RedisConfig) => {
    //namespace isolation requires the connection options to be hashed
    //as multiple intersecting databases can be used by the same service
    const targetNS = namespace ?? 'hmsh';
    const connectionNS = `${targetNS}${topic}`;
    if (MeshCall.instances.has(connectionNS)) {
      const hotMeshClient = await MeshCall.instances.get(connectionNS);
      await MeshCall.verifyWorkflowActive(hotMeshClient, targetNS);
      return hotMeshClient;
    }

    //create and cache an instance
    const hotMeshClient = HotMesh.init({
      appId: targetNS,
      logLevel: HMSH_LOGLEVEL,
      engine: {
        redis: {
          class: connection.class,
          options: connection.options,
        },
      },
    });
    MeshCall.instances.set(connectionNS, hotMeshClient);
    await MeshCall.activateWorkflow(await hotMeshClient, targetNS);
    return hotMeshClient;
  };

  /**
   * @private
   */
  static async verifyWorkflowActive(
    hotMesh: HotMesh,
    appId = 'hmsh',
    count = 0,
  ): Promise<boolean> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as number;
    if (isNaN(appVersion)) {
      if (count > 10) {
        throw new Error('Workflow failed to activate');
      }
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await MeshCall.verifyWorkflowActive(hotMesh, appId, count + 1);
    }
    return true;
  }

  /**
   * @private
   */
  static async activateWorkflow(
    hotMesh: HotMesh,
    appId = 'hmsh',
    version = '1',
  ): Promise<void> {
    const app = await hotMesh.engine.store.getApp(appId);
    const appVersion = app?.version as unknown as string;
    if (appVersion === version && !app.active) {
      try {
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-activate-err', { error });
        throw error;
      }
    } else if (isNaN(Number(appVersion)) || appVersion < version) {
      try {
        //await hotMesh.deploy(getWorkflowYAML(appId, version));
        await hotMesh.activate(version);
      } catch (error) {
        hotMesh.engine.logger.error('durable-client-deploy-activate-err', {
          ...error,
        });
        throw error;
      }
    }
  }

  static async shutdown(): Promise<void> {
    for (const [_, hotMeshInstance] of MeshCall.instances) {
      (await hotMeshInstance).stop();
    }
  }

  static connect(params: MeshCallConnectParams): void {
    const { namespace, topic } = params;
    // Connection logic to register the function on the mesh
    console.log(`Function connected under namespace: ${namespace}, topic: ${topic}`);
  }

  static async exec(params: MeshCallExecParams): Promise<any> {
    const { namespace, topic, args } = params;
    console.log(`Executing function in namespace: ${namespace}, topic: ${topic}`);
    // Dummy response, replace with actual logic
    return { hello: args ? args[0] : 'world' };
  }

  static async flush(params: MeshCallFlushParams): Promise<void> {
    const { namespace, topic } = params;
    console.log(`Flushing cache for namespace: ${namespace}, topic: ${topic}`);
  }

  static cron(params: MeshCallCronParams): void {
    const { namespace, topic } = params;
    console.log(`Cron job set for namespace: ${namespace}, topic: ${topic}`);
  }

  static interrupt(params: MeshCallInterruptParams): void {
    const { namespace, topic } = params;
    console.log(`Interrupting cron job in namespace: ${namespace}, topic: ${topic}`);
  }
}

export { MeshCall };
