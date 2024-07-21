import { HotMesh } from "../hotmesh";
import { HMSH_LOGLEVEL, HMSH_QUORUM_DELAY_MS } from "../../modules/enums";
import { sleepFor } from "../../modules/utils";
import {
  MCConnectOptions,
  MCCronParams,
  MCExecParams
} from "../../types/meshcall";
import { RedisConfig } from "../../types";

/**
 * MeshCall is a fire-and-forget module that connects any function to the mesh.
 * Calls are brokered via Redis Streams, reducing the overhead of HTTP
 * without backpressure risk.
 */
class MeshCall {
  static instances = new Map<string, any>();

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

  static connect(options: MCConnectOptions): void {
    if (!MeshCall.instances.has(options.topic ?? 'hmsh')) {
      MeshCall.getHotMeshClient(
        options.topic ?? 'hmsh',
        options.namespace,
        options.redis
      );
    }

  }

  static async exec(params: MCExecParams): Promise<Record<string, any>> {

    return {}; // Placeholder return
  }

  static cron(params: MCCronParams): void {


  }
}

export { MeshCall };
