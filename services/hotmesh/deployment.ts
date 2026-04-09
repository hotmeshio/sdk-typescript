import { EngineService } from '../engine';
import { QuorumService } from '../quorum';
import { HotMeshManifest } from '../../types/hotmesh';

interface DeploymentContext {
  engine: EngineService | null;
  quorum: QuorumService | null;
}

/**
 * Preview changes and provide an analysis of risk prior to deployment.
 * @private
 */
export async function plan(
  instance: DeploymentContext,
  path: string,
): Promise<HotMeshManifest> {
  return await instance.engine?.plan(path);
}

/**
 * Deploys a YAML workflow graph to Postgres.
 */
export async function deploy(
  instance: DeploymentContext,
  pathOrYAML: string,
): Promise<HotMeshManifest> {
  return await instance.engine?.deploy(pathOrYAML);
}

/**
 * Activates a deployed version across the entire mesh.
 */
export async function activate(
  instance: DeploymentContext,
  version: string,
  delay?: number,
): Promise<boolean> {
  return await instance.quorum?.activate(version, delay);
}
