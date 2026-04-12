/**
 * YAML app compilation and deployment.
 *
 * Thin delegation to CompilerService — kept as a separate module
 * so the engine index reads as a clear list of capabilities.
 */

import { CompilerService } from '../compiler';
import { StoreService } from '../store';
import { StreamService } from '../stream';
import { ILogger } from '../logger';
import { HotMeshManifest } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

interface CompilerContext {
  store: StoreService<ProviderClient, ProviderTransaction>;
  stream: StreamService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
}

export async function plan(
  instance: CompilerContext,
  pathOrYAML: string,
): Promise<HotMeshManifest> {
  const compiler = new CompilerService(
    instance.store,
    instance.stream,
    instance.logger,
  );
  return await compiler.plan(pathOrYAML);
}

export async function deploy(
  instance: CompilerContext,
  pathOrYAML: string,
): Promise<HotMeshManifest> {
  const compiler = new CompilerService(
    instance.store,
    instance.stream,
    instance.logger,
  );
  return await compiler.deploy(pathOrYAML);
}
