import { ILogger } from '../../logger';
import { HotMeshApps } from '../../../types/hotmesh';

export interface StoreInitializable {
  init(namespace: string, appId: string, logger: ILogger): Promise<HotMeshApps>;
}
