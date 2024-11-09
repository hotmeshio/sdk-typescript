import { ILogger } from '../../logger';

export interface StreamInitializable {
  init(namespace: string, appId: string, logger: ILogger): Promise<void>;
}
