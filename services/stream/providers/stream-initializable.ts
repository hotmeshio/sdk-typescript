import { ILogger } from '../../logger';

export interface StreamInitializable<T, U> {
  init(namespace: string, appId: string, logger: ILogger): Promise<void>;
  getMulti(): U;
}
