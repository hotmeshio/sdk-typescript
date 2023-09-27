import { EngineService } from '../engine';
import { Activity, ActivityType } from './activity';
import {
  ActivityData,
  ActivityMetadata,
  IterateActivity } from '../../types/activity';

class Iterate extends Activity {
  config: IterateActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService
    ) {
      super(config, data, metadata, hook, engine);
  }

  async mapInputData(): Promise<void> {
    this.logger.info('iterate-map-input-data');
  }
}

export { Iterate };
