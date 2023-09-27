import { EngineService } from '../engine';
import { Activity, ActivityType } from './activity';
import {
  ActivityData,
  ActivityMetadata,
  EmitActivity } from '../../types/activity';

class Emit extends Activity {
  config: EmitActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService) {
    super(config, data, metadata, hook, engine);
  }

  async mapInputData(): Promise<void> {
    this.logger.info('emit-map-input-data');
  }
}

export { Emit };
