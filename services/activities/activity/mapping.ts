import { deepCopy } from '../../../modules/utils';
import { MapperService } from '../../mapper';
import { Pipe } from '../../pipe';
import { ActivityType, ActivityMetadata } from '../../../types/activity';
import { JobState } from '../../../types/job';

interface MappingContext {
  config: ActivityType;
  context: JobState;
  metadata: ActivityMetadata;
}

export function mapInputData(instance: MappingContext): void {
  if (instance.config.input?.maps) {
    const mapper = new MapperService(
      deepCopy(instance.config.input.maps),
      instance.context,
    );
    instance.context.data = mapper.mapRules();
  }
}

export function mapOutputData(instance: MappingContext): void {
  if (instance.config.output?.maps) {
    const mapper = new MapperService(
      deepCopy(instance.config.output.maps),
      instance.context,
    );
    const actOutData = mapper.mapRules();
    const activityId = instance.metadata.aid;
    const data = { ...instance.context[activityId].output, ...actOutData };
    instance.context[activityId].output.data = data;
  }
}

export function mapJobData(instance: MappingContext): void {
  if (instance.config.job?.maps) {
    const mapper = new MapperService(
      deepCopy(instance.config.job.maps),
      instance.context,
    );
    const output = mapper.mapRules();
    if (output) {
      for (const key in output) {
        const f1 = key.indexOf('[');
        if (f1 > -1) {
          const amount = key.substring(f1 + 1).split(']')[0];
          if (!isNaN(Number(amount))) {
            const left = key.substring(0, f1);
            output[left] = output[key];
            delete output[key];
          } else if (amount === '-' || amount === '_') {
            const obj = output[key];
            if (obj && typeof obj === 'object') {
              Object.keys(obj).forEach((newKey) => {
                output[newKey] = obj[newKey];
              });
            }
            delete output[key];
          }
        }
      }
    }
    instance.context.data = output;
  }
}

export function mapStatusThreshold(instance: MappingContext): number {
  if (instance.config.statusThreshold !== undefined) {
    const threshold = Pipe.resolve(
      instance.config.statusThreshold,
      instance.context,
    );
    if (threshold !== undefined && !isNaN(Number(threshold))) {
      return threshold;
    }
  }
  return 0;
}
