import {
  EXPIRE_DURATION,
  FIDELITY_SECONDS } from '../../modules/enums';
import { XSleepFor, sleepFor } from '../../modules/utils';
import { ILogger } from '../logger';
import { StoreService } from '../store';
import { HookInterface } from '../../types/hook';
import { JobCompletionOptions } from '../../types/job';
import { RedisClient, RedisMulti } from '../../types/redis';

class TaskService {
  store: StoreService<RedisClient, RedisMulti>;
  logger: ILogger;
  cleanupTimeout: NodeJS.Timeout | null = null;

  constructor(
    store: StoreService<RedisClient, RedisMulti>,
    logger: ILogger
  ) {
    this.logger = logger;
    this.store = store;
  }

  async processWebHooks(hookEventCallback: HookInterface): Promise<void> {
    const workItemKey = await this.store.getActiveTaskQueue();
    if (workItemKey) {
      const [topic, sourceKey, scrub, ...sdata] = workItemKey.split('::');
      const data = JSON.parse(sdata.join('::'));
      const destinationKey = `${sourceKey}:processed`;
      const jobId = await this.store.processTaskQueue(sourceKey, destinationKey);
      if (jobId) {
        //todo: don't use 'id', make configurable using hook rule
        await hookEventCallback(topic, { ...data, id: jobId });
      } else {
        await this.store.deleteProcessedTaskQueue(workItemKey, sourceKey, destinationKey, scrub === 'true');
      }
      setImmediate(() => this.processWebHooks(hookEventCallback));
    }
  }

  async enqueueWorkItems(keys: string[]): Promise<void> {
    await this.store.addTaskQueues(keys);
  }

  async registerJobForCleanup(jobId: string, inSeconds = EXPIRE_DURATION, options: JobCompletionOptions): Promise<void> {
    if (inSeconds > 0) {
      await this.store.expireJob(jobId, inSeconds);
      const expireTimeSlot = Math.floor((Date.now() + (inSeconds * 1000)) / (FIDELITY_SECONDS * 1000)) * (FIDELITY_SECONDS * 1000); //n second awaken groups
      await this.store.registerExpireJob(jobId, expireTimeSlot, options);
    }
  }

  async registerTimeHook(jobId: string, activityId: string, type: 'sleep'|'expire'|'interrupt', inSeconds = FIDELITY_SECONDS, multi?: RedisMulti): Promise<void> {
    const awakenTimeSlot = Math.floor((Date.now() + (inSeconds * 1000)) / (FIDELITY_SECONDS * 1000)) * (FIDELITY_SECONDS * 1000); //n second awaken groups
    await this.store.registerTimeHook(jobId, activityId, type, awakenTimeSlot, multi);
  }

  async processTimeHooks(timeEventCallback: (jobId: string, activityId: string, type: 'sleep'|'expire'|'interrupt') => Promise<void>, listKey?: string): Promise<void> {
    try {
      const timeJob = await this.store.getNextTimeJob(listKey);
      if (timeJob) {
        const [listKey, jobId, activityId, type] = timeJob;
        await timeEventCallback(jobId, activityId, type);
        await sleepFor(0);          
        this.processTimeHooks(timeEventCallback, listKey);
      } else {
        let sleep = XSleepFor(FIDELITY_SECONDS * 1000);
        this.cleanupTimeout = sleep.timerId;
        await sleep.promise;
        this.processTimeHooks(timeEventCallback)
      }
    } catch (err) {
      //todo: retry connect to redis
      this.logger.error('task-process-timehooks-error', err);
    }
  }

  cancelCleanup() {
    if (this.cleanupTimeout !== undefined) {
      clearTimeout(this.cleanupTimeout);
      this.cleanupTimeout = undefined;
    }
  }
}

export { TaskService };
