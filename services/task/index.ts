import { ILogger } from '../logger';
import { StoreService } from '../store';
import { RedisClient, RedisMulti } from '../../types/redis';
import { HookInterface } from '../../types/hook';
import { XSleepFor, sleepFor } from '../../modules/utils';

//system timer granularity limit (task queues organize)
const FIDELITY_SECONDS = 15; //note: this can be reduced using 'watch' or scout role
//default resolution/fidelity when expiring
const EXPIRATION_FIDELITY_SECONDS = 60;

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

  async registerJobForCleanup(jobId: string, inSeconds = EXPIRATION_FIDELITY_SECONDS): Promise<void> {
    if (inSeconds > -1) {
      await this.store.expireJob(jobId, inSeconds);
    }
  }

  async registerTimeHook(jobId: string, activityId: string, type: 'sleep'|'expire'|'cron', inSeconds = FIDELITY_SECONDS, multi?: RedisMulti): Promise<void> {
    const awakenTimeSlot = Math.floor((Date.now() + inSeconds * 1000) / (FIDELITY_SECONDS * 1000)) * (FIDELITY_SECONDS * 1000); //n second awaken groups
    await this.store.registerTimeHook(jobId, activityId, type, awakenTimeSlot, multi);
  }

  //todo: need 'scout' role in quorum to check for this and then alert the quorum to get to work
  async processTimeHooks(timeEventCallback: (jobId: string, activityId: string) => Promise<void>, listKey?: string): Promise<void> {
    try {
      const job = await this.store.getNextTimeJob(listKey);
      if (job) {
        const [listKey, jobId, activityId] = job;
        await timeEventCallback(jobId, activityId);
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
