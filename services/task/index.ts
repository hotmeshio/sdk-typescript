import {
  HMSH_EXPIRE_DURATION,
  HMSH_FIDELITY_SECONDS,
  HMSH_SCOUT_INTERVAL_SECONDS,
} from '../../modules/enums';
import { XSleepFor, sleepFor } from '../../modules/utils';
import { ILogger } from '../logger';
import { Pipe } from '../pipe';
import { StoreService } from '../store';
import { HookInterface, HookRule, HookSignal } from '../../types/hook';
import { KeyType } from '../../types/hotmesh';
import { JobCompletionOptions, JobState } from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { WorkListTaskType } from '../../types/task';
import { WEBSEP } from '../../modules/key';

class TaskService {
  store: StoreService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
  cleanupTimeout: NodeJS.Timeout | null = null;
  isScout = false;
  errorCount = 0;

  constructor(
    store: StoreService<ProviderClient, ProviderTransaction>,
    logger: ILogger,
  ) {
    this.logger = logger;
    this.store = store;
  }

  async processWebHooks(hookEventCallback: HookInterface): Promise<void> {
    const workItemKey = await this.store.getActiveTaskQueue();
    if (workItemKey) {
      const [topic, sourceKey, scrub, ...sdata] = workItemKey.split(WEBSEP);
      const data = JSON.parse(sdata.join(WEBSEP));
      const destinationKey = `${sourceKey}:processed`;
      const jobId = await this.store.processTaskQueue(
        sourceKey,
        destinationKey,
      );
      if (jobId) {
        //todo: don't use 'id', make configurable using hook rule
        await hookEventCallback(topic, { ...data, id: jobId });
      } else {
        await this.store.deleteProcessedTaskQueue(
          workItemKey,
          sourceKey,
          destinationKey,
          scrub === 'true',
        );
      }
      setImmediate(() => this.processWebHooks(hookEventCallback));
    }
  }

  async enqueueWorkItems(keys: string[]): Promise<void> {
    await this.store.addTaskQueues(keys);
  }

  async registerJobForCleanup(
    jobId: string,
    inSeconds = HMSH_EXPIRE_DURATION,
    options: JobCompletionOptions,
  ): Promise<void> {
    if (inSeconds > 0) {
      await this.store.expireJob(jobId, inSeconds);
      // const fromNow = Date.now() + inSeconds * 1000;
      // const fidelityMS = HMSH_FIDELITY_SECONDS * 1000;
      // const timeSlot = Math.floor(fromNow / fidelityMS) * fidelityMS;
      // await this.store.registerDependenciesForCleanup(jobId, timeSlot, options);
    }
  }

  async registerTimeHook(
    jobId: string,
    gId: string,
    activityId: string,
    type: WorkListTaskType,
    inSeconds = HMSH_FIDELITY_SECONDS,
    dad: string,
    transaction?: ProviderTransaction,
  ): Promise<void> {
    const fromNow = Date.now() + inSeconds * 1000;
    const fidelityMS = HMSH_FIDELITY_SECONDS * 1000;
    const awakenTimeSlot = Math.floor(fromNow / fidelityMS) * fidelityMS;
    await this.store.registerTimeHook(
      jobId,
      gId,
      activityId,
      type,
      awakenTimeSlot,
      dad,
      transaction,
    );
  }

  /**
   * Should this engine instance play the role of 'scout' on behalf
   * of the entire quorum? The scout role is responsible for processing
   * task lists on behalf of the collective.
   */
  async shouldScout() {
    const wasScout = this.isScout;
    const isScout =
      wasScout || (this.isScout = await this.store.reserveScoutRole('time'));
    if (isScout) {
      if (!wasScout) {
        setTimeout(() => {
          this.isScout = false;
        }, HMSH_SCOUT_INTERVAL_SECONDS * 1_000);
      }
      return true;
    }
    return false;
  }

  /**
   * Callback handler that takes an item from a work list and
   * processes according to its type
   */
  async processTimeHooks(
    timeEventCallback: (
      jobId: string,
      gId: string,
      activityId: string,
      type: WorkListTaskType,
    ) => Promise<void>,
    listKey?: string,
  ): Promise<void> {
    if (await this.shouldScout()) {
      try {
        const workListTask = await this.store.getNextTask(listKey);

        if (Array.isArray(workListTask)) {
          const [listKey, target, gId, activityId, type] = workListTask;
          if (type === 'child') {
            //continue; this child is listed here for convenience, but
            //  will be expired by an origin ancestor and is listed there
          } else if (type === 'delist') {
            //delist the signalKey (target)
            const key = this.store.mintKey(KeyType.SIGNALS, {
              appId: this.store.appId,
            });
            await this.store.delistSignalKey(key, target);
          } else {
            //awaken/expire/interrupt
            await timeEventCallback(target, gId, activityId, type);
          }
          await sleepFor(0);
          this.errorCount = 0;
          this.processTimeHooks(timeEventCallback, listKey);
        } else if (workListTask) {
          //a worklist was just emptied; try again immediately
          await sleepFor(0);
          this.errorCount = 0;
          this.processTimeHooks(timeEventCallback);
        } else {
          //no worklists exist; sleep before checking
          const sleep = XSleepFor(HMSH_FIDELITY_SECONDS * 1000);
          this.cleanupTimeout = sleep.timerId;
          await sleep.promise;
          this.errorCount = 0;
          this.processTimeHooks(timeEventCallback);
        }
      } catch (err) {
        //most common reasons: deleted job not found; container stopping; test stopping
        //less common: redis/cluster down; retry with fallback (5s max main reassignment)
        this.logger.warn('task-process-timehooks-error', err);
        await sleepFor(1_000 * this.errorCount++);
        if (this.errorCount < 5) {
          this.processTimeHooks(timeEventCallback);
        }
      }
    } else {
      //didn't get the scout role; try again in 'one-ish' minutes
      const sleep = XSleepFor(
        HMSH_SCOUT_INTERVAL_SECONDS * 1_000 * 2 * Math.random(),
      );
      this.cleanupTimeout = sleep.timerId;
      await sleep.promise;
      this.processTimeHooks(timeEventCallback);
    }
  }

  cancelCleanup() {
    if (this.cleanupTimeout !== undefined) {
      clearTimeout(this.cleanupTimeout);
      this.cleanupTimeout = undefined;
    }
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  async registerWebHook(
    topic: string,
    context: JobState,
    dad: string,
    expire: number,
    transaction?: ProviderTransaction,
  ): Promise<string> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      const mapExpression = hookRule.conditions.match[0].expected;
      const resolved = Pipe.resolve(mapExpression, context);
      const jobId = context.metadata.jid;
      const gId = context.metadata.gid;
      const activityId = hookRule.to;
      //composite keys are used to fully describe the task target
      const compositeJobKey = [activityId, dad, gId, jobId].join(WEBSEP);

      const hook: HookSignal = {
        topic,
        resolved,
        jobId: compositeJobKey,
        expire,
      };
      await this.store.setHookSignal(hook, transaction);
      return jobId;
    } else {
      throw new Error('signaler.registerWebHook:error: hook rule not found');
    }
  }

  async processWebHookSignal(
    topic: string,
    data: Record<string, unknown>,
  ): Promise<[string, string, string, string] | undefined> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //NOTE: both formats are supported by the mapping engine:
      //      `$self.hook.data` OR `$hook.data`
      const context = { $self: { hook: { data } }, $hook: { data } };
      const mapExpression = hookRule.conditions.match[0].actual;
      const resolved = Pipe.resolve(mapExpression, context);
      const hookSignalId = await this.store.getHookSignal(topic, resolved);
      if (!hookSignalId) {
        //messages can be double-processed; not an issue; return `undefined`
        //users can also provide a bogus topic; not an issue; return `undefined`
        return undefined;
      }
      //`aid` is part of composite key, but the hook `topic` is its public interface;
      // this means that a new version of the graph can be deployed and the
      // topic can be re-mapped to a different activity id. Outside callers
      // can adhere to the unchanged contract (calling the same topic),
      // while the internal system can be updated in real-time as necessary.
      const [_aid, dad, gid, ...jid] = hookSignalId.split(WEBSEP);
      return [jid.join(WEBSEP), hookRule.to, dad, gid];
    } else {
      throw new Error('signal-not-found');
    }
  }

  async deleteWebHookSignal(
    topic: string,
    data: Record<string, unknown>,
  ): Promise<number> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //NOTE: both formats are supported by the mapping engine:
      //      `$self.hook.data` OR `$hook.data`
      const context = { $self: { hook: { data } }, $hook: { data } };
      const mapExpression = hookRule.conditions.match[0].actual;
      const resolved = Pipe.resolve(mapExpression, context);
      return await this.store.deleteHookSignal(topic, resolved);
    } else {
      throw new Error('signaler.process:error: hook rule not found');
    }
  }

  /**
   * Enhanced processTimeHooks that uses notifications for PostgreSQL stores
   */
  async processTimeHooksWithNotifications(
    timeEventCallback: (
      jobId: string,
      gId: string,
      activityId: string,
      type: WorkListTaskType,
    ) => Promise<void>,
  ): Promise<void> {
    // Check if the store supports notifications
    if (this.isPostgresStore() && this.supportsNotifications()) {
      try {
        this.logger.info('task-using-notification-mode', {
          appId: this.store.appId,
          message:
            'Time scout using PostgreSQL LISTEN/NOTIFY mode for efficient task processing',
        });
        // Use the PostgreSQL store's notification-based approach
        await (this.store as any).startTimeScoutWithNotifications(
          timeEventCallback,
        );
      } catch (error) {
        this.logger.warn('task-notifications-fallback', {
          appId: this.store.appId,
          error: error.message,
          fallbackTo: 'polling',
          message:
            'Notification mode failed - falling back to traditional polling',
        });
        // Fall back to regular polling
        await this.processTimeHooks(timeEventCallback);
      }
    } else {
      this.logger.info('task-using-polling-mode', {
        appId: this.store.appId,
        storeType: this.store.constructor.name,
        message:
          'Time scout using traditional polling mode (notifications not available)',
      });
      // Use regular polling for non-PostgreSQL stores
      await this.processTimeHooks(timeEventCallback);
    }
  }

  /**
   * Check if this is a PostgreSQL store
   */
  private isPostgresStore(): boolean {
    return this.store.constructor.name === 'PostgresStoreService';
  }

  /**
   * Check if the store supports notifications
   */
  private supportsNotifications(): boolean {
    return (
      typeof (this.store as any).startTimeScoutWithNotifications === 'function'
    );
  }
}

export { TaskService };
