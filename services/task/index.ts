import {
  HMSH_EXPIRE_DURATION,
  HMSH_FIDELITY_SECONDS,
  HMSH_PENDING_SIGNAL_EXPIRE,
} from '../../modules/enums';
import { s } from '../../modules/utils';
import { ILogger } from '../logger';
import { Pipe } from '../pipe';
import { StoreService } from '../store';
import { HookInterface, HookRule, HookSignal } from '../../types/hook';
import { JobCompletionOptions, JobState } from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { WorkListTaskType } from '../../types/task';
import { WEBSEP } from '../../modules/key';

class TaskService {
  store: StoreService<ProviderClient, ProviderTransaction>;
  logger: ILogger;

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
    transaction?: ProviderTransaction,
  ): Promise<void> {
    if (inSeconds > 0) {
      await this.store.expireJob(jobId, inSeconds, transaction);
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
    const awakenTimeSlot = Math.ceil(fromNow / fidelityMS) * fidelityMS;
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
   * No-op: time hooks are now engine_streams messages with future visible_at.
   * The engine's normal dequeue handles them when they become visible.
   * Kept for interface compatibility with quorum/engine callers.
   */
  async processTimeHooks(
    _timeEventCallback: (
      jobId: string,
      gId: string,
      activityId: string,
      type: WorkListTaskType,
    ) => Promise<void>,
  ): Promise<void> {
    //nothing to poll — sleeps are engine_streams messages
  }

  cancelCleanup() {
    //no-op: time hooks no longer use polling
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
  ): Promise<{ jobId: string; pending?: string }> {
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
      //called standalone (no transaction) so the single CTE query can
      //atomically detect and return pending signal data on collision.
      //the redelivery routing lets the store republish a consumed
      //pending signal durably, in the same transaction that consumes it
      const result = await this.store.setHookSignal(hook, undefined, {
        aid: hookRule.to,
        topic,
      });
      if (result.pendingData) {
        this.logger.warn('task-signal-race-pending-consumed', {
          topic,
          resolved,
          jobId,
        });
        return { jobId, pending: result.pendingData };
      }
      if (!result.success) {
        //setnxex failed but no pending signal; likely a retry where
        //our own hook signal was already set. continue normally.
        this.logger.debug('task-signal-hook-already-set', {
          topic,
          resolved,
          jobId,
        });
      }
      return { jobId };
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

      //resolve $expire override from the signal data (e.g., '1h', '30d')
      const pendingExpire = typeof data.$expire === 'string'
        ? s(data.$expire)
        : HMSH_PENDING_SIGNAL_EXPIRE;

      //atomic: returns the hook signal, or stores a pending signal
      //in the same SQL statement if no hook is registered yet
      const hookSignalId = await this.store.getHookSignal(
        topic,
        resolved,
        JSON.stringify(data),
        pendingExpire,
      );
      if (!hookSignalId) {
        this.logger.warn('task-signal-race-pending-stored', {
          topic,
          resolved,
          expire: pendingExpire,
        });
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

}

export { TaskService };
