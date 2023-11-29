import { ILogger } from '../logger';
import { StoreService } from '../store';
import { HookRule, HookSignal } from '../../types/hook';
import { JobState } from '../../types/job';
import { RedisClient, RedisMulti } from '../../types/redis';
import { Pipe } from '../pipe';

class StoreSignaler {
  store: StoreService<RedisClient, RedisMulti>;
  logger: ILogger

  constructor(store: StoreService<RedisClient, RedisMulti>, logger: ILogger) {
    this.store = store;
    this.logger = logger;
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  async registerWebHook(topic: string, context: JobState, dad: string, multi?: RedisMulti): Promise<string> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      const mapExpression = hookRule.conditions.match[0].expected;
      const resolved = Pipe.resolve(mapExpression, context);
      const jobId = context.metadata.jid;
      const hook: HookSignal = {
        topic,
        resolved,
        //hookSignalId is composed of `<dad>::<jid>`
        jobId: `${dad}::${jobId}`,
      }
      await this.store.setHookSignal(hook, multi);
      return jobId;
    } else {
      throw new Error('signaler.registerWebHook:error: hook rule not found');
    }
  }

  async processWebHookSignal(topic: string, data: Record<string, unknown>): Promise<[string, string, string] | undefined> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //NOTE: both formats are supported: $self.hook.data OR $hook.data
      const context = { $self: { hook: { data }}, $hook: { data }};
      const mapExpression = hookRule.conditions.match[0].actual;
      const resolved = Pipe.resolve(mapExpression, context);
      const hookSignalId = await this.store.getHookSignal(topic, resolved);
      if (!hookSignalId) {
        //messages can be double-processed; not an issue; return undefined
        //users can also provide a bogus topic; not an issue; return undefined
        return undefined;
      }
      const [dad, jid] = hookSignalId.split('::');
      //return [jid, aid, dad]
      return [jid, hookRule.to, dad];
    } else {
      throw new Error('signal-not-found');
    }
  }

  async deleteWebHookSignal(topic: string, data: Record<string, unknown>): Promise<number> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //NOTE: both formats are supported: $self.hook.data OR $hook.data
      const context = { $self: { hook: { data }}, $hook: { data }};
      const mapExpression = hookRule.conditions.match[0].actual;
      const resolved = Pipe.resolve(mapExpression, context);
      return await this.store.deleteHookSignal(topic, resolved);
    } else {
      throw new Error('signaler.process:error: hook rule not found');
    }
  }
}

export { StoreSignaler };
