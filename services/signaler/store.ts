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

  async registerWebHook(topic: string, context: JobState, multi?: RedisMulti): Promise<string> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      const mapExpression = hookRule.conditions.match[0].expected;
      const resolved = Pipe.resolve(mapExpression, context);
      const jobId = context.metadata.jid;
      const hook: HookSignal = {
        topic,
        resolved,
        jobId,
      }
      await this.store.setHookSignal(hook, multi);
      return jobId;
    } else {
      throw new Error('signaler.registerWebHook:error: hook rule not found');
    }
  }

  async processWebHookSignal(topic: string, data: Record<string, unknown>): Promise<string> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //NOTE: both formats are supported: $self.hook.data OR $hook.data
      const context = { $self: { hook: { data }}, $hook: { data }};
      const mapExpression = hookRule.conditions.match[0].actual;
      const resolved = Pipe.resolve(mapExpression, context);
      const jobId = await this.store.getHookSignal(topic, resolved);
      return jobId;
    } else {
      throw new Error('signaler.process:error: hook rule not found');
    }
  }

  async deleteWebHookSignal(topic: string, data: Record<string, unknown>): Promise<number> {
    const hookRule = await this.getHookRule(topic);
    if (hookRule) {
      //todo: use the rule to generate `resolved`
      const resolved = (data as { id: string}).id;
      return await this.store.deleteHookSignal(topic, resolved);
    } else {
      throw new Error('signaler.process:error: hook rule not found');
    }
  }
}

export { StoreSignaler };
