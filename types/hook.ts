
interface HookCondition {
  expected: string;
  actual: string;
}

enum HookGate {
  AND = 'and',
  OR = 'or',
}

interface HookConditions {
  gate?: HookGate;
  match: HookCondition[];
}

interface HookRule {
  to: string;
  conditions: HookConditions;
}

interface HookRules {
  [eventName: string]: HookRule[];
}

type HookSignal = { topic: string, resolved: string, jobId: string};

interface HookInterface {
  (topic: string, data: { [key: string]: any, id: string }): Promise<void>;
}

export { HookCondition, HookConditions, HookGate, HookInterface, HookRule, HookRules, HookSignal };
