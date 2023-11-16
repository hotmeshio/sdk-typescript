
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
  keep_alive?: boolean; //if true, the hook will not be deleted after use
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
