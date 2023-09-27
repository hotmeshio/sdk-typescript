import { matchesStatus, matchesStatusCode } from "../../modules/utils";
import { Pipe } from "../pipe";
import { JobState } from "../../types/job";
import { Pipe as PipeType } from "../../types/pipe";
import { TransitionMatch, TransitionRule } from "../../types/transition";
import { StreamCode, StreamStatus } from "../../types";

type RuleType = null | undefined | boolean | string | number | Date | Record<string, any>;

class MapperService {
  private rules: Record<string, unknown>;
  private data: JobState;

  constructor(rules: Record<string, unknown>, data: JobState) {
    this.rules = rules;
    this.data = data;
  }

  public mapRules(): Record<string, unknown> {
    return this.traverseRules(this.rules);
  }

  private traverseRules(rules: RuleType): Record<string, unknown> {
    if (typeof rules === 'object' && '@pipe' in rules) {
      return this.pipe(rules['@pipe'] as PipeType);
    } if (typeof rules === 'object' && rules !== null) {
      const mappedRules: Record<string, any> = {};
      for (const key in rules) {
        if (Object.prototype.hasOwnProperty.call(rules, key)) {
          mappedRules[key] = this.traverseRules(rules[key]);
        }
      }
      return mappedRules;
    } else {
      return this.resolve(rules);
    }
  }

  /**
   * resolve a pipe expression of the form: { @pipe: [["{data.foo.bar}", 2, false, "hello world"]] }
   * @param value 
   * @returns 
   */
  private pipe(value: PipeType): any {
    const pipe = new Pipe(value, this.data);
    return pipe.process();
  }

  /**
   * resolve a simple mapping expression in the form: "{data.foo.bar}" or 2 or false or "hello world"
   * @param value 
   * @returns 
   */
  private resolve(value: any): any {
    const pipe = new Pipe([[value]], this.data);
    return pipe.process();
  }

  static evaluate(transitionRule: TransitionRule | boolean, context: JobState, code: StreamCode): boolean {
    if (typeof transitionRule === 'boolean') {
      return transitionRule;
    }
    if (code.toString() === (transitionRule.code || 200).toString()) {
      const orGate = transitionRule.gate === 'or';
      let allAreTrue = true;
      let someAreTrue = false;
      transitionRule.match.forEach(({ expected, actual }: TransitionMatch) => {
        if ((orGate && !someAreTrue) || (!orGate && allAreTrue)) {
          const result = Pipe.resolve(actual, context) === expected;
          if (orGate && result) {
            someAreTrue = true;
          } else if(!orGate && !result) {
            allAreTrue = false;
          }
        }
      });
      return orGate ? someAreTrue : allAreTrue;
    }
    return false;
  }
}


export { MapperService }