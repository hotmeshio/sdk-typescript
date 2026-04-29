import { Pipe } from '../pipe';
import { JobState } from '../../types/job';
import { Pipe as PipeType } from '../../types/pipe';
import { TransitionMatch, TransitionRule } from '../../types/transition';
import { StreamCode } from '../../types';

type RuleType =
  | null
  | undefined
  | boolean
  | string
  | number
  | Date
  | Record<string, any>;

/**
 * Evaluates and transforms data-mapping rules against live job state.
 *
 * @remarks
 * MapperService is the bridge between a workflow's declarative mapping
 * rules (including `@pipe` expressions) and the runtime job data. It
 * recursively walks a rule tree, delegating leaf-level resolution to
 * the {@link Pipe} engine. Static helpers such as {@link evaluate}
 * also power transition-condition checks during workflow execution.
 *
 * @example
 * ```typescript
 * const mapper = new MapperService(
 *   { greeting: '{data.name}', score: { '@pipe': [['{data.x}', '{data.y}'], ['{@math.add}']] } },
 *   jobState,
 * );
 * const result = mapper.mapRules();
 * // => { greeting: 'Alice', score: 7 }
 * ```
 */
class MapperService {
  private rules: Record<string, unknown>;
  private data: JobState;

  /**
   * @param rules - The mapping rule tree to evaluate. May contain
   *   literal values, `{data.*}` references, or nested `@pipe` objects.
   * @param data  - The current {@link JobState} used to resolve references.
   */
  constructor(rules: Record<string, unknown>, data: JobState) {
    this.rules = rules;
    this.data = data;
  }

  /**
   * Recursively resolves every rule in the tree against the current job
   * state and returns a fully-evaluated copy.
   *
   * @returns A plain object mirroring the rule structure with all
   *   expressions replaced by their resolved values.
   */
  public mapRules(): Record<string, unknown> {
    return this.traverseRules(this.rules);
  }

  private traverseRules(rules: RuleType): Record<string, unknown> {
    if (typeof rules === 'object' && '@pipe' in rules) {
      return this.pipe(rules['@pipe'] as PipeType);
    }
    if (typeof rules === 'object' && rules !== null) {
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
   * resolves a pipe expression of the form: { @pipe: [["{data.foo.bar}", 2, false, "hello world"]] }
   * @param value
   * @returns
   */
  private pipe(value: PipeType): any {
    const pipe = new Pipe(value, this.data);
    return pipe.process();
  }

  /**
   * resolves a mapping expression in the form: "{data.foo.bar}" or 2 or false or "hello world"
   * @param value
   * @returns
   */
  private resolve(value: any): any {
    const pipe = new Pipe([[value]], this.data);
    return pipe.process();
  }

  /**
   * Evaluates a transition rule against the current job state and an
   * incoming Stream message code to decide whether the transition fires.
   *
   * @remarks
   * Supports both simple boolean rules (`true` / `false`) and compound
   * match rules with optional AND / OR gating. When the rule includes
   * a `match` array, each entry's `actual` expression is resolved via
   * {@link Pipe.resolve} and compared to the `expected` value.
   *
   * @param transitionRule - A boolean shorthand or a {@link TransitionRule}
   *   containing `code`, optional `gate`, and optional `match` conditions.
   * @param context - The current {@link JobState} used to resolve
   *   `actual` expressions inside match entries.
   * @param code - The {@link StreamCode} returned by the preceding
   *   activity (e.g. `200`).
   * @returns `true` if the transition should be taken, `false` otherwise.
   *
   * @example
   * ```typescript
   * const shouldTransition = MapperService.evaluate(
   *   { code: 200, match: [{ expected: true, actual: '{data.isReady}' }] },
   *   jobState,
   *   200,
   * );
   * ```
   */
  static evaluate(
    transitionRule: TransitionRule | boolean,
    context: JobState,
    code: StreamCode,
  ): boolean {
    if (typeof transitionRule === 'boolean') {
      return transitionRule;
    }
    if (
      (Array.isArray(transitionRule.code) &&
        transitionRule.code.includes(code || 200)) ||
      code.toString() === (transitionRule.code || 200).toString()
    ) {
      if (!transitionRule.match) {
        return true;
      }
      const orGate = transitionRule.gate === 'or';
      let allAreTrue = true;
      let someAreTrue = false;
      transitionRule.match.forEach(({ expected, actual }: TransitionMatch) => {
        if ((orGate && !someAreTrue) || (!orGate && allAreTrue)) {
          const result = Pipe.resolve(actual, context) === expected;
          if (orGate && result) {
            someAreTrue = true;
          } else if (!orGate && !result) {
            allAreTrue = false;
          }
        }
      });
      return orGate ? someAreTrue : allAreTrue;
    }
    return false;
  }
}

export { MapperService };
