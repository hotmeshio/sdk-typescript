import { JobState, JobData, JobsData } from '../../types/job';
import {
  PipeContext,
  PipeItem,
  PipeItems,
  PipeObject,
  Pipe as PipeType,
  ReduceObject,
} from '../../types/pipe';

import FUNCTIONS from './functions';

/**
 * A functional data-transformation pipeline that resolves expressions
 * row-by-row against live job data.
 *
 * @remarks
 * ## Overview
 *
 * Pipe is the engine behind HotMesh's `@pipe` syntax — a uniform,
 * functional approach to data mapping and transformation. Every
 * ECMAScript operation is expressed as a **function with inputs**,
 * eliminating the syntactic variability of the language (ternaries,
 * property access, instance methods) in favor of a single,
 * composable pattern.
 *
 * ## Postfix notation (Reverse Polish Notation)
 *
 * If you've used an RPN calculator, `@pipe` will feel familiar.
 * Operands come first, then the operator consumes them:
 *
 * ```
 * RPN:   4  4  +   →  8
 *
 * @pipe:
 *   - [4, 4]           ← operands
 *   - ["{@math.add}"]  ← operator  →  8
 * ```
 *
 * That's the entire model. Operands are pushed, an operator pops
 * them, and the result becomes an operand for the next row.
 *
 * ## How it works — row by row
 *
 * A pipe is an ordered array of **rows**. Each row is an array of
 * **cells**. The key rule: **every resolved value on a row flows
 * into cell 0 of the next row** (the operator). The operator
 * consumes all upstream operands, resolves to a new value, and
 * that result *becomes an operand*. Any remaining cells on the
 * same row are resolved independently and appended as additional
 * operands. Then the cycle repeats.
 *
 * ```
 * @pipe:
 *   ┌──────────────────────────────────────┐
 *   │ Row 0: [operand, operand, operand]   │  ← resolve all cells
 *   │            │        │        │       │
 *   │            └────────┼────────┘       │
 *   │                     │                │
 *   ├─────────────────────┼────────────────┤
 *   │                     ▼                │
 *   │ Row 1: [  OPERATOR  , operand]       │  ← ALL operands from Row 0
 *   │         ▲ receives all ▲             │     flow into cell 0;
 *   │         │ from above   │             │     result becomes operand;
 *   │         └──────────────┘             │     remaining cells resolve
 *   │              │            │          │
 *   │              └────────────┘          │
 *   │                     │                │
 *   ├─────────────────────┼────────────────┤
 *   │                     ▼                │
 *   │ Row 2: [  OPERATOR  , operand]       │  ← same pattern repeats
 *   └──────────────────────────────────────┘
 *       ▼
 *   final value = cell 0 of last row
 * ```
 *
 * Step by step:
 *
 * 1. **Row 0** — Every cell is resolved independently (static
 *    literals, `{data.*}` references, or nullary functions like
 *    `{@date.now}`). The resolved values are all **operands**.
 * 2. **Row 1** — Cell 0 is the **operator** (`{@domain.method}`).
 *    It receives *all* operands from Row 0 as its arguments and
 *    produces a result. That result replaces cell 0 — it is now
 *    an operand. Any remaining cells (cell 1, 2, ...) on this row
 *    are resolved independently and become additional operands.
 * 3. **Row 2** — Cell 0 is the next operator. It receives *all*
 *    operands from Row 1 (the prior result + any extra cells).
 *    The cycle repeats.
 * 4. **Return** — The first cell of the final row is the result.
 *
 * When an additional cell on an operator row needs computed (not
 * just a static value or simple reference), use a **nested
 * `@pipe`** (sub-pipe) to resolve it. See Example 4 below.
 *
 * ## Three types of cell values
 *
 * | Type | Syntax | Example |
 * |------|--------|---------|
 * | **Static** | literal | `42`, `"hello"`, `true` |
 * | **Dynamic** | `{path}` | `{a.output.data.name}` |
 * | **Function** | `{@domain.method}` | `{@string.concat}`, `{@math.add}` |
 *
 * ## Available function domains
 *
 * - **array** — `get`, `length`, `join`, `concat`, `push`, `indexOf`, ...
 * - **bitwise** — `and`, `or`, `xor`, `not`, ...
 * - **conditional** — `ternary`, `equality`, `nullish`, ...
 * - **cron** — `nextDelay`
 * - **date** — `now`, `toLocaleString`, `yyyymmdd`, ...
 * - **json** — `parse`, `stringify`
 * - **logical** — `and`, `or`, `not`
 * - **math** — `add`, `multiply`, `pow`, `max`, `min`, `abs`, ...
 * - **number** — `isEven`, `isOdd`, `gte`, `lte`, ...
 * - **object** — `create`, `get`, `set`, `keys`, ...
 * - **string** — `concat`, `split`, `charAt`, `toLowerCase`, `includes`, ...
 * - **symbol**
 * - **unary**
 *
 * ---
 *
 * ## Example 1 — Simple field mapping (YAML)
 *
 * Most fields need only a one-to-one reference. Curly braces pull
 * values from upstream activity outputs:
 *
 * ```yaml
 * # Map fields from activities a and b into a new shape
 * maps:
 *   first: "{a.output.data.first_name}"
 *   last:  "{a.output.data.last_name}"
 *   email: "{b.output.data.email}"
 *   age:   "{b.output.data.age}"
 *   company: "ACME Corp"        # static string
 *   bonus:   500                 # static number
 * ```
 *
 * The equivalent JavaScript object passed to the mapper:
 *
 * ```typescript
 * const rules = {
 *   first: '{a.output.data.first_name}',
 *   last:  '{a.output.data.last_name}',
 *   email: '{b.output.data.email}',
 *   age:   '{b.output.data.age}',
 *   company: 'ACME Corp',
 *   bonus:   500,
 * };
 * ```
 *
 * ## Example 2 — String transformation (YAML → JS)
 *
 * Build a `user_name` like `jdoe` from "John Doe". Follow the
 * RPN flow — operands feed into the operator on the next row,
 * the result becomes an operand, extra cells append more operands:
 *
 * ```yaml
 * user_name:
 *   "@pipe":
 *     - ["{a.output.data.first_name}", 0]
 *     - ["{@string.charAt}", "{a.output.data.last_name}"]
 *     - ["{@string.concat}"]
 *     - ["{@string.toLowerCase}"]
 * ```
 *
 * ```typescript
 * // Identical logic in JavaScript
 * const rules = {
 *   user_name: {
 *     '@pipe': [
 *       ['{a.output.data.first_name}', 0],
 *       ['{@string.charAt}', '{a.output.data.last_name}'],
 *       ['{@string.concat}'],
 *       ['{@string.toLowerCase}'],
 *     ],
 *   },
 * };
 * ```
 *
 * RPN trace (given first_name="John", last_name="Doe"):
 *
 * ```
 * Row 0: ["John", 0]            ← two operands
 * Row 1: charAt("John", 0)="J"  ← operator consumes both → result "J"
 *         then resolve "Doe"    ← extra cell → operands are ["J", "Doe"]
 * Row 2: concat("J", "Doe")     ← operator → result "JDoe"
 *                                  operands are ["JDoe"]
 * Row 3: toLowerCase("JDoe")    ← operator → result "jdoe"
 * ```
 *
 * ## Example 3 — Conditional logic (YAML → JS)
 *
 * Classify an employee as "Senior" or "Junior" based on age:
 *
 * ```yaml
 * status:
 *   "@pipe":
 *     - ["{b.output.data.age}", 40]
 *     - ["{@number.gte}", "Senior", "Junior"]
 *     - ["{@conditional.ternary}"]
 * ```
 *
 * ```typescript
 * const rules = {
 *   status: {
 *     '@pipe': [
 *       ['{b.output.data.age}', 40],
 *       ['{@number.gte}', 'Senior', 'Junior'],
 *       ['{@conditional.ternary}'],
 *     ],
 *   },
 * };
 * ```
 *
 * RPN trace (given age=30):
 *
 * ```
 * Row 0: [30, 40]                          ← two operands
 * Row 1: gte(30, 40)=false                 ← operator consumes both → false
 *         resolve "Senior", "Junior"       ← extra cells → [false, "Senior", "Junior"]
 * Row 2: ternary(false, "Senior", "Junior") ← operator → "Junior"
 * ```
 *
 * ## Example 4 — Nested pipes (fan-out / fan-in)
 *
 * Extract initials from a full name. Each nested `@pipe` runs
 * independently (fan-out), then the first standard row after them
 * receives all sub-pipe results as inputs (fan-in):
 *
 * ```yaml
 * initials:
 *   "@pipe":
 *     - ["{a.output.data.full_name}", " "]
 *     - "@pipe":                           # fan-out: first initial
 *       - ["{@string.split}", 0]
 *       - ["{@array.get}", 0]
 *       - ["{@string.charAt}"]
 *     - "@pipe":                           # fan-out: last initial
 *       - ["{@string.split}", 1]
 *       - ["{@array.get}", 0]
 *       - ["{@string.charAt}"]
 *     - ["{@string.concat}"]              # fan-in: combine both
 * ```
 *
 * ```typescript
 * const rules = {
 *   initials: {
 *     '@pipe': [
 *       ['{a.output.data.full_name}', ' '],
 *       { '@pipe': [['{@string.split}', 0], ['{@array.get}', 0], ['{@string.charAt}']] },
 *       { '@pipe': [['{@string.split}', 1], ['{@array.get}', 0], ['{@string.charAt}']] },
 *       ['{@string.concat}'],
 *     ],
 *   },
 * };
 * // "Luke Birdeau" → split → ["Luke","Birdeau"]
 * //   sub-pipe 1: get(0) → "Luke" → charAt(0) → "L"
 * //   sub-pipe 2: get(1) → "Birdeau" → charAt(0) → "B"
 * // fan-in: concat("L", "B") → "LB"
 * ```
 *
 * ## Example 5 — Reduce (iterate and accumulate)
 *
 * Transform an array of `{ full_name }` objects into
 * `{ first, last }` pairs using `@reduce`. Context variables
 * `{$item}`, `{$key}`, `{$index}`, `{$input}`, and `{$output}`
 * are available inside the reducer body:
 *
 * ```typescript
 * const jobData = {
 *   a: { output: { data: [
 *     { full_name: 'Luke Birdeau' },
 *     { full_name: 'John Doe' },
 *   ]}}
 * };
 *
 * const rules = [
 *   ['{a.output.data}', []],         // input array + initial accumulator
 *   { '@reduce': [
 *     { '@pipe': [['{$output}']] },  // carry forward accumulator
 *     { '@pipe': [
 *       { '@pipe': [['first']] },
 *       { '@pipe': [
 *         ['{$item.full_name}', ' '],
 *         ['{@string.split}', 0],
 *         ['{@array.get}'],
 *       ]},
 *       { '@pipe': [['last']] },
 *       { '@pipe': [
 *         ['{$item.full_name}', ' '],
 *         ['{@string.split}', 1],
 *         ['{@array.get}'],
 *       ]},
 *       ['{@object.create}'],
 *     ]},
 *     ['{@array.push}'],
 *   ]},
 * ];
 * // => [{ first: 'Luke', last: 'Birdeau' }, { first: 'John', last: 'Doe' }]
 * ```
 *
 * ## Example 6 — Transition conditions
 *
 * Pipes power conditional transitions between workflow activities.
 * In YAML, a transition fires only when the `@pipe` resolves to the
 * `expected` value:
 *
 * ```yaml
 * transitions:
 *   t1:
 *     - to: a1
 *       conditions:
 *         match:
 *           - expected: false
 *             actual:
 *               "@pipe":
 *                 - ["{t1.output.data.a}", "goodbye"]
 *                 - ["{@conditional.equality}"]
 * ```
 *
 * ## Example 7 — Inline YAML with HotMesh.deploy
 *
 * HotMesh ships with an inline YAML parser, so a complete
 * workflow with data mapping can be deployed in a single call:
 *
 * ```typescript
 * await hotMesh.deploy(`
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: order.process
 *       activities:
 *         t1:
 *           type: trigger
 *         a1:
 *           type: worker
 *           topic: inventory.check
 *           input:
 *             maps:
 *               itemId: "{t1.output.data.itemId}"
 *           output:
 *             schema:
 *               type: object
 *           job:
 *             maps:
 *               result: "{$self.output.data.available}"
 *       transitions:
 *         t1:
 *           - to: a1
 * `);
 *
 * await hotMesh.activate('1');
 * const response = await hotMesh.pubsub('order.process', { itemId: 'sku-42' });
 * ```
 */
class Pipe {
  rules: PipeType;
  jobData: JobData;
  context: PipeContext;

  /**
   * @param rules   - The ordered row array defining the pipeline.
   * @param jobData - The current job data used to resolve `{data.*}`
   *   and `{activity.*}` references.
   * @param context - Optional iteration context (`$item`, `$key`,
   *   `$output`, etc.) supplied during `@reduce` execution.
   */
  constructor(rules: PipeType, jobData: JobData, context?: PipeContext) {
    this.rules = rules;
    this.jobData = jobData;
    this.context = context;
  }

  private isPipeType(
    currentRow: PipeItem[] | PipeType | PipeObject | ReduceObject,
  ): currentRow is PipeType {
    return !Array.isArray(currentRow) && '@pipe' in currentRow;
  }

  private isreduceType(
    currentRow: PipeItem[] | PipeType | PipeObject | ReduceObject,
  ): currentRow is PipeType {
    return !Array.isArray(currentRow) && '@reduce' in currentRow;
  }

  /**
   * Returns `true` if the value is a `@pipe` object (i.e. `{ '@pipe': [...] }`).
   *
   * @param obj - The value to test.
   */
  static isPipeObject(obj: { [key: string]: unknown } | PipeItem): boolean {
    return (
      typeof obj === 'object' &&
      obj !== null &&
      !Array.isArray(obj) &&
      '@pipe' in obj
    );
  }

  /**
   * One-shot convenience method that resolves a single value or
   * `@pipe` expression against the given context.
   *
   * @param unresolved - A literal value, a `{data.*}` reference string,
   *   or a `@pipe` object.
   * @param context - Partial {@link JobState} used for resolution.
   * @returns The fully resolved value.
   *
   * @example
   * ```typescript
   * Pipe.resolve('{data.user.email}', jobState);
   * Pipe.resolve({ '@pipe': [['{data.a}', '{data.b}'], ['{@math.multiply}']] }, jobState);
   * ```
   */
  static resolve(
    unresolved: { [key: string]: unknown } | PipeItem,
    context: Partial<JobState>,
  ): any {
    let pipe: Pipe;
    if (Pipe.isPipeObject(unresolved)) {
      pipe = new Pipe(unresolved['@pipe'], context);
    } else {
      pipe = new Pipe([[unresolved as unknown as PipeItem]], context);
    }
    return pipe.process();
  }

  /**
   * Executes the pipeline row-by-row, resolving and transforming
   * until the final value is produced.
   *
   * @remarks
   * Row 0 is resolved independently (values only). Each subsequent
   * row feeds the prior row's resolved output as arguments to the
   * function named in cell 0. Nested `@pipe` rows are queued
   * (fan-out) and collected into the next standard row (fan-in).
   * `@reduce` rows iterate over the prior row's array output.
   *
   * @param resolved - Optional pre-resolved seed values (used
   *   internally by `reduce` to inject the accumulator).
   * @returns The first cell of the final resolved row.
   *
   * @example
   * ```typescript
   * // Split a full name and grab the first element
   * const pipe = new Pipe(
   *   [['{a.output.data.full_name}', ' '], ['{@string.split}', 0], ['{@array.get}']],
   *   { a: { output: { data: { full_name: 'Luke Birdeau' } } } },
   * );
   * pipe.process(); // => 'Luke'
   * ```
   */
  process(resolved: unknown[] | null = null): any {
    let index = 0;
    if (
      !(
        resolved ||
        this.isPipeType(this.rules[0]) ||
        this.isreduceType(this.rules[0])
      )
    ) {
      resolved = this.processCells(this.rules[0] as PipeItem[]); // Add type assertion
      index = 1;
    }
    const len = this.rules.length;
    const subPipeQueue = [];
    for (let i = index; i < len; i++) {
      resolved = this.processRow(this.rules[i], resolved, subPipeQueue);
    }
    return resolved[0];
  }

  cloneUnknown<T>(value: T): T {
    if (value === null || typeof value !== 'object') {
      return value;
    }
    if (value instanceof Date) {
      return new Date(value.getTime()) as T;
    }
    if (value instanceof RegExp) {
      return new RegExp(value) as T;
    }
    if (Array.isArray(value)) {
      return value.map((item) => this.cloneUnknown(item)) as unknown as T;
    }
    const clonedObj = {} as T;
    for (const key in value) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        (clonedObj as any)[key] = this.cloneUnknown((value as any)[key]);
      }
    }
    return clonedObj;
  }

  /**
   * Iterates over an array or object and accumulates a result,
   * similar to `Array.prototype.reduce`.
   *
   * @remarks
   * Inside the reducer body the following context variables are
   * available as cell references:
   *
   * | Variable | Description |
   * |----------|-------------|
   * | `{$input}` | The full input collection |
   * | `{$output}` | The current accumulator |
   * | `{$item}` | The current element |
   * | `{$key}` | The current key (string for objects, index string for arrays) |
   * | `{$index}` | The current numeric index |
   *
   * The preceding row supplies `[inputCollection, initialAccumulator]`.
   * If no initial accumulator is provided, it defaults to `null`.
   *
   * @param input - A two-element array: `[collection, initialAccumulator]`.
   * @returns The final accumulated value, wrapped in an array.
   * @private
   */
  reduce(input: Array<unknown[]>): unknown {
    let resolved = this.cloneUnknown<any>(input[1] ?? null);

    if (Array.isArray(input[0])) {
      for (let index = 0; index < input[0].length; index++) {
        this.context = {
          $input: input[0],
          $output: resolved,
          $item: input[0][index],
          $key: index.toString(),
          $index: index,
        };
        resolved = this.process([resolved]);
      }
    } else {
      let index = -1;
      for (const $key in input[0] as Record<string, unknown>) {
        index++;
        this.context = {
          $input: input[0],
          $output: resolved,
          $item: input[0][$key],
          $key,
          $index: index,
        };
        resolved = this.process([resolved]);
      }
    }
    return [resolved];
  }

  private processRow(
    currentRow: PipeItem[] | PipeType | PipeObject | ReduceObject,
    resolvedPriorRow: unknown[] | null,
    subPipeQueue: unknown[],
  ): PipeItem[] {
    if (resolvedPriorRow && this.isreduceType(currentRow)) {
      //reduce the resolvedPriorRow and return the output from the reducer
      const subPipe = new Pipe(currentRow['@reduce'], this.jobData);
      const reduced = subPipe.reduce(resolvedPriorRow as Array<unknown[]>);
      return reduced as PipeItem[];
    } else if (this.isPipeType(currentRow)) {
      //process subPipe and push to subPipeQueue; echo resolvedPriorRow
      const subPipe = new Pipe(currentRow['@pipe'], this.jobData, this.context);
      subPipeQueue.push(subPipe.process());
      return resolvedPriorRow as PipeItem[];
    } else {
      //pivot the subPipeQueue into the arguments array (resolvedPriorRow)
      if (subPipeQueue.length > 0) {
        resolvedPriorRow = [...subPipeQueue];
        subPipeQueue.length = 0;
      }

      if (!resolvedPriorRow) {
        //if no prior row, use current row as prior row
        return [].concat(
          this.processCells(
            Array.isArray(currentRow) ? ([...currentRow] as PipeItem[]) : [],
          ),
        );
      } else {
        const [functionName, ...params] = currentRow as PipeItem[]; // Add type assertion
        //use resolved values from prior row (n - 1) as input params to cell 1 function
        let resolvedValue: unknown;
        if (this.isContextVariable(functionName)) {
          resolvedValue = this.resolveContextValue(functionName as string);
        } else {
          resolvedValue = Pipe.resolveFunction(functionName as string)(
            ...resolvedPriorRow,
          );
        }
        //resolve remaining cells in row and return concatenated with resolvedValue
        return [resolvedValue as PipeItem].concat(
          this.processCells([...params]),
        );
      }
    }
  }

  /**
   * Looks up a domain function by its `{@domain.method}` name string
   * and returns the callable.
   *
   * @param functionName - A string like `{@math.add}` or `{@string.concat}`.
   * @returns The resolved function reference.
   * @throws If the domain or method is not registered.
   */
  static resolveFunction(functionName: string) {
    let [prefix, suffix] = functionName.split('.');
    prefix = prefix.substring(2);
    suffix = suffix.substring(0, suffix.length - 1);
    const domain = FUNCTIONS[prefix];
    if (!domain) {
      throw new Error(`Unknown domain name [${functionName}]: ${prefix}`);
    }
    if (!domain[suffix]) {
      throw new Error(
        `Unknown domain function [${functionName}]: ${prefix}.${suffix}`,
      );
    }
    return domain[suffix];
  }

  /**
   * Resolves every cell in a single row independently — each cell
   * is evaluated for function calls, context variables, or mappable
   * references.
   *
   * @param cells - The array of {@link PipeItems} to resolve.
   * @returns An array of resolved values, one per input cell.
   */
  processCells(cells: PipeItems): unknown[] {
    const resolved = [];
    if (Array.isArray(cells)) {
      for (const currentCell of cells) {
        resolved.push(this.resolveCellValue(currentCell));
      }
    }
    return resolved;
  }

  private isFunction(currentCell: PipeItem): boolean {
    return (
      typeof currentCell === 'string' &&
      currentCell.startsWith('{@') &&
      currentCell.endsWith('}')
    );
  }

  private isContextVariable(currentCell: PipeItem): boolean {
    if (typeof currentCell === 'string' && currentCell.endsWith('}')) {
      return (
        currentCell.startsWith('{$item') ||
        currentCell.startsWith('{$key') ||
        currentCell.startsWith('{$index') ||
        currentCell.startsWith('{$input') ||
        currentCell.startsWith('{$output')
      );
    }
  }

  private isMappable(currentCell: PipeItem): boolean {
    return (
      typeof currentCell === 'string' &&
      currentCell.startsWith('{') &&
      currentCell.endsWith('}')
    );
  }

  /**
   * Resolves a single cell value by detecting its type — function
   * call (`{@domain.fn}`), context variable (`{$item}`, `{$key}`,
   * etc.), mappable reference (`{data.*}`), or literal.
   *
   * @param currentCell - The cell to resolve.
   * @returns The resolved runtime value.
   */
  resolveCellValue(currentCell: PipeItem): unknown {
    if (this.isFunction(currentCell)) {
      const fn = Pipe.resolveFunction(currentCell as string);
      return fn.call();
    } else if (this.isContextVariable(currentCell)) {
      return this.resolveContextValue(currentCell as string);
    } else if (this.isMappable(currentCell)) {
      return this.resolveMappableValue(currentCell as string);
    } else {
      return currentCell;
    }
  }

  private getNestedProperty(obj: JobsData | unknown, path: string): any {
    const pathParts = path.split('.');
    let current = obj;
    for (const part of pathParts) {
      if (
        current === null ||
        typeof current !== 'object' ||
        !current.hasOwnProperty(part)
      ) {
        return undefined;
      }
      current = current[part];
    }

    return current;
  }

  /**
   * Resolves a `{data.*}` or `{activity.*}` reference by walking
   * the dot-delimited path into {@link jobData}.
   *
   * @param currentCell - The mappable reference string (e.g. `{data.user.name}`).
   */
  resolveMappableValue(currentCell: string): unknown {
    const term = this.resolveMapTerm(currentCell);
    return this.getNestedProperty(this.jobData, term);
  }

  resolveContextValue(currentCell: string): unknown {
    const term = this.resolveContextTerm(currentCell);
    return this.getNestedProperty(this.context, term);
  }

  resolveContextTerm(currentCell: string): string {
    return currentCell.substring(1, currentCell.length - 1);
  }

  resolveFunctionTerm(currentCell: string): string {
    return currentCell.substring(2, currentCell.length - 1);
  }

  resolveMapTerm(currentCell: string): string {
    return currentCell.substring(1, currentCell.length - 1);
  }
}

export { Pipe };
