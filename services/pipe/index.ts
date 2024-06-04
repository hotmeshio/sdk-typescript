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

class Pipe {
  rules: PipeType;
  jobData: JobData;
  context: PipeContext;

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

  static isPipeObject(obj: { [key: string]: unknown } | PipeItem): boolean {
    return (
      typeof obj === 'object' &&
      obj !== null &&
      !Array.isArray(obj) &&
      '@pipe' in obj
    );
  }

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
   * loop through each PipeItem row in this Pipe, resolving and transforming line by line
   * @returns {any} the result of the pipe
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
   * Transforms iterable `input` into a single value. Vars $output, $item, $key
   * and $input are available. The final statement in the iterator (the reduction)
   * is assumed to be the return value. A default $output object may be provided
   * to the iterator by placing the the second cell of the preceding row. Otherwise,
   * construct the object during first run and ensure it is the first cell of the
   * last row of the iterator, so it is returned as the $output for the next cycle
   * @param {unknown[]} input
   * @returns {unknown}
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
