/**
 * Represents a single cell value in a pipeline row
 * If the value is a string, the value will be a literal OR mapping expression
 */
type PipeItem = string | boolean | number | null | object;

/**
 * Array of `PipeItem` types.
 */
type PipeItems = PipeItem[];

/**
 * Represents a new nested context in the pipeline.
 * As the structure is recursive, the nested pipe may contain an
 * array of `PipeItem[]`, `PipeObject`, OR `ReduceObject` items.
 */
type PipeObject = { '@pipe': Array<PipeItem[] | PipeObject | ReduceObject> };

/**
 * Reduce is similar to Pipe in that it represents a new context
 * and may include an array of `PipeItem[]`, `PipeObject`, or `ReduceObject`.
 * But it also iterates and produces output. The context variables,
 * `$input`, `$output`, `$item`, `$key`, and `$index` are available.
 * @example
 *
 * // Example of a reduce expression. The optional object is empty and
 * // serves as the $output during iteration. The last step in the iteration
 * // sets $output. if ther are no more iterations, the $output is returned
 * // and provided to the next step in the pipeline.
 *
 * // If data is:
 * // { response: ['a', 'b', 'c'] }
 *
 * // The reduced/transformed expression will be:
 * // { 'a': 0, 'b': 1, 'c': 2 }
 *
 * // Arrays and objects may be iterated over and transformed.
 *
 * //YAML
 * '@pipe':
 *   - ['{data.response}', {}]
 *   - '@reduce':
 *       - '@pipe':
 *           - ['{$output}']
 *       - '@pipe':
 *           - ['{$item}']
 *       - '@pipe':
 *           - ['{$index}']
 *       - ['{@object.set}']
 */
type ReduceObject = {
  '@reduce': Array<PipeItem[] | PipeObject | ReduceObject>;
};

/**
 * Represents a sequence in the pipeline that can include arrays of `PipeItem`, `PipeObject`, or `ReduceObject`.
 */
type Pipe = (PipeItem[] | PipeObject | ReduceObject)[];

/**
 * Defines the context of a pipeline operation.
 */
type PipeContext = {
  /**
   * Input of the current iteration.
   */
  $input: unknown[];

  /**
   * Output of the current iteration and the final output for the reducer.
   */
  $output: unknown;

  /**
   * Target item in the iterator.
   */
  $item: unknown;

  /**
   * Array index as string or object key.
   */
  $key: string;

  /**
   * Numeric index of the iterator.
   */
  $index: number;
};

export { Pipe, PipeContext, PipeItem, PipeItems, PipeObject, ReduceObject };
