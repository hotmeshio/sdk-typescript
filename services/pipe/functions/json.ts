/**
 * Provides JSON serialization and deserialization functions for use
 * in HotMesh mapping rules. Although inspired by JavaScript, they
 * have been adapted to follow a functional approach. Each transformation
 * is a function that expects one or more input parameters from the
 * prior row in the `@pipe` structure.
 *
 * @remarks
 * Invoked in mapping rules using `{@json.<method>}` syntax.
 */
class JsonHandler {
  /**
   * Converts a JavaScript object or value to a JSON string.
   * Wraps `JSON.stringify` with support for optional replacer
   * and space parameters for pretty-printing.
   *
   * @param {any} value - The value to convert to a JSON string
   * @param {Function | Array | null} [replacer] - A function that alters the behavior of the stringification process, or an array of strings and numbers to filter properties
   * @param {string | number} [space] - A string or number used to insert whitespace for readability
   * @returns {string} The JSON string representation of the value
   * @example
   * ```yaml
   * json_string:
   *   "@pipe":
   *     - ["{a}", null, 2]
   *     - ["{@json.stringify}"]
   * ```
   */
  stringify(
    value: any,
    replacer?: (
      this: any,
      key: string,
      value: any,
    ) => any | (number | string)[] | null,
    space?: string | number,
  ): string {
    return JSON.stringify(value, replacer, space);
  }

  /**
   * Parses a JSON string and returns the resulting JavaScript object.
   * Wraps `JSON.parse` with support for an optional reviver function.
   *
   * @param {string} text - The JSON string to parse
   * @param {Function} [reviver] - A function that transforms the results, called for each key-value pair
   * @returns {any} The JavaScript value parsed from the JSON string
   * @example
   * ```yaml
   * js_object:
   *   "@pipe":
   *     - ["{a}"]
   *     - ["{@json.parse}"]
   * ```
   */
  parse(
    text: string,
    reviver?: (this: any, key: string, value: any) => any,
  ): any {
    return JSON.parse(text, reviver);
  }
}

export { JsonHandler };
