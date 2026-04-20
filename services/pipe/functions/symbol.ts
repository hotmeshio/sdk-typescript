/**
 * Provides methods for returning common symbolic values and objects,
 * such as `null`, `undefined`, `whitespace`, `object`, `array`, and `date`.
 * These methods can be used in a variety of contexts where it is necessary
 * to represent these values or objects programmatically.
 *
 * @remarks
 * Invoked in mapping rules using `{@symbol.<method>}` syntax.
 * Symbol methods take no parameters; they are called directly
 * in the `@pipe` structure.
 */
class SymbolHandler {
  /**
   * Returns the value `null`, representing a deliberate non-value.
   *
   * @returns {null} The null value
   * @example
   * ```yaml
   * set_null:
   *   "@pipe":
   *     - ["{@symbol.null}"]
   * ```
   */
  null(): null {
    return null;
  }

  /**
   * Returns the value `undefined`, representing a variable
   * that has not been assigned a value.
   *
   * @returns {undefined} The undefined value
   * @example
   * ```yaml
   * set_undefined:
   *   "@pipe":
   *     - ["{@symbol.undefined}"]
   * ```
   */
  undefined(): undefined {
    return undefined;
  }

  /**
   * Returns a single whitespace character as a string.
   *
   * @returns {string} A single space character
   * @example
   * ```yaml
   * set_whitespace:
   *   "@pipe":
   *     - ["{@symbol.whitespace}"]
   * ```
   */
  whitespace(): string {
    return ' ';
  }

  /**
   * Returns an empty object (`{}`).
   *
   * @returns {object} An empty object
   * @example
   * ```yaml
   * set_object:
   *   "@pipe":
   *     - ["{@symbol.object}"]
   * ```
   */
  object(): object {
    return {};
  }

  /**
   * Returns an empty array (`[]`).
   *
   * @returns {any[]} An empty array
   * @example
   * ```yaml
   * set_array:
   *   "@pipe":
   *     - ["{@symbol.array}"]
   * ```
   */
  array(): any[] {
    return [];
  }

  /**
   * Returns the positive infinity value (`Infinity`).
   *
   * @returns {number} Positive infinity
   * @example
   * ```yaml
   * set_pos_infinity:
   *   "@pipe":
   *     - ["{@symbol.posInfinity}"]
   * ```
   */
  posInfinity(): number {
    return Infinity;
  }

  /**
   * Returns the negative infinity value (`-Infinity`).
   *
   * @returns {number} Negative infinity
   * @example
   * ```yaml
   * set_neg_infinity:
   *   "@pipe":
   *     - ["{@symbol.negInfinity}"]
   * ```
   */
  negInfinity(): number {
    return -Infinity;
  }

  /**
   * Returns the not-a-number value (`NaN`).
   *
   * @returns {number} NaN
   * @example
   * ```yaml
   * set_nan:
   *   "@pipe":
   *     - ["{@symbol.NaN}"]
   * ```
   */
  NaN(): number {
    return NaN;
  }

  /**
   * Returns the current date and time as a Date object.
   *
   * @returns {Date} The current date and time
   * @example
   * ```yaml
   * set_date:
   *   "@pipe":
   *     - ["{@symbol.date}"]
   *     - ["{@json.stringify}"]
   * ```
   */
  date(): Date {
    return new Date();
  }
}

export { SymbolHandler };
