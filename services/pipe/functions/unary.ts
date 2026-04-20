/**
 * Provides unary operation functions for use in HotMesh mapping rules.
 * The functions facilitate unary operations such as logical negation,
 * making a number positive or negative, and bitwise negation during
 * the mapping process. Each transformation is a function that expects
 * one input parameter from the prior row in the `@pipe` structure.
 *
 * @remarks
 * Invoked in mapping rules using `{@unary.<method>}` syntax.
 */
class UnaryHandler {
  /**
   * Returns the logical negation of a boolean value.
   *
   * @param {boolean} value - The boolean value to negate
   * @returns {boolean} The negated boolean value
   * @example
   * ```yaml
   * not_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@unary.not}"]
   * ```
   */
  not(value: boolean): boolean {
    return !value;
  }

  /**
   * Returns the positive representation of a number using
   * the unary plus operator.
   *
   * @param {number} value - The number to convert
   * @returns {number} The positive representation of the value
   * @example
   * ```yaml
   * positive_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@unary.positive}"]
   * ```
   */
  positive(value: number): number {
    return +value;
  }

  /**
   * Returns the negative representation of a number using
   * the unary negation operator.
   *
   * @param {number} value - The number to negate
   * @returns {number} The negated number
   * @example
   * ```yaml
   * negative_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@unary.negative}"]
   * ```
   */
  negative(value: number): number {
    return -value;
  }

  /**
   * Returns the bitwise negation of a number using the
   * bitwise NOT operator (`~`).
   *
   * @param {number} value - The number to bitwise negate
   * @returns {number} The bitwise negation of the value
   * @example
   * ```yaml
   * bitwise_not_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@unary.bitwise_not}"]
   * ```
   */
  bitwise_not(value: number): number {
    return ~value;
  }
}

export { UnaryHandler };
