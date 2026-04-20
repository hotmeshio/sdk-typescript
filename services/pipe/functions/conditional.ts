/**
 * Provides conditional logic functions for use within HotMesh mapping
 * rules. Although inspired by JavaScript operators, these methods have
 * been adapted to follow a functional approach. Each transformation is
 * a function that expects one or more input parameters from the prior
 * row in the `@pipe` structure.
 *
 * @remarks Invoked via {@conditional.<method>} in YAML mapping rules.
 */
class ConditionalHandler {
  /**
   * Evaluates a condition and returns one of two provided values based
   * on the result. Equivalent to the JavaScript ternary operator
   * (`condition ? valueIfTrue : valueIfFalse`).
   *
   * @param {boolean} condition - The condition to evaluate
   * @param {any} valueIfTrue - The value to return if the condition is true
   * @param {any} valueIfFalse - The value to return if the condition is false
   * @returns {any} The value corresponding to the condition result
   * @example
   * ```yaml
   * status_label:
   *   "@pipe":
   *     - ["{a.data.isActive}", "{a.data.activeLabel}", "{a.data.inactiveLabel}"]
   *     - ["{@conditional.ternary}"]
   * ```
   */
  ternary(condition: boolean, valueIfTrue: any, valueIfFalse: any): any {
    return condition ? valueIfTrue : valueIfFalse;
  }

  /**
   * Checks whether two values are equal using non-strict equality (`==`).
   * Type coercion is applied, so `42 == "42"` returns true.
   *
   * @param {any} value1 - The first value to compare
   * @param {any} value2 - The second value to compare
   * @returns {boolean} True if the values are loosely equal, otherwise false
   * @example
   * ```yaml
   * are_values_equal:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.equality}"]
   * ```
   */
  equality(value1: any, value2: any): boolean {
    return value1 == value2;
  }

  /**
   * Checks whether two values are equal using strict equality (`===`).
   * No type coercion is applied, so `42 === "42"` returns false.
   *
   * @param {any} value1 - The first value to compare
   * @param {any} value2 - The second value to compare
   * @returns {boolean} True if the values are strictly equal, otherwise false
   * @example
   * ```yaml
   * are_values_strictly_equal:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.strict_equality}"]
   * ```
   */
  strict_equality(value1: any, value2: any): boolean {
    return value1 === value2;
  }

  /**
   * Checks whether two values are not equal using non-strict inequality
   * (`!=`). Type coercion is applied, so `42 != "42"` returns false.
   *
   * @param {any} value1 - The first value to compare
   * @param {any} value2 - The second value to compare
   * @returns {boolean} True if the values are not loosely equal, otherwise false
   * @example
   * ```yaml
   * are_values_not_equal:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.inequality}"]
   * ```
   */
  inequality(value1: any, value2: any): boolean {
    return value1 != value2;
  }

  /**
   * Checks whether two values are not equal using strict inequality
   * (`!==`). No type coercion is applied, so `42 !== "42"` returns true.
   *
   * @param {any} value1 - The first value to compare
   * @param {any} value2 - The second value to compare
   * @returns {boolean} True if the values are strictly not equal, otherwise false
   * @example
   * ```yaml
   * are_values_strictly_not_equal:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.strict_inequality}"]
   * ```
   */
  strict_inequality(value1: any, value2: any): boolean {
    return value1 !== value2;
  }

  /**
   * Checks whether the first value is greater than the second value.
   *
   * @param {number} value1 - The first number to compare
   * @param {number} value2 - The second number to compare
   * @returns {boolean} True if value1 is greater than value2, otherwise false
   * @example
   * ```yaml
   * is_greater:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.greater_than}"]
   * ```
   */
  greater_than(value1: number, value2: number): boolean {
    return value1 > value2;
  }

  /**
   * Checks whether the first value is less than the second value.
   *
   * @param {number} value1 - The first number to compare
   * @param {number} value2 - The second number to compare
   * @returns {boolean} True if value1 is less than value2, otherwise false
   * @example
   * ```yaml
   * is_less:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.less_than}"]
   * ```
   */
  less_than(value1: number, value2: number): boolean {
    return value1 < value2;
  }

  /**
   * Checks whether the first value is greater than or equal to the
   * second value.
   *
   * @param {number} value1 - The first number to compare
   * @param {number} value2 - The second number to compare
   * @returns {boolean} True if value1 is greater than or equal to value2, otherwise false
   * @example
   * ```yaml
   * is_gte:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.greater_than_or_equal}"]
   * ```
   */
  greater_than_or_equal(value1: number, value2: number): boolean {
    return value1 >= value2;
  }

  /**
   * Checks whether the first value is less than or equal to the second
   * value.
   *
   * @param {number} value1 - The first number to compare
   * @param {number} value2 - The second number to compare
   * @returns {boolean} True if value1 is less than or equal to value2, otherwise false
   * @example
   * ```yaml
   * is_lte:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.less_than_or_equal}"]
   * ```
   */
  less_than_or_equal(value1: number, value2: number): boolean {
    return value1 <= value2;
  }

  /**
   * Returns the first value if it is not null or undefined, otherwise
   * returns the second value. Equivalent to the JavaScript nullish
   * coalescing operator (`??`). Unlike logical OR, this allows falsy
   * values like `0` and `false` to pass through.
   *
   * @param {any} value1 - The value to test for null/undefined
   * @param {any} value2 - The fallback value if value1 is null or undefined
   * @returns {any} value1 if it is not null/undefined, otherwise value2
   * @example
   * ```yaml
   * non_null_value:
   *   "@pipe":
   *     - ["{a.data.value1}", "{a.data.value2}"]
   *     - ["{@conditional.nullish}"]
   * ```
   */
  nullish(value1: any, value2: any): any {
    return value1 ?? value2;
  }
}

export { ConditionalHandler };
