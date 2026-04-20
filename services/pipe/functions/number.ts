/**
 * Provides functional transformations for numbers within HotMesh mapping
 * rules. These functions facilitate the manipulation and transformation
 * of numbers during the mapping process. Although inspired by JavaScript,
 * they have been adapted to follow a functional approach. Each transformation
 * is a function that expects one or more input parameters from the prior
 * row in the `@pipe` structure.
 *
 * @remarks Invoked via `{@number.<method>}` in YAML mapping rules.
 */
class NumberHandler {
  /**
   * Checks if a number is finite. Returns false for Infinity, -Infinity,
   * and NaN.
   *
   * @param {number} input - The number to check
   * @returns {boolean} True if the number is finite, otherwise false
   * @example
   * ```yaml
   * is_finite:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.isFinite}"]
   * ```
   */
  isFinite(input: number): boolean {
    return Number.isFinite(input);
  }

  /**
   * Checks if a number is even.
   *
   * @param {number} input - The number to check
   * @returns {boolean} True if the number is even, otherwise false
   * @example
   * ```yaml
   * is_even:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.isEven}"]
   * ```
   */
  isEven(input: number): boolean {
    return !isNaN(input) && input % 2 === 0;
  }

  /**
   * Checks if a number is odd.
   *
   * @param {number} input - The number to check
   * @returns {boolean} True if the number is odd, otherwise false
   * @example
   * ```yaml
   * is_odd:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.isOdd}"]
   * ```
   */
  isOdd(input: number): boolean {
    return !isNaN(input) && input % 2 !== 0;
  }

  /**
   * Checks if a number is an integer (has no fractional component).
   *
   * @param {number} input - The number to check
   * @returns {boolean} True if the number is an integer, otherwise false
   * @example
   * ```yaml
   * is_integer:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.isInteger}"]
   * ```
   */
  isInteger(input: number): boolean {
    return Number.isInteger(input);
  }

  /**
   * Checks if the given value is NaN (Not-a-Number).
   *
   * @param {number} input - The value to check
   * @returns {boolean} True if the value is NaN, otherwise false
   * @example
   * ```yaml
   * is_nan:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.isNaN}"]
   * ```
   */
  isNaN(input: number): boolean {
    return Number.isNaN(input);
  }

  /**
   * Parses a string and returns a floating-point number.
   *
   * @param {string} input - The string to parse
   * @returns {number} The parsed floating-point number, or NaN if the string cannot be parsed
   * @example
   * ```yaml
   * float_value:
   *   "@pipe":
   *     - ["{a.output.data.string_value}"]
   *     - ["{@number.parseFloat}"]
   * ```
   */
  parseFloat(input: string): number {
    return parseFloat(input);
  }

  /**
   * Parses a string and returns an integer of the specified radix (base).
   *
   * @param {string} input - The string to parse
   * @param {number} [radix] - The radix (base) to use for parsing (e.g., 10 for decimal, 16 for hexadecimal)
   * @returns {number} The parsed integer, or NaN if the string cannot be parsed
   * @example
   * ```yaml
   * decimal_value:
   *   "@pipe":
   *     - ["{a.output.data.hex_value}", 16]
   *     - ["{@number.parseInt}"]
   * ```
   */
  parseInt(input: string, radix?: number): number {
    return parseInt(input, radix);
  }

  /**
   * Formats a number using fixed-point notation with a specified number
   * of digits after the decimal point.
   *
   * @param {number} input - The number to format
   * @param {number} [digits] - The number of digits to appear after the decimal point
   * @returns {string} A string representing the number in fixed-point notation
   * @example
   * ```yaml
   * fixed_value:
   *   "@pipe":
   *     - ["{a.output.data.value}", 2]
   *     - ["{@number.toFixed}"]
   * ```
   */
  toFixed(input: number, digits?: number): string {
    return input.toFixed(digits);
  }

  /**
   * Formats a number using exponential (scientific) notation with a
   * specified number of fractional digits.
   *
   * @param {number} input - The number to format
   * @param {number} [fractionalDigits] - The number of digits after the decimal point
   * @returns {string} A string representing the number in exponential notation
   * @example
   * ```yaml
   * exponential_value:
   *   "@pipe":
   *     - ["{a.output.data.value}", 2]
   *     - ["{@number.toExponential}"]
   * ```
   */
  toExponential(input: number, fractionalDigits?: number): string {
    return input.toExponential(fractionalDigits);
  }

  /**
   * Formats a number to a specified precision (total number of significant
   * digits), using either fixed-point or exponential notation depending
   * on the value.
   *
   * @param {number} input - The number to format
   * @param {number} [precision] - The number of significant digits
   * @returns {string} A string representing the number to the specified precision
   * @example
   * ```yaml
   * precise_value:
   *   "@pipe":
   *     - ["{a.output.data.value}", 4]
   *     - ["{@number.toPrecision}"]
   * ```
   */
  toPrecision(input: number, precision?: number): string {
    return input.toPrecision(precision);
  }

  /**
   * Checks if a number is greater than or equal to a comparison value.
   *
   * @param {number} input - The number to compare
   * @param {number} compareValue - The value to compare against
   * @returns {boolean} True if input is greater than or equal to compareValue
   * @example
   * ```yaml
   * is_gte:
   *   "@pipe":
   *     - ["{a.output.data.value}", 10]
   *     - ["{@number.gte}"]
   * ```
   */
  gte(input: number, compareValue: number): boolean {
    return input >= compareValue;
  }

  /**
   * Checks if a number is less than or equal to a comparison value.
   *
   * @param {number} input - The number to compare
   * @param {number} compareValue - The value to compare against
   * @returns {boolean} True if input is less than or equal to compareValue
   * @example
   * ```yaml
   * is_lte:
   *   "@pipe":
   *     - ["{a.output.data.value}", 10]
   *     - ["{@number.lte}"]
   * ```
   */
  lte(input: number, compareValue: number): boolean {
    return input <= compareValue;
  }

  /**
   * Checks if a number is strictly greater than a comparison value.
   *
   * @param {number} input - The number to compare
   * @param {number} compareValue - The value to compare against
   * @returns {boolean} True if input is greater than compareValue
   * @example
   * ```yaml
   * is_gt:
   *   "@pipe":
   *     - ["{a.output.data.value}", 10]
   *     - ["{@number.gt}"]
   * ```
   */
  gt(input: number, compareValue: number): boolean {
    return input > compareValue;
  }

  /**
   * Checks if a number is strictly less than a comparison value.
   *
   * @param {number} input - The number to compare
   * @param {number} compareValue - The value to compare against
   * @returns {boolean} True if input is less than compareValue
   * @example
   * ```yaml
   * is_lt:
   *   "@pipe":
   *     - ["{a.output.data.value}", 10]
   *     - ["{@number.lt}"]
   * ```
   */
  lt(input: number, compareValue: number): boolean {
    return input < compareValue;
  }

  /**
   * Returns the largest of the provided numbers.
   *
   * @param {...number[]} values - The numbers to compare
   * @returns {number} The largest number among the provided values
   * @example
   * ```yaml
   * maximum:
   *   "@pipe":
   *     - ["{a.output.data.val1}", "{a.output.data.val2}", "{a.output.data.val3}"]
   *     - ["{@number.max}"]
   * ```
   */
  max(...values: number[]): number {
    return Math.max(...values);
  }

  /**
   * Returns the smallest of the provided numbers.
   *
   * @param {...number[]} values - The numbers to compare
   * @returns {number} The smallest number among the provided values
   * @example
   * ```yaml
   * minimum:
   *   "@pipe":
   *     - ["{a.output.data.val1}", "{a.output.data.val2}", "{a.output.data.val3}"]
   *     - ["{@number.min}"]
   * ```
   */
  min(...values: number[]): number {
    return Math.min(...values);
  }

  /**
   * Returns the base raised to the exponent power.
   *
   * @param {number} base - The base number
   * @param {number} exponent - The exponent to raise the base to
   * @returns {number} The result of base raised to the power of exponent
   * @example
   * ```yaml
   * power:
   *   "@pipe":
   *     - ["{a.output.data.base}", "{a.output.data.exponent}"]
   *     - ["{@number.pow}"]
   * ```
   */
  pow(base: number, exponent: number): number {
    return Math.pow(base, exponent);
  }

  /**
   * Rounds a number to the nearest integer.
   *
   * @param {number} input - The number to round
   * @returns {number} The value of the number rounded to the nearest integer
   * @example
   * ```yaml
   * rounded:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@number.round}"]
   * ```
   */
  round(input: number): number {
    return Math.round(input);
  }
}

export { NumberHandler };
