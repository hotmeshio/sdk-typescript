/**
 * Provides bitwise operation functions for use in HotMesh mapping rules.
 * Although inspired by JavaScript, they have been adapted to follow a
 * functional approach. Each transformation is a function that expects
 * one or more input parameters from the prior row in the `@pipe` structure.
 *
 * @remarks
 * Invoked in mapping rules using `{@bitwise.<method>}` syntax.
 */
class BitwiseHandler {
  /**
   * Performs a bitwise AND operation on two numbers.
   *
   * @param {number} a - The first operand
   * @param {number} b - The second operand
   * @returns {number} The result of `a & b`
   * @example
   * ```yaml
   * bitwise_and_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.b}"]
   *     - ["{@bitwise.and}"]
   * ```
   */
  and(a: number, b: number): number {
    return a & b;
  }

  /**
   * Performs a bitwise OR operation on two numbers.
   *
   * @param {number} a - The first operand
   * @param {number} b - The second operand
   * @returns {number} The result of `a | b`
   * @example
   * ```yaml
   * bitwise_or_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.b}"]
   *     - ["{@bitwise.or}"]
   * ```
   */
  or(a: number, b: number): number {
    return a | b;
  }

  /**
   * Performs a bitwise XOR operation on two numbers.
   *
   * @param {number} a - The first operand
   * @param {number} b - The second operand
   * @returns {number} The result of `a ^ b`
   * @example
   * ```yaml
   * bitwise_xor_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.b}"]
   *     - ["{@bitwise.xor}"]
   * ```
   */
  xor(a: number, b: number): number {
    return a ^ b;
  }

  /**
   * Performs a bitwise left shift operation on a number.
   *
   * @param {number} a - The number to shift
   * @param {number} b - The number of positions to shift left
   * @returns {number} The result of `a << b`
   * @example
   * ```yaml
   * bitwise_left_shift_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.shift}"]
   *     - ["{@bitwise.leftShift}"]
   * ```
   */
  leftShift(a: number, b: number): number {
    return a << b;
  }

  /**
   * Performs a bitwise right shift operation on a number,
   * preserving the sign bit.
   *
   * @param {number} a - The number to shift
   * @param {number} b - The number of positions to shift right
   * @returns {number} The result of `a >> b`
   * @example
   * ```yaml
   * bitwise_right_shift_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.shift}"]
   *     - ["{@bitwise.rightShift}"]
   * ```
   */
  rightShift(a: number, b: number): number {
    return a >> b;
  }

  /**
   * Performs a bitwise unsigned right shift operation on a number.
   *
   * @param {number} a - The number to shift
   * @param {number} b - The number of positions to shift right
   * @returns {number} The result of `a >>> b`
   * @example
   * ```yaml
   * bitwise_unsigned_right_shift_result:
   *   "@pipe":
   *     - ["{a.numbers.a}", "{a.numbers.shift}"]
   *     - ["{@bitwise.unsignedRightShift}"]
   * ```
   */
  unsignedRightShift(a: number, b: number): number {
    return a >>> b;
  }
}

export { BitwiseHandler };
