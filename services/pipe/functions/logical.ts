/**
 * Provides logical operator functions for combining multiple boolean
 * expressions and returning a single boolean value. These operators
 * are available for use in HotMesh mapping rules.
 *
 * @remarks
 * Invoked in mapping rules using `{@logical.<method>}` syntax.
 */
class LogicalHandler {
  /**
   * Returns `true` if both boolean expressions evaluate to `true`.
   * Performs a logical AND operation.
   *
   * @param {boolean} firstValue - The first boolean expression
   * @param {boolean} secondValue - The second boolean expression
   * @returns {boolean} `true` if both values are truthy, `false` otherwise
   * @example
   * ```yaml
   * is_active_and_paid:
   *   "@pipe":
   *     - ["{a.user.isActive}", "{a.user.hasPaid}"]
   *     - ["{@logical.and}"]
   * ```
   */
  and(firstValue: boolean, secondValue: boolean): boolean {
    return firstValue && secondValue;
  }

  /**
   * Returns `true` if at least one of the boolean expressions
   * evaluates to `true`. Performs a logical OR operation.
   *
   * @param {boolean} firstValue - The first boolean expression
   * @param {boolean} secondValue - The second boolean expression
   * @returns {boolean} `true` if either value is truthy, `false` otherwise
   * @example
   * ```yaml
   * has_trial_or_subscription:
   *   "@pipe":
   *     - ["{b.user.hasTrial}", "{b.user.hasSubscription}"]
   *     - ["{@logical.or}"]
   * ```
   */
  or(firstValue: boolean, secondValue: boolean): boolean {
    return firstValue || secondValue;
  }
}

export { LogicalHandler };
