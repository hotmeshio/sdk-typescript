/**
 * Provides mathematical operations and transformations for use in HotMesh
 * mapping rules. The functions facilitate a wide range of mathematical
 * operations during the mapping process, all of which can be utilised
 * through the @pipe system for a functional approach.
 *
 * @remarks Methods are invoked with the syntax `{@math.<method>}`, e.g., `{@math.add}` or `{@math.sqrt}`.
 */
class MathHandler {
  /**
   * Sums all the numbers passed as arguments. Accepts any number of
   * arguments, including nested arrays, and returns their total.
   *
   * @param {...(number | number[])[]} operands - Numbers or arrays of numbers to sum.
   * @returns {number} The sum of all operands.
   * @example
   * ```yaml
   * sum:
   *   "@pipe":
   *     - ["{a.output.data.values}"]
   *     - ["{@math.add}"]
   * ```
   */
  add(...operands: (number | number[])[]): number {
    // @ts-ignore
    return operands.reduce((a: number, b: number | number[]) => {
      if (Array.isArray(b)) {
        return a + this.add(...b);
      } else {
        return a + b;
      }
    }, 0);
  }

  /**
   * Subtracts all the numbers passed as arguments in the order they are
   * given. Accepts any number of arguments, and all arguments should be
   * numbers. The first number is the starting value; subsequent numbers are
   * subtracted from it.
   *
   * @param {...(number | number[])[]} operands - Numbers or arrays of numbers to subtract sequentially.
   * @returns {number} The result of sequential subtraction.
   * @example
   * ```yaml
   * difference:
   *   "@pipe":
   *     - ["{a.output.data.values}"]
   *     - ["{@math.subtract}"]
   * ```
   */
  subtract(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }
    let flatOperands: number[] = [];
    operands.forEach((op: number | number[]) => {
      if (Array.isArray(op)) {
        flatOperands = [...flatOperands, ...op];
      } else {
        flatOperands.push(op);
      }
    });
    if (flatOperands.length === 0) {
      throw new Error('At least one operand is required after flattening.');
    }
    const result = flatOperands.reduce((a: number, b: number, i: number) => {
      return i === 0 ? a : a - b;
    });
    return result;
  }

  /**
   * Multiplies all the numbers passed as arguments. Accepts any number of
   * arguments, including nested arrays, and returns their product.
   *
   * @param {...(number | number[])[]} operands - Numbers or arrays of numbers to multiply.
   * @returns {number} The product of all operands.
   * @example
   * ```yaml
   * product:
   *   "@pipe":
   *     - ["{a.output.data.values}", 5]
   *     - ["{@math.multiply}"]
   * ```
   */
  multiply(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }

    // @ts-ignore
    return operands.reduce((a: number, b: number | number[]) => {
      if (Array.isArray(b)) {
        return a * this.multiply(...b);
      } else {
        return a * b;
      }
    }, 1);
  }

  /**
   * Divides all the numbers passed as arguments in the order they are given.
   * The first number is the dividend; subsequent numbers are divisors.
   * Division by zero returns `NaN`.
   *
   * @param {...(number | number[])[]} operands - Numbers or arrays of numbers to divide sequentially.
   * @returns {number} The result of sequential division, or `NaN` on division by zero.
   * @example
   * ```yaml
   * quotient:
   *   "@pipe":
   *     - ["{a.output.data.values}"]
   *     - ["{@math.divide}"]
   * ```
   */
  divide(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }
    let flatOperands: number[] = [];
    operands.forEach((op: number | number[]) => {
      if (Array.isArray(op)) {
        flatOperands = [...flatOperands, ...op];
      } else {
        flatOperands.push(op);
      }
    });
    if (flatOperands.length === 0) {
      throw new Error('At least one operand is required after flattening.');
    }
    const result = flatOperands.reduce((a: number, b: number, i: number) => {
      if (b === 0) {
        return NaN;
      }
      return i === 0 ? a : a / b;
    });
    if (isNaN(result)) {
      return NaN;
    }
    return result;
  }

  /**
   * Returns the absolute value of a number.
   *
   * @param {number} x - The number to find the absolute value of.
   * @returns {number} The absolute value.
   * @example
   * ```yaml
   * absolute_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.abs}"]
   * ```
   */
  abs(x: number): number {
    return Math.abs(x);
  }

  /**
   * Returns the arccosine (in radians) of a number.
   *
   * @param {number} x - A number between -1 and 1.
   * @returns {number} The arccosine in radians.
   * @example
   * ```yaml
   * arccosine_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.acos}"]
   * ```
   */
  acos(x: number): number {
    return Math.acos(x);
  }

  /**
   * Returns the inverse hyperbolic cosine of a number.
   *
   * @param {number} x - A number greater than or equal to 1.
   * @returns {number} The inverse hyperbolic cosine.
   * @example
   * ```yaml
   * inverse_hyp_cosine:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.acosh}"]
   * ```
   */
  acosh(x: number): number {
    return Math.acosh(x);
  }

  /**
   * Returns the arcsine (in radians) of a number.
   *
   * @param {number} x - A number between -1 and 1.
   * @returns {number} The arcsine in radians.
   * @example
   * ```yaml
   * arcsine_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.asin}"]
   * ```
   */
  asin(x: number): number {
    return Math.asin(x);
  }

  /**
   * Returns the inverse hyperbolic sine of a number.
   *
   * @param {number} x - The number to compute the inverse hyperbolic sine of.
   * @returns {number} The inverse hyperbolic sine.
   * @example
   * ```yaml
   * inverse_hyp_sine:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.asinh}"]
   * ```
   */
  asinh(x: number): number {
    return Math.asinh(x);
  }

  /**
   * Returns the arctangent (in radians) of a number.
   *
   * @param {number} x - The number to compute the arctangent of.
   * @returns {number} The arctangent in radians.
   * @example
   * ```yaml
   * arctangent_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.atan}"]
   * ```
   */
  atan(x: number): number {
    return Math.atan(x);
  }

  /**
   * Returns the arctangent (in radians) of the quotient of its arguments
   * (y/x), using the signs of both to determine the quadrant.
   *
   * @param {number} y - The dividend (y-coordinate).
   * @param {number} x - The divisor (x-coordinate).
   * @returns {number} The arctangent of y/x in radians.
   * @example
   * ```yaml
   * arctangent2_value:
   *   "@pipe":
   *     - ["{a.output.data.y}", "{a.output.data.x}"]
   *     - ["{@math.atan2}"]
   * ```
   */
  atan2(y: number, x: number): number {
    return Math.atan2(y, x);
  }

  /**
   * Returns the inverse hyperbolic tangent of a number.
   *
   * @param {number} x - A number between -1 and 1 (exclusive).
   * @returns {number} The inverse hyperbolic tangent.
   * @example
   * ```yaml
   * inverse_hyp_tangent:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.atanh}"]
   * ```
   */
  atanh(x: number): number {
    return Math.atanh(x);
  }

  /**
   * Returns the cube root of a number.
   *
   * @param {number} x - The number to compute the cube root of.
   * @returns {number} The cube root.
   * @example
   * ```yaml
   * cube_root:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.cbrt}"]
   * ```
   */
  cbrt(x: number): number {
    return Math.cbrt(x);
  }

  /**
   * Returns the smallest integer greater than or equal to a given number
   * (rounds up).
   *
   * @param {number} x - The number to round up.
   * @returns {number} The ceiling value.
   * @example
   * ```yaml
   * ceiling_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.ceil}"]
   * ```
   */
  ceil(x: number): number {
    return Math.ceil(x);
  }

  /**
   * Returns the number of leading zero bits in the 32-bit binary
   * representation of a number.
   *
   * @param {number} x - The number to count leading zero bits for.
   * @returns {number} The count of leading zero bits.
   * @example
   * ```yaml
   * leading_zeros:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.clz32}"]
   * ```
   */
  clz32(x: number): number {
    return Math.clz32(x);
  }

  /**
   * Returns the cosine of a number (in radians).
   *
   * @param {number} x - The angle in radians.
   * @returns {number} The cosine of the angle.
   * @example
   * ```yaml
   * cosine_value:
   *   "@pipe":
   *     - ["{a.output.data.angle}"]
   *     - ["{@math.cos}"]
   * ```
   */
  cos(x: number): number {
    return Math.cos(x);
  }

  /**
   * Returns the hyperbolic cosine of a number.
   *
   * @param {number} x - The number to compute the hyperbolic cosine of.
   * @returns {number} The hyperbolic cosine.
   * @example
   * ```yaml
   * hyperbolic_cosine:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.cosh}"]
   * ```
   */
  cosh(x: number): number {
    return Math.cosh(x);
  }

  /**
   * Returns e raised to the power of the given number (e^x).
   *
   * @param {number} x - The exponent to raise e to.
   * @returns {number} The value of e^x.
   * @example
   * ```yaml
   * exponential_value:
   *   "@pipe":
   *     - ["{a.output.data.exponent}"]
   *     - ["{@math.exp}"]
   * ```
   */
  exp(x: number): number {
    return Math.exp(x);
  }

  /**
   * Returns e^x minus 1, providing better precision for small values of x
   * than using `exp(x) - 1`.
   *
   * @param {number} x - The exponent to raise e to before subtracting 1.
   * @returns {number} The value of e^x - 1.
   * @example
   * ```yaml
   * exponential_minus_one:
   *   "@pipe":
   *     - ["{a.output.data.exponent}"]
   *     - ["{@math.expm1}"]
   * ```
   */
  expm1(x: number): number {
    return Math.expm1(x);
  }

  /**
   * Returns the largest integer less than or equal to a given number (rounds
   * down).
   *
   * @param {number} x - The number to round down.
   * @returns {number} The floor value.
   * @example
   * ```yaml
   * floor_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.floor}"]
   * ```
   */
  floor(x: number): number {
    return Math.floor(x);
  }

  /**
   * Returns the nearest single-precision float representation of a number.
   *
   * @param {number} x - The number to round to single-precision float.
   * @returns {number} The nearest single-precision float.
   * @example
   * ```yaml
   * fround_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.fround}"]
   * ```
   */
  fround(x: number): number {
    return Math.fround(x);
  }

  /**
   * Returns the square root of the sum of the squares of its arguments
   * (Euclidean distance / hypotenuse).
   *
   * @param {...number[]} values - The numbers to compute the hypotenuse from.
   * @returns {number} The square root of the sum of squares.
   * @example
   * ```yaml
   * hypot_value:
   *   "@pipe":
   *     - ["{a.output.data.a}", "{a.output.data.b}"]
   *     - ["{@math.hypot}"]
   * ```
   */
  hypot(...values: number[]): number {
    return Math.hypot(...values);
  }

  /**
   * Returns the result of a 32-bit integer multiplication of two numbers.
   *
   * @param {number} x - The first number to multiply.
   * @param {number} y - The second number to multiply.
   * @returns {number} The 32-bit integer product.
   * @example
   * ```yaml
   * imul_value:
   *   "@pipe":
   *     - ["{a.output.data.a}", "{a.output.data.b}"]
   *     - ["{@math.imul}"]
   * ```
   */
  imul(x: number, y: number): number {
    return Math.imul(x, y);
  }

  /**
   * Returns the natural logarithm (base e) of a number.
   *
   * @param {number} x - The number to compute the natural logarithm of.
   * @returns {number} The natural logarithm.
   * @example
   * ```yaml
   * log_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.log}"]
   * ```
   */
  log(x: number): number {
    return Math.log(x);
  }

  /**
   * Returns the base 10 logarithm of a number.
   *
   * @param {number} x - The number to compute the base 10 logarithm of.
   * @returns {number} The base 10 logarithm.
   * @example
   * ```yaml
   * log10_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.log10}"]
   * ```
   */
  log10(x: number): number {
    return Math.log10(x);
  }

  /**
   * Returns the natural logarithm (base e) of 1 plus a given number.
   * Provides better precision for small values of x than `log(1 + x)`.
   *
   * @param {number} x - The number to add to 1 before computing the logarithm.
   * @returns {number} The natural logarithm of 1 + x.
   * @example
   * ```yaml
   * log1p_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.log1p}"]
   * ```
   */
  log1p(x: number): number {
    return Math.log1p(x);
  }

  /**
   * Returns the base 2 logarithm of a number.
   *
   * @param {number} x - The number to compute the base 2 logarithm of.
   * @returns {number} The base 2 logarithm.
   * @example
   * ```yaml
   * log2_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.log2}"]
   * ```
   */
  log2(x: number): number {
    return Math.log2(x);
  }

  /**
   * Returns the largest of the given numbers.
   *
   * @param {...number[]} values - The numbers to compare.
   * @returns {number} The largest value.
   * @example
   * ```yaml
   * max_value:
   *   "@pipe":
   *     - ["{a.output.data.a}", "{a.output.data.b}", "{a.output.data.c}"]
   *     - ["{@math.max}"]
   * ```
   */
  max(...values: number[]): number {
    return Math.max(...values);
  }

  /**
   * Returns the smallest of the given numbers.
   *
   * @param {...number[]} values - The numbers to compare.
   * @returns {number} The smallest value.
   * @example
   * ```yaml
   * min_value:
   *   "@pipe":
   *     - ["{a.output.data.a}", "{a.output.data.b}", "{a.output.data.c}"]
   *     - ["{@math.min}"]
   * ```
   */
  min(...values: number[]): number {
    return Math.min(...values);
  }

  /**
   * Returns the result of raising the base to the exponent power (base^exp).
   *
   * @param {number} x - The base.
   * @param {number} y - The exponent.
   * @returns {number} The result of x raised to the power y.
   * @example
   * ```yaml
   * pow_value:
   *   "@pipe":
   *     - ["{a.output.data.base}", "{a.output.data.exponent}"]
   *     - ["{@math.pow}"]
   * ```
   */
  pow(x: number, y: number): number {
    return Math.pow(x, y);
  }

  /**
   * Returns a random number between 0 (inclusive) and 1 (exclusive). Takes
   * no parameters.
   *
   * @returns {number} A pseudo-random number in [0, 1).
   * @example
   * ```yaml
   * random_value:
   *   "@pipe":
   *     - []
   *     - ["{@math.random}"]
   * ```
   */
  random(): number {
    return Math.random();
  }

  /**
   * Returns the value of a number rounded to the nearest integer.
   *
   * @param {number} x - The number to round.
   * @returns {number} The rounded integer.
   * @example
   * ```yaml
   * rounded_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.round}"]
   * ```
   */
  round(x: number): number {
    return Math.round(x);
  }

  /**
   * Returns the sign of a number, indicating whether the number is positive
   * (1), negative (-1), or zero (0).
   *
   * @param {number} x - The number to determine the sign of.
   * @returns {number} 1, -1, or 0.
   * @example
   * ```yaml
   * sign_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.sign}"]
   * ```
   */
  sign(x: number): number {
    return Math.sign(x);
  }

  /**
   * Returns the sine of a number (in radians).
   *
   * @param {number} x - The angle in radians.
   * @returns {number} The sine of the angle.
   * @example
   * ```yaml
   * sin_value:
   *   "@pipe":
   *     - ["{a.output.data.angle}"]
   *     - ["{@math.sin}"]
   * ```
   */
  sin(x: number): number {
    return Math.sin(x);
  }

  /**
   * Returns the hyperbolic sine of a number.
   *
   * @param {number} x - The number to compute the hyperbolic sine of.
   * @returns {number} The hyperbolic sine.
   * @example
   * ```yaml
   * sinh_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.sinh}"]
   * ```
   */
  sinh(x: number): number {
    return Math.sinh(x);
  }

  /**
   * Returns the square root of a number.
   *
   * @param {number} x - The number to compute the square root of.
   * @returns {number} The square root.
   * @example
   * ```yaml
   * sqrt_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.sqrt}"]
   * ```
   */
  sqrt(x: number): number {
    return Math.sqrt(x);
  }

  /**
   * Returns the tangent of a number (in radians).
   *
   * @param {number} x - The angle in radians.
   * @returns {number} The tangent of the angle.
   * @example
   * ```yaml
   * tan_value:
   *   "@pipe":
   *     - ["{a.output.data.angle}"]
   *     - ["{@math.tan}"]
   * ```
   */
  tan(x: number): number {
    return Math.tan(x);
  }

  /**
   * Returns the hyperbolic tangent of a number.
   *
   * @param {number} x - The number to compute the hyperbolic tangent of.
   * @returns {number} The hyperbolic tangent.
   * @example
   * ```yaml
   * tanh_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.tanh}"]
   * ```
   */
  tanh(x: number): number {
    return Math.tanh(x);
  }

  /**
   * Returns the integer part of a number by removing any fractional digits.
   *
   * @param {number} x - The number to truncate.
   * @returns {number} The integer part of the number.
   * @example
   * ```yaml
   * trunc_value:
   *   "@pipe":
   *     - ["{a.output.data.value}"]
   *     - ["{@math.trunc}"]
   * ```
   */
  trunc(x: number): number {
    return Math.trunc(x);
  }
}

export { MathHandler };
