# Math Functions

In this section, various Math functions provided by the MathHandler class will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate mathematical operations and transformations during the mapping process. The MathHandler class consists of numerous functions that cover a wide range of mathematical operations, all of which can be utilized through the @pipe system for a functional approach.

**Table of Contents**
- [math.abs](#mathabs)
- [math.acos](#mathacos)
- [math.acosh](#mathacosh)
- [math.asin](#mathasin)
- [math.asinh](#mathasinh)
- [math.atan](#mathatan)
- [math.atan2](#mathatan2)
- [math.atanh](#mathatanh)
- [math.cbrt](#mathcbrt)
- [math.ceil](#mathceil)
- [math.clz32](#mathclz32)
- [math.cos](#mathcos)
- [math.cosh](#mathcosh)
- [math.exp](#mathexp)
- [math.expm1](#mathexpm1)
- [math.floor](#mathfloor)
- [math.fround](#mathfround)
- [math.hypot](#mathhypot)
- [math.imul](#mathimul)
- [math.log](#mathlog)
- [math.log10](#mathlog10)
- [math.log1p](#mathlog1p)
- [math.log2](#mathlog2)
- [math.max](#mathmax)
- [math.min](#mathmin)
- [math.pow](#mathpow)
- [math.random](#mathrandom)
- [math.round](#mathround)
- [math.sign](#mathsign)
- [math.sin](#mathsin)
- [math.sinh](#mathsinh)
- [math.sqrt](#mathsqrt)
- [math.tan](#mathtan)
- [math.tanh](#mathtanh)
- [math.trunc](#mathtrunc)

## math.abs

The `math.abs` function returns the absolute value of a number. It takes one parameter: the number to find the absolute value for.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: -42
    }
  }
}
```

The goal is to find the absolute value of the number at `a.output.data.value`. The `math.abs` function can be used in the mapping rules as follows:

```yaml
absolute_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.abs}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  absolute_value: 42
}
```

## math.acos

The `math.acos` function returns the arccosine (in radians) of a number. It takes one parameter: the number for which to calculate the arccosine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 0.5
    }
  }
}
```

The goal is to find the arccosine of the number at `a.output.data.value`. The `math.acos` function can be used in the mapping rules as follows:

```yaml
arccosine_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.acos}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  arccosine_value: 1.0471975511965979
}
```

## math.acosh

The `math.acosh` function returns the inverse hyperbolic cosine of a number. It takes one parameter: the number for which to calculate the inverse hyperbolic cosine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1.5
    }
  }
}
```

The goal is to find the inverse hyperbolic cosine of the number at `a.output.data.value`. The `math.acosh` function can be used in the mapping rules as follows:

```yaml
inverse_hyp_cosine:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.acosh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  inverse_hyp_cosine: 0.9624236501192069
}
```

## math.asin

The `math.asin` function returns the arcsine (in radians) of a number. It takes one parameter: the number for which to calculate the arcsine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 0.5
    }
  }
}
```

The goal is to find the arcsine of the number at `a.output.data.value`. The `math.asin` function can be used in the mapping rules as follows:

```yaml
arcsine_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.asin}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  arcsine_value: 0.5235987755982989
}
```

## math.asinh

The `math.asinh` function returns the inverse hyperbolic sine of a number. It takes one parameter: the number for which to calculate the inverse hyperbolic sine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 0.5
    }
  }
}
```

The goal is to find the inverse hyperbolic sine of the number at `a.output.data.value`. The `math.asinh` function can be used in the mapping rules as follows:

```yaml
inverse_hyp_sine:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.asinh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  inverse_hyp_sine: 0.48121182505960347
}
```

## math.atan

The `math.atan` function returns the arctangent (in radians) of a number. It takes one parameter: the number for which to calculate the arctangent.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1
    }
  }
}
```

The goal is to find the arctangent of the number at `a.output.data.value`. The `math.atan` function can be used in the mapping rules as follows:

```yaml
arctangent_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.atan}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  arctangent_value: 0.7853981633974483
}
```

## math.atan2

The `math.atan2` function returns the arctangent (in radians) of the quotient of its arguments. It takes two parameters: the dividend (y) and the divisor (x).

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      y: 1,
      x: 1
    }
  }
}
```

The goal is to find the arctangent of the quotient of the numbers at `a.output.data.y` and `a.output.data.x`. The `math.atan2` function can be used in the mapping rules as follows:

```yaml
arctangent2_value:
  "@pipe":
    - ["{a.output.data.y}", "{a.output.data.x}"]
    - ["{@math.atan2}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  arctangent2_value: 0.7853981633974483
}
```

## math.atanh

The `math.atanh` function returns the inverse hyperbolic tangent of a number. It takes one parameter: the number for which to calculate the inverse hyperbolic tangent.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 0.5
    }
  }
}
```

The goal is to find the inverse hyperbolic tangent of the number at `a.output.data.value`. The `math.atanh` function can be used in the mapping rules as follows:

```yaml
inverse_hyp_tangent:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.atanh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  inverse_hyp_tangent: 0.5493061443340549
}
```

## math.cbrt

The `math.cbrt` function returns the cube root of a number. It takes one parameter: the number for which to calculate the cube root.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 27
    }
  }
}
```

The goal is to find the cube root of the number at `a.output.data.value`. The `math.cbrt` function can be used in the mapping rules as follows:

```yaml
cube_root:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.cbrt}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  cube_root: 3
}
```

## math.ceil

The `math.ceil` function returns the smallest integer greater than or equal to a given number. It takes one parameter: the number to round up.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 4.3
    }
  }
}
```

The goal is to find the smallest integer greater than or equal to the number at `a.output.data.value`. The `math.ceil` function can be used in the mapping rules as follows:

```yaml
ceiling_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.ceil}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  ceiling_value: 5
}
```

## math.clz32

The `math.clz32` function returns the number of leading zero bits in the 32-bit binary representation of a number. It takes one parameter: the number for which to count leading zero bits.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1000
    }
  }
}
```

The goal is to count the leading zero bits in the 32-bit binary representation of the number at `a.output.data.value`. The `math.clz32` function can be used in the mapping rules as follows:

```yaml
leading_zeros:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.clz32}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  leading_zeros: 22
}
```

## math.cos

The `math.cos` function returns the cosine of a number (in radians). It takes one parameter: the number for which to calculate the cosine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      angle: 1
    }
  }
}
```

The goal is to find the cosine of the angle at `a.output.data.angle`. The `math.cos` function can be used in the mapping rules as follows:

```yaml
cosine_value:
  "@pipe":
    - ["{a.output.data.angle}"]
    - ["{@math.cos}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  cosine_value: 0.5403023058681398
}
```

## math.cosh

The `math.cosh` function returns the hyperbolic cosine of a number. It takes one parameter: the number for which to calculate the hyperbolic cosine.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1
    }
  }
}
```

The goal is to find the hyperbolic cosine of the number at `a.output.data.value`. The `math.cosh` function can be used in the mapping rules as follows:

```yaml
hyperbolic_cosine:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.cosh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  hyperbolic_cosine: 1.5430806348152437
}
```

## math.exp

The `math.exp` function returns the base of the natural logarithm (e) raised to the power of the given number. It takes one parameter: the exponent to raise e to.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      exponent: 2
    }
  }
}
```

The goal is to find e raised to the power of the exponent at `a.output.data.exponent`. The `math.exp` function can be used in the mapping rules as follows:

```yaml
exponential_value:
  "@pipe":
    - ["{a.output.data.exponent}"]
    - ["{@math.exp}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  exponential_value: 7.389056098930649
}
```

## math.expm1

The `math.expm1` function returns the result of subtracting 1 from the base of the natural logarithm (e) raised to the power of a given number. It takes one parameter: the exponent to raise e to.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      exponent: 2
    }
  }
}
```

The goal is to find e raised to the power of the exponent at `a.output.data.exponent` and subtract 1. The `math.expm1` function can be used in the mapping rules as follows:

```yaml
exponential_minus_one:
  "@pipe":
    - ["{a.output.data.exponent}"]
    - ["{@math.expm1}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  exponential_minus_one: 6.38905609893065
}
```

## math.floor

The `math.floor` function returns the largest integer less than or equal to a given number. It takes one parameter: the number to round down.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 4.7
    }
  }
}
```

The goal is to find the largest integer less than or equal to the number at `a.output.data.value`. The `math.floor` function can be used in the mapping rules as follows:

```yaml
floor_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.floor}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  floor_value: 4
}
```

## math.fround

The `math.fround` function returns the nearest single-precision float representation of a number. It takes one parameter: the number to round to the nearest single-precision float.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1.337
    }
  }
}
```

The goal is to find the nearest single-precision float representation of the number at `a.output.data.value`. The `math.fround` function can be used in the mapping rules as follows:

```yaml
fround_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.fround}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  fround_value: 1.3370000123977661
}
```

## math.hypot

The `math.hypot` function returns the square root of the sum of the squares of its arguments. It takes one or more parameters: the numbers to calculate the square root of the sum of their squares.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      a: 3,
      b: 4
    }
  }
}
```

The goal is to find the square root of the sum of the squares of the numbers at `a.output.data.a` and `a.output.data.b`. The `math.hypot` function can be used in the mapping rules as follows:

```yaml
hypot_value:
  "@pipe":
    - ["{a.output.data.a}", "{a.output.data.b}"]
    - ["{@math.hypot}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  hypot_value: 5
}
```

## math.imul

The `math.imul` function returns the result of a 32-bit integer multiplication of two numbers. It takes two parameters: the first and second numbers to multiply.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      a: 5,
      b: 10
    }
  }
}
```

The goal is to multiply the numbers at `a.output.data.a` and `a.output.data.b` using 32-bit integer multiplication. The `math.imul` function can be used in the mapping rules as follows:

```yaml
imul_value:
  "@pipe":
    - ["{a.output.data.a}", "{a.output.data.b}"]
    - ["{@math.imul}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  imul_value: 50
}
```

## math.log

The `math.log` function returns the natural logarithm (base e) of a number. It takes one parameter: the number for which to calculate the natural logarithm.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 10
    }
  }
}
```

The goal is to find the natural logarithm of the number at `a.output.data.value`. The `math.log` function can be used in the mapping rules as follows:

```yaml
log_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.log}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  log_value: 2.302585092994046
}
```

## math.log10

The `math.log10` function returns the base 10 logarithm of a number. It takes one parameter: the number for which to calculate the base 10 logarithm.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 100
    }
  }
}
```

The goal is to find the base 10 logarithm of the number at `a.output.data.value`. The `math.log10` function can be used in the mapping rules as follows:

```yaml
log10_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.log10}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  log10_value: 2
}
```

## math.log1p

The `math.log1p` function returns the natural logarithm (base e) of 1 plus a given number. It takes one parameter: the number for which to calculate the natural logarithm of 1 plus the number.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 9
    }
  }
}
```

The goal is to find the natural logarithm of 1 plus the number at `a.output.data.value`. The `math.log1p` function can be used in the mapping rules as follows:

```yaml
log1p_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.log1p}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  log1p_value: 2.302585092994046
}
```


## math.log2

The `math.log2` function returns the base 2 logarithm of a number. It takes one parameter: the number for which to calculate the base 2 logarithm.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 8
    }
  }
}
```

The goal is to find the base 2 logarithm of the number at `a.output.data.value`. The `math.log2` function can be used in the mapping rules as follows:

```yaml
log2_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.log2}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  log2_value: 3
}
```

## math.max

The `math.max` function returns the largest of the given numbers. It takes one or more parameters: the numbers to compare.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      a: 5,
      b: 10,
      c: 8
    }
  }
}
```

The goal is to find the maximum value among the numbers at `a.output.data.a`, `a.output.data.b`, and `a.output.data.c`. The `math.max` function can be used in the mapping rules as follows:

```yaml
max_value:
  "@pipe":
    - ["{a.output.data.a}", "{a.output.data.b}", "{a.output.data.c}"]
    - ["{@math.max}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  max_value: 10
}
```

## math.min

The `math.min` function returns the smallest of the given numbers. It takes one or more parameters: the numbers to compare.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      a: 5,
      b: 10,
      c: 8
    }
  }
}
```

The goal is to find the minimum value among the numbers at `a.output.data.a`, `a.output.data.b`, and `a.output.data.c`. The `math.min` function can be used in the mapping rules as follows:

```yaml
min_value:
  "@pipe":
    - ["{a.output.data.a}", "{a.output.data.b}", "{a.output.data.c}"]
    - ["{@math.min}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  min_value: 5
}
```

## math.pow

The `math.pow` function returns the result of raising the base to the exponent power. It takes two parameters: the base and the exponent.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      base: 2,
      exponent: 3
    }
  }
}
```

The goal is to find the result of raising the base at `a.output.data.base` to the exponent at `a.output.data.exponent`. The `math.pow` function can be used in the mapping rules as follows:

```yaml
pow_value:
  "@pipe":
    - ["{a.output.data.base}", "{a.output.data.exponent}"]
    - ["{@math.pow}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  pow_value: 8
}
```

## math.random

The `math.random` function returns a random number between 0 (inclusive) and 1 (exclusive). It takes no parameters.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
    }
  }
}
```

The goal is to generate a random number. The `math.random` function can be used in the mapping rules as follows:

```yaml
random_value:
  "@pipe":
    - []
    - ["{@math.random}"]
```

After executing the mapping rules, the resulting JavaScript object will contain a random number between 0 and 1:

```javascript
{
  random_value: 0.34985083016392684
}
```

## math.round

The `math.round` function returns the value of a number rounded to the nearest integer. It takes one parameter: the number to round.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 3.7
    }
  }
}
```

The goal is to round the number at `a.output.data.value`. The `math.round` function can be used in the mapping rules as follows:

```yaml
rounded_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.round}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  rounded_value: 4
}
```

## math.sign

The `math.sign` function returns the sign of a number, indicating whether the number is positive, negative, or zero. It takes one parameter: the number to determine the sign of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: -42
    }
  }
}
```

The goal is to find the sign of the number at `a.output.data.value`. The `math.sign` function can be used in the mapping rules as follows:

```yaml
sign_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.sign}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  sign_value: -1
}
```

## math.sin

The `math.sin` function returns the sine of a number (in radians). It takes one parameter: the number (in radians) to calculate the sine of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      angle: 1.5707963267948966 // pi/2 radians
    }
  }
}
```

The goal is to find the sine of the angle at `a.output.data.angle`. The `math.sin` function can be used in the mapping rules as follows:

```yaml
sin_value:
  "@pipe":
    - ["{a.output.data.angle}"]
    - ["{@math.sin}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  sin_value: 1
}
```

## math.sinh

The `math.sinh` function returns the hyperbolic sine of a number. It takes one parameter: the number to calculate the hyperbolic sine of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1
    }
  }
}
```

The goal is to find the hyperbolic sine of the number at `a.output.data.value`. The `math.sinh` function can be used in the mapping rules as follows:

```yaml
sinh_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.sinh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  sinh_value: 1.1752011936438014
}
```

## math.sqrt

The `math.sqrt` function returns the square root of a number. It takes one parameter: the number to calculate the square root of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 16
    }
  }
}
```

The goal is to find the square root of the number at `a.output.data.value`. The `math.sqrt` function can be used in the mapping rules as follows:

```yaml
sqrt_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.sqrt}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  sqrt_value: 4
}
```

## math.tan

The `math.tan` function returns the tangent of a number (in radians). It takes one parameter: the number (in radians) to calculate the tangent of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      angle: 0
    }
  }
}
```

The goal is to find the tangent of the angle at `a.output.data.angle`. The `math.tan` function can be used in the mapping rules as follows:

```yaml
tan_value:
  "@pipe":
    - ["{a.output.data.angle}"]
    - ["{@math.tan}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  tan_value: 0
}
```

## math.tanh

The `math.tanh` function returns the hyperbolic tangent of a number. It takes one parameter: the number to calculate the hyperbolic tangent of.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 1
    }
  }
}
```

The goal is to find the hyperbolic tangent of the number at `a.output.data.value`. The `math.tanh` function can be used in the mapping rules as follows:

```yaml
tanh_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.tanh}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  tanh_value: 0.7615941559557649
}
```

## math.trunc

The `math.trunc` function returns the integer part of a number by removing any fractional digits. It takes one parameter: the number to truncate.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: 42.9
    }
  }
}
```

The goal is to truncate the number at `a.output.data.value`. The `math.trunc` function can be used in the mapping rules as follows:

```yaml
trunc_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@math.trunc}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  trunc_value: 42
}
```
