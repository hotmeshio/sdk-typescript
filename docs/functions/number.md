# Number Functions

In this section, various Number functions will be explored, which are available for use in HotMesh mapping rules. These functions are designed to facilitate the manipulation and transformation of numbers during the mapping process. They are inspired by and should be familiar to JavaScript developers. The key principle to keep in mind is that each transformation is a function that expects one or more input parameters from the prior row in the \@pipe structure.

**Table of Contents**
- [number.isFinite](#numberisfinite): Check if a number is finite
- [number.isEven](#numberiseven): Check if a number is even
- [number.isOdd](#numberisodd): Check if a number is odd
- [number.isInteger](#numberisinteger): Check if a number is an integer
- [number.isNaN](#numberisnan): Check if a value is NaN
- [number.parseFloat](#numberparsefloat): Parse a string and return a floating-point number
- [number.parseInt](#numberparseint): Parse a string and return an integer
- [number.toExponential](#numbertoexponential): Return a string representing the number in exponential notation
- [number.toFixed](#numbertofixed): Return a string representing the number in fixed-point notation
- [number.toPrecision](#numbertoprecision): Return a string representing the number to a specified precision

## number.isFinite
The `number.isFinite` function checks if a number is finite. It takes one parameter: the number to be checked.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42
    }
  }
}
```

The goal is to create a new object with a boolean field indicating if the `value` field is a finite number. The `number.isFinite` function can be used in the mapping rules as follows:

```yaml
is_finite: 
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@number.isFinite}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_finite": true
}
```

## number.isOdd
The `number.isOdd` function checks if a number is odd. It takes one parameter: the number to be checked.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42
    }
  }
}
```

The goal is to create a new object with a boolean field indicating if the `value` field is an odd number. The `number.isOdd` function can be used in the mapping rules as follows:

```yaml
is_odd: 
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@number.isOdd}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_odd": false
}
```

## number.isEven
The `number.isEven` function checks if a number is even. It takes one parameter: the number to be checked.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42
    }
  }
}
```

The goal is to create a new object with a boolean field indicating if the `value` field is an even number. The `number.isEven` function can be used in the mapping rules as follows:

```yaml
is_even: 
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@number.isEven}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_even": true
}
```

## number.isInteger
The `number.isInteger` function checks if a number is an integer. It takes one parameter: the number to be checked.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42
    }
  }
}
```

The goal is to create a new object with a boolean field indicating if the `value` field is an integer. The `number.isInteger` function can be used in the mapping rules as follows:

```yaml
is_integer: 
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@number.isInteger}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_integer": true
}
```

## number.toExponential
The `number.toExponential` function formats a number using exponential notation, rounding if necessary. It takes two parameters: the number to be formatted and the number of digits after the decimal point.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 123456
    }
  }
}
```

The goal is to create a new object with the `value` field represented in exponential notation with 2 decimal places. The `number.toExponential` function can be used in the mapping rules as follows:

```yaml
exponential_value: 
  "@pipe":
    - ["{a.output.data.value}", 2]
    - ["{@number.toExponential}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "exponential_value": "1.23e+5"
}
```
## number.toPrecision
The `number.toPrecision` function formats a number to a specified precision (total number of significant digits) using either fixed-point or exponential notation, depending on the value. It takes two parameters: the number to be formatted and the precision.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42.12345
    }
  }
}
```

The goal is to create a new object with the `value` field formatted to a precision of 4 significant digits. The `number.toPrecision` function can be used in the mapping rules as follows:

```yaml
precise_value: 
  "@pipe":
    - ["{a.output.data.value}", 4]
    - ["{@number.toPrecision}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "precise_value": "42.12"
}
```

## number.parseInt
The `number.parseInt` function parses a string and returns an integer of the specified radix (base). It takes two parameters: the string to be parsed and the radix.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "hex_value": "1a"
    }
  }
}
```

The goal is to create a new object with the `hex_value` field converted to a decimal integer. The `number.parseInt` function can be used in the mapping rules as follows:

```yaml
decimal_value: 
  "@pipe":
    - ["{a.output.data.hex_value}", 16]
    - ["{@number.parseInt}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "decimal_value": 26
}
```
## number.parseFloat
The `number.parseFloat` function parses a string and returns a floating-point number. It takes a single parameter: the string to be parsed.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "string_value": "3.14159"
    }
  }
}
```

The goal is to create a new object with the `string_value` field converted to a floating-point number. The `number.parseFloat` function can be used in the mapping rules as follows:

```yaml
float_value: 
  "@pipe":
    - ["{a.output.data.string_value}"]
    - ["{@number.parseFloat}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "float_value": 3.14159
}
```

## number.toFixed
The `number.toFixed` function formats a number using fixed-point notation, with a specified number of digits after the decimal point. It takes two parameters: the number to be formatted and the number of digits after the decimal point.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 123.45678
    }
  }
}
```

The goal is to create a new object with the `value` field formatted with 2 digits after the decimal point. The `number.toFixed` function can be used in the mapping rules as follows:

```yaml
fixed_value: 
  "@pipe":
    - ["{a.output.data.value}", 2]
    - ["{@number.toFixed}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "fixed_value": "123.46"
}
```

## number.toString
The `number.toString` function converts a number to a string representation. It takes two parameters: the number to be converted, and the radix (optional, default is 10) to which the number should be converted.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value": 42
    }
  }
}
```

The goal is to create a new object with the `value` field converted to a string representation. The `number.toString` function can be used in the mapping rules as follows:

```yaml
string_value: 
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@number.toString}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "string_value": "42"
}
```
## number.isNaN
The `number.isNaN` function checks if the given value is Not-a-Number (NaN). It takes one parameter: the value to be checked.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "value1": "NaN",
      "value2": 42
    }
  }
}
```

The goal is to create a new object with two boolean fields, `is_value1_nan` and `is_value2_nan`, indicating whether the respective values are NaN or not. The `number.isNaN` function can be used in the mapping rules as follows:

```yaml
is_value1_nan: 
  "@pipe":
    - ["{a.output.data.value1}"]
    - ["{@number.isNaN}"]
is_value2_nan: 
  "@pipe":
    - ["{a.output.data.value2}"]
    - ["{@number.isNaN}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_value1_nan": true,
  "is_value2_nan": false
}
```
