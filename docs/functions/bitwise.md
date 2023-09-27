# Bitwise Functions

In this section, various bitwise functions will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate the manipulation and transformation of numbers using bitwise operations during the mapping process. These functions are adapted from the `BitwiseHandler` class, and are implemented as a functional system referred to as @pipes.

**Table of Contents**
- [bitwise.and](#bitwiseand)
- [bitwise.or](#bitwiseor)
- [bitwise.xor](#bitwisexor)
- [bitwise.leftShift](#bitwiseleftshift)
- [bitwise.rightShift](#bitwiserightshift)
- [bitwise.unsignedRightShift](#bitwiseunsignedrightshift)

## bitwise.and

The `bitwise.and` function performs a bitwise AND operation on two numbers. It takes two parameters: the first number (`a`) and the second number (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": 5,
    "b": 3
  }
}
```

The goal is to create a new object with the result of the bitwise AND operation on the numbers `a` and `b`. The `bitwise.and` function can be used in the mapping rules as follows:

```yaml
bitwise_and_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.b}"]
    - ["{@bitwise.and}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_and_result": 1
}
```

## bitwise.or

The `bitwise.or` function performs a bitwise OR operation on two numbers. It takes two parameters: the first number (`a`) and the second number (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": 5,
    "b": 3
  }
}
```

The goal is to create a new object with the result of the bitwise OR operation on the numbers `a` and `b`. The `bitwise.or` function can be used in the mapping rules as follows:

```yaml
bitwise_or_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.b}"]
    - ["{@bitwise.or}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_or_result": 7
}
```

## bitwise.xor

The `bitwise.xor` function performs a bitwise XOR operation on two numbers. It takes two parameters: the first number (`a`) and the second number (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": 5,
    "b": 3
  }
}
```

The goal is to create a new object with the result of the bitwise XOR operation on the numbers `a` and `b`. The `bitwise.xor` function can be used in the mapping rules as follows:

```yaml
bitwise_xor_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.b}"]
    - ["{@bitwise.xor}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_xor_result": 6
}
```

## bitwise.leftShift

The `bitwise.leftShift` function performs a bitwise left shift operation on a number. It takes two parameters: the first number (`a`) and the number of positions to shift (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": 5,
    "shift": 2
  }
}
```

The goal is to create a new object with the result of the bitwise left shift operation on the number `a` by `shift` positions. The `bitwise.leftShift` function can be used in the mapping rules as follows:

```yaml
bitwise_left_shift_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.shift}"]
    - ["{@bitwise.leftShift}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_left_shift_result": 20
}
```

## bitwise.rightShift

The `bitwise.rightShift` function performs a bitwise right shift operation on a number, preserving the sign bit. It takes two parameters: the first number (`a`) and the number of positions to shift (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": -12,
    "shift": 2
  }
}
```

The goal is to create a new object with the result of the bitwise right shift operation on the number `a` by `shift` positions. The `bitwise.rightShift` function can be used in the mapping rules as follows:

```yaml
bitwise_right_shift_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.shift}"]
    - ["{@bitwise.rightShift}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_right_shift_result": -3
}
```

## bitwise.unsignedRightShift

The `bitwise.unsignedRightShift` function performs a bitwise unsigned right shift operation on a number. It takes two parameters: the first number (`a`) and the number of positions to shift (`b`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "numbers": {
    "a": -12,
    "shift": 2
  }
}
```

The goal is to create a new object with the result of the bitwise unsigned right shift operation on the number `a` by `shift` positions. The `bitwise.unsignedRightShift` function can be used in the mapping rules as follows:

```yaml
bitwise_unsigned_right_shift_result:
  "@pipe":
    - ["{a.numbers.a}", "{a.numbers.shift}"]
    - ["{@bitwise.unsignedRightShift}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "bitwise_unsigned_right_shift_result": 1073741821
}
```
