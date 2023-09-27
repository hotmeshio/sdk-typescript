# Unary Functions

In this section, various Unary functions will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate unary operations such as logical negation, making a number positive or negative, and bitwise negation during the mapping process. Each transformation is a function that expects one input parameter from the prior row in the @pipe structure.

**Table of Contents**
- [unary.not](#unarynot)
- [unary.positive](#unarypositive)
- [unary.negative](#unarynegative)
- [unary.bitwise_not](#unarybitwise_not)

## unary.not

The `unary.not` function returns the logical negation of a boolean value. It takes one parameter: the boolean value to negate.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  output: {
    data: {
      value: true
    }
  }
}
```

The goal is to negate the boolean value at `a.output.data.value`. The `unary.not` function can be used in the mapping rules as follows:

```yaml
not_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@unary.not}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  not_value: false
}
```

## unary.positive

The `unary.positive` function returns the positive representation of a number. It takes one parameter: the number to make positive.

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

The goal is to make the number at `a.output.data.value` positive. The `unary.positive` function can be used in the mapping rules as follows:

```yaml
positive_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@unary.positive}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  positive_value: 42
}
```

## unary.negative

The `unary.negative` function returns the negative representation of a number. It takes one parameter: the number to make negative.

### Example

Suppose there is the following input JavaScript object:

```javascript
{
  output: {
    data: {
      value: 42
    }
  }
}
```

The goal is to make the number at `a.output.data.value` negative. The `unary.negative` function can be used in the mapping rules as follows:

```yaml
negative_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@unary.negative}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  negative_value: -42
}
```

## unary.bitwise_not

The `unary.bitwise_not` function returns the bitwise negation of a number. It takes one parameter: the number to negate.

### Example

Suppose there is the following input JavaScript object:

```javascript
{
  output: {
    data: {
      value: 42
    }
  }
}
```

The goal is to perform a bitwise negation of the number at `a.output.data.value`. The `unary.bitwise_not` function can be used in the mapping rules as follows:

```yaml
bitwise_not_value:
  "@pipe":
    - ["{a.output.data.value}"]
    - ["{@unary.bitwise_not}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  bitwise_not_value: -43
}
```