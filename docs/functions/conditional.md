# Conditional Functions

In this section, various conditional functions will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate the manipulation and transformation of data using conditional operations during the mapping process. These functions are adapted from the `ConditionalHandler` class, and are implemented as a functional system referred to as @pipes.

**Table of Contents**
- [conditional.ternary](#conditionalternary)
- [conditional.equality](#conditionalequality)
- [conditional.strict_equality](#conditionalstrict_equality)
- [conditional.greater_than](#conditionalgreater_than)
- [conditional.less_than](#conditionalless_than)
- [conditional.greater_than_or_equal](#conditionalgreater_than_or_equal)
- [conditional.less_than_or_equal](#conditionalless_than_or_equal)

## conditional.ternary

The `conditional.ternary` function evaluates a condition and returns one of the two provided values based on the result of the condition. It takes three parameters: the condition to evaluate, the value to return if the condition is true, and the value to return if the condition is false.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "isActive": true,
    "activeLabel": "Active",
    "inactiveLabel": "Inactive"
  }
}
```

The goal is to create a new object with the label based on the `isActive` value. The `conditional.ternary` function can be used in the mapping rules as follows:

```yaml
status_label:
  "@pipe":
    - ["{a.data.isActive}", "{a.data.activeLabel}", "{a.data.inactiveLabel}"]
    - ["{@conditional.ternary}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "status_label": "Active"
}
```

## conditional.equality

The `conditional.equality` function checks whether two values are equal, using non-strict equality (==). It takes two parameters: the first value (`value1`) and the second value (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": "42"
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` and `value2` are equal. The `conditional.equality` function can be used in the mapping rules as follows:

```yaml
are_values_equal:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.equality}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "are_values_equal": true
}
```

## conditional.strict_equality

The `conditional.strict_equality` function checks whether two values are equal, using strict equality (===). It takes two parameters: the first value (`value1`) and the second value (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": "42"
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` and `value2` are strictly equal. The `conditional.strict_equality` function can be used in the mapping rules as follows:

```yaml
are_values_strictly_equal:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.strict_equality}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "are_values_strictly_equal": false
}
```

## conditional.greater_than

The `conditional.greater_than` function checks whether the first value is greater than the second value. It takes two parameters: the first number (`value1`) and the second number (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": 30
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` is greater than `value2`. The `conditional.greater_than` function can be used in the mapping rules as follows:

```yaml
is_value1_greater_than_value2:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.greater_than}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_value1_greater_than_value2": true
}
```

## conditional.less_than

The `conditional.less_than` function checks whether the first value is less than the second value. It takes two parameters: the first number (`value1`) and the second number (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": 50
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` is less than `value2`. The `conditional.less_than` function can be used in the mapping rules as follows:

```yaml
is_value1_less_than_value2:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.less_than}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_value1_less_than_value2": true
}
```

## conditional.greater_than_or_equal

The `conditional.greater_than_or_equal` function checks whether the first value is greater than or equal to the second value. It takes two parameters: the first number (`value1`) and the second number (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": 42
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` is greater than or equal to `value2`. The `conditional.greater_than_or_equal` function can be used in the mapping rules as follows:

```yaml
is_value1_greater_than_or_equal_value2:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.greater_than_or_equal}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_value1_greater_than_or_equal_value2": true
}
```

## conditional.less_than_or_equal

The `conditional.less_than_or_equal` function checks whether the first value is less than or equal to the second value. It takes two parameters: the first number (`value1`) and the second number (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 42,
    "value2": 42
  }
}
```

The goal is to create a new object with a boolean value indicating whether `value1` is less than or equal to `value2`. The `conditional.less_than_or_equal` function can be used in the mapping rules as follows:

```yaml
is_value1_less_than_or_equal_value2:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.less_than_or_equal}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "is_value1_less_than_or_equal_value2": true
}
```


## conditional.nullish

The `conditional.nullish` function checks whether the first value is null or undefined. It takes two parameters: the first value (`value1`) and the second value (`value2`).

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "data": {
    "value1": 0,
    "value2": 10
  }
}
```

The goal is to create a new object with a non-null value, allowing for values like `0` and `false` to be considered:

```yaml
non_null_value:
  "@pipe":
    - ["{a.data.value1}", "{a.data.value2}"]
    - ["{@conditional.nullish}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "non_null_value": 0
}
```
