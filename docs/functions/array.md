# Array Functions

In this section, various Array functions will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate the manipulation and transformation of arrays during the mapping process. Although inspired by and familiar to JavaScript developers, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

**Table of Contents**
- [array.get](#arrayget)
- [array.indexOf](#arrayindexof)
- [array.length](#arraylength)
- [array.map](#arraymap)
- [array.reverse](#arrayreverse)
- [array.slice](#arrayslice)
- [array.some](#arraysome)
- [array.find](#arrayfind)
- [array.filter](#arrayfilter)
- [array.findIndex](#arrayfindindex)
- [array.join](#arrayjoin)
- [array.reduce](#arrayreduce)

## array.get

The `array.get` function retrieves an element from an array by its index. It takes two parameters: the array from which to retrieve the element and the index of the element.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue", "yellow"]
    }
  }
}
```

The goal is to create a new object with the second color in the `colors` array. The `array.get` function can be used in the mapping rules as follows:

```yaml
second_color: 
  "@pipe":
    - ["{a.output.data.colors}", 1]
    - ["{@array.get}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "second_color": "green"
}
```

## array.indexOf

The `array.indexOf` function returns the first index at which a given element can be found in an array, or -1 if the element is not found. It takes two parameters: the array to search and the element to find.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue", "yellow"]
    }
  }
}
```

The goal is to create a new object with the index of the color "green" in the `colors` array. The `array.indexOf` function can be used in the mapping rules as follows:

```yaml
green_index: 
  "@pipe":
    - ["{a.output.data.colors}", "green"]
    - ["{@array.indexOf}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "green_index": 1
}
```

## array.length

The `array.length` function returns the length of an array. It takes one parameter: the array for which the length needs to be calculated.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue", "yellow"]
    }
  }
}
```

The goal is to create a new object with the length of the `colors` array. The `array.length` function can be used in the mapping rules as follows:

```yaml
num_colors: 
  "@pipe":
    - ["{a.output.data.colors}"]
    - ["{@array.length}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "num_colors": 4
}
```

## array.map

The `array.map` function applies a given transformation function to each element of an array and returns a new array with the transformed elements. It takes two parameters: the array to transform and the transformation function.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 2, 3, 4, 5]
    }
  }
}
```

The goal is to create a new object with the squares of the numbers in the `numbers` array. The `array.map` function can be used in the mapping rules as follows:

```yaml
squares: 
  "@pipe":
    - ["{a.output.data.numbers}", "{@number.square}"]
    - ["{@array.map}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "squares": [1, 4, 9, 16, 25]
}
```

## array.reverse

The `array.reverse` function reverses the order of the elements in an array. It takes one parameter: the array to reverse.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 2, 3, 4, 5]
    }
  }
}
```

The goal is to create a new object with the reversed `numbers` array. The `array.reverse` function can be used in the mapping rules as follows:

```yaml
reversed_numbers: 
  "@pipe":
    - ["{a.output.data.numbers}"]
    - ["{@array.reverse}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "reversed_numbers": [5, 4, 3, 2, 1]
}
```

## array.slice

The `array.slice` function returns a shallow copy of a portion of an array into a new array object selected from the beginning index to, but not including, the end index. It takes three parameters: the array to slice, the beginning index, and the end index.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 2, 3, 4, 5]
    }
  }
}
```

The goal is to create a new object with a slice of the `numbers` array from index 1 to index 3. The `array.slice` function can be used in the mapping rules as follows:

```yaml
numbers_slice: 
  "@pipe":
    - ["{a.output.data.numbers}", 1, 3]
    - ["{@array.slice}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "numbers_slice": [2, 3]
}
```

## array.some

The `array.some` function tests whether at least one element in the array passes the test implemented by the provided function. It takes two parameters: the array to test and the test function. The test function should be in the format of `{@function_name}`.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 3, 5, 7, 9]
    }
  }
}
```

The goal is to create a new object with a boolean indicating whether at least one element in the `numbers` array is even. The `array.some` function can be used in the mapping rules as follows:

```yaml
any_even: 
  "@pipe":
    - ["{a.output.data.numbers}", "{@number.isEven}"]
    - ["{@array.some}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "any_even": false
}
```
## array.find

The `array.find` function returns the first element in the array that satisfies the provided testing function, or `null` if no such element is found. It takes two parameters: the array to search and the test function. The test function should be in the format of `{@function_name}`.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 3, 5, 4, 9]
    }
  }
}
```

The goal is to create a new object with the first even number in the `numbers` array. The `array.find` function can be used in the mapping rules as follows:

```yaml
first_even: 
  "@pipe":
    - ["{a.output.data.numbers}", "{@number.isEven}"]
    - ["{@array.find}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "first_even": 4
}
```

## array.filter

The `array.filter` function creates a new array with all elements that pass the test implemented by the provided function. It takes two parameters: the array to filter and the test function. The test function should be in the format of `{@function_name}`.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }
  }
}
```

The goal is to create a new object with an array of even numbers from the `numbers` array. The `array.filter` function can be used in the mapping rules as follows:

```yaml
even_numbers: 
  "@pipe":
    - ["{a.output.data.numbers}", "{@number.isEven}"]
    - ["{@array.filter}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "even_numbers": [2, 4, 6, 8]
}
```
## array.findIndex

`array.findIndex` function returns the index of the first element in the array that satisfies the provided testing function. If no element is found that satisfies the testing function, it returns -1.

**Example:**

Input JSON:

```json
{
  "a": {
    "output": {
      "data": {
        "numbers": [10, 20, 30, 40, 50]
      }
    }
  }
}
```

Mapping rules:

```yaml
first_greater_than_25:
  "@pipe":
    - ["{a.output.data.numbers}"]
    - ["{@array.findIndex}", 25, "{@number.gt}"]
```

Output JSON:

```json
{
  "first_greater_than_25": 2
}
```

## array.join

`array.join` function joins all elements of an array into a string, using a specified delimiter between each element.

**Example:**

Input JSON:

```json
{
  "a": {
    "output": {
      "data": {
        "words": ["Hello", "world", "how", "are", "you?"]
      }
    }
  }
}
```

Mapping rules:

```yaml
sentence:
  "@pipe":
    - ["{a.output.data.words}", " "]
    - ["{@array.join}"]
```

Output JSON:

```json
{
  "sentence": "Hello world how are you?"
}
```
## array.reduce

`array.reduce` function reduces an array to a single value, using a provided reducer function. The reducer function takes an accumulator and the current element as arguments, and returns a new accumulator value.

**Example:**

Input JSON:

```json
{
  "a": {
    "output": {
      "data": {
        "numbers": [1, 2, 3, 4, 5]
      }
    }
  }
}
```

Mapping rules:

```yaml
sum:
  "@pipe":
    - ["{a.output.data.numbers}"]
    - ["{@array.reduce}",
        "@pipe":
          - ["{@math.add}"]
      ]
```

Output JSON:

```json
{
  "sum": 15
}
```
