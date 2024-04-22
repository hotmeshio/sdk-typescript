# Array Functions

In this section, various Array functions will be explored, which are available for use in HotMesh mapping rules. Although inspired by and familiar to JavaScript developers, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

**Table of Contents**
- [array.get](#arrayget)
- [array.indexOf](#arrayindexof)
- [array.length](#arraylength)
- [array.concat](#arrayconcat)
- [array.join](#arrayjoin)
- [array.lastIndexOf](#arraylastindexof)
- [array.pop](#arraypop)
- [array.push](#arraypush)
- [array.reverse](#arrayreverse)
- [array.shift](#arrayshift)
- [array.slice](#arrayslice)
- [array.sort](#arraysort)
- [array.splice](#arraysplice)
- [array.unshift](#arrayunshift)

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

The `array.slice` function returns a shallow copy of a portion of an array into a new array object selected from the beginning index to, but not including, the end index. It takes three parameters: the array to slice, the beginning index, and the end index. The end index is optional and if not provided, the slice will continue to the end of the array.

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

## array.concat

The `array.concat` function concatenates two arrays and returns a new array. It takes two parameters: the first array and the second array to be concatenated.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors1": ["red", "green"],
      "colors2": ["blue", "yellow"]
    }
  }
}
```

The goal is to create a new object with the concatenated colors from `colors1` and `colors2`. The `array.concat` function can be used in the mapping rules as follows:

```yaml
concatenated_colors:
  "@pipe":
    - ["{a.output.data.colors1}", "{a.output.data.colors2}"]
    - ["{@array.concat}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "concatenated_colors": ["red", "green", "blue", "yellow"]
}
```

## array.lastIndexOf

The `array.lastIndexOf` function returns the last index at which a given element can be found in the array, or -1 if it is not present. It takes two parameters: the array and the element to locate in the array.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue", "green", "yellow"]
    }
  }
}
```

The goal is to find the last index of the color "green" in the `colors` array. The `array.lastIndexOf` function can be used in the mapping rules as follows:

```yaml
last_green_index:
  "@pipe":
    - ["{a.output.data.colors}", "green"]
    - ["{@array.lastIndexOf}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "last_green_index": 3
}
```

## array.pop

The `array.pop` function removes the last element from an array and returns that element. It takes one parameter: the array from which to remove the last element.

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

The goal is to remove the last color from the `colors` array and create a new object with the removed color. The `array.pop` function can be used in the mapping rules as follows:

```yaml
last_color:
  "@pipe":
    - ["{a.output.data.colors}"]
    - ["{@array.pop}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "last_color": "yellow"
}
```

## array.push

The `array.push` function adds one or more elements to the end of an array and returns the modified array. It takes two or more parameters: the array to which to add elements and the elements to add.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue"]
    }
  }
}
```

The goal is to add the color "yellow" to the end of the `colors` array and create a new object with the modified array. The `array.push` function can be used in the mapping rules as follows:

```yaml
updated_colors:
  "@pipe":
    - ["{a.output.data.colors}", "yellow"]
    - ["{@array.push}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "updated_colors": ["red", "green", "blue", "yellow"]
}
```

## array.shift

The `array.shift` function removes the first element from an array and returns that element. It takes one parameter: the array from which to remove the first element.

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

The goal is to remove the first color from the `colors` array and create a new object with the removed color. The `array.shift` function can be used in the mapping rules as follows:

```yaml
first_color:
  "@pipe":
    - ["{a.output.data.colors}"]
    - ["{@array.shift}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "first_color": "red"
}
```

## array.sort

The `array.sort` function sorts the elements of an array in place and returns the sorted array. It takes two parameters: the array to sort and an optional compare function to define the sort order.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "numbers": [3, 1, 4, 1, 5, 9, 2, 6, 5]
    }
  }
}
```

The goal is to sort the `numbers` array in ascending order (the default) and create a new object with the sorted array. The `array.sort` function can be used in the mapping rules as follows:

```yaml
sorted_numbers:
  "@pipe":
    - ["{a.output.data.numbers}"]
    - ["{@array.sort}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "sorted_numbers": [1, 1, 2, 3, 4, 5, 5, 6, 9]
}
```

The goal is to sort the `numbers` array in descending order and create a new object with the sorted array. The `array.sort` function can be used in the mapping rules as follows:

```yaml
sorted_numbers:
  "@pipe":
    - ["{a.output.data.numbers}", DESCENDING]
    - ["{@array.sort}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "sorted_numbers": [9, 6, 5, 5, 4, 3, 2, 1, 1]
}
```

## array.splice

The `array.splice` function changes the contents of an array by removing or replacing existing elements and/or adding new elements in place. It takes three or more parameters: the array to modify, the start index, the number of elements to remove (optional), and the elements to add (optional).

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

The goal is to remove two colors starting from index 1 and add the color "orange" in their place. The `array.splice` function can be used in the mapping rules as follows:

```yaml
modified_colors:
  "@pipe":
    - ["{a.output.data.colors}", 1, 2, "orange"]
    - ["{@array.splice}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "modified_colors": ["red", "orange", "yellow"]
}
```

## array.unshift

The `array.unshift` function adds one or more elements to the beginning of an array and returns the new length of the array. It takes two or more parameters: the array to which to add elements and the elements to add.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": ["red", "green", "blue"]
    }
  }
}
```

The goal is to add the color "yellow" to the beginning of the `colors` array and create a new object with the modified array. The `array.unshift` function can be used in the mapping rules as follows:

```yaml
updated_colors:
  "@pipe":
    - ["{a.output.data.colors}", "yellow"]
    - ["{@array.unshift}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "updated_colors": ["yellow", "red", "green", "blue"]
}
```
