# String Functions

In this section, various String functions will be explored, which are available for use in HotMesh mapping rules. Although inspired by JavaScript, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

**Table of Contents**
- [string.charAt](#stringcharAt): Get character at the given index
- [string.concat](#stringconcat): Concatenate two or more strings
- [string.includes](#stringincludes): Check if a string contains a specified substring
- [string.indexOf](#stringindexOf): Get the index of the first occurrence of a substring
- [string.lastIndexOf](#stringlastIndexOf): Get the index of the last occurrence of a substring
- [string.match](#stringmatch): Search for a match using a regular expression
- [string.replace](#stringreplace): Replace a substring or a pattern in a string
- [string.slice](#stringslice): Extract a portion of a string
- [string.split](#stringsplit): Split a string into an array of substrates
- [string.substring](#stringsubstring): Extract a portion of a string between two indices
- [string.toLowerCase](#stringtoLowerCase): Convert a string to lowercase
- [string.toUpperCase](#stringtoUpperCase): Convert a string to uppercase
- [string.trim](#stringtrim): Remove whitespace from both ends of a string
- [string.trimEnd](#stringtrimEnd): Remove whitespace from the end of a string
- [string.trimStart](#stringtrimStart): Remove whitespace from the start of a string

## string.charAt
The `string.charAt` function retrieves a character from a string by its index. It takes two parameters: the string from which to retrieve the character and the index of the character.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "word": "hello"
    }
  }
}
```

The goal is to create a new object with the first character in the `word` string. The `string.charAt` function can be used in the mapping rules as follows:

```yaml
first_letter: 
  "@pipe":
    - ["{a.output.data.word}", 0]
    - ["{@string.charAt}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "first_letter": "h"
}
```

## string.concat
The `string.concat` function concatenates two or more strings. It takes two or more input parameters: the strings that need to be concatenated.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "first_name": "John",
      "last_name": "Doe"
    }
  }
}
```

The goal is to create a new object with the full name by concatenating the `first_name` and `last_name`. The `string.concat` function can be used in the mapping rules as follows:

```yaml
full_name: 
  "@pipe":
    - ["{a.output.data.first_name}", " ", "{a.output.data.last_name}"]
    - ["{@string.concat}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "full_name": "John Doe"
}
```
## string.includes
The `string.includes` function checks if a string contains a specified substring. It takes two parameters: the string to search within and the substring to search for.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with a boolean value indicating if the `sentence` string contains the word "fox". The `string.includes` function can be used in the mapping rules as follows:

```yaml
contains_fox: 
  "@pipe":
    - ["{a.output.data.sentence}", "fox"]
    - ["{@string.includes}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "contains_fox": true
}
```

## string.indexOf
The `string.indexOf` function returns the index of the first occurrence of a specified substring in a string. It takes two parameters: the string to search within and the substring to search for.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with the index of the first occurrence of the word "fox" in the `sentence` string. The `string.indexOf` function can be used in the mapping rules as follows:

```yaml
fox_index: 
  "@pipe":
    - ["{a.output.data.sentence}", "fox"]
    - ["{@string.indexOf}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "fox_index": 16
}
```
## string.lastIndexOf
The `string.lastIndexOf` function returns the index of the last occurrence of a specified substring in a string. It takes two parameters: the string to search within and the substring to search for.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog and the quick cat."
    }
  }
}
```

The goal is to create a new object with the index of the last occurrence of the word "quick" in the `sentence` string. The `string.lastIndexOf` function can be used in the mapping rules as follows:

```yaml
last_quick_index: 
  "@pipe":
    - ["{a.output.data.sentence}", "quick"]
    - ["{@string.lastIndexOf}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "last_quick_index": 48
}
```

## string.repeat
The `string.repeat` function creates a new string by repeating a specified string a given number of times. It takes two parameters: the string to repeat and the number of times to repeat it.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "text": "hello"
    }
  }
}
```

The goal is to create a new object with a string consisting of the `text` string repeated 3 times. The `string.repeat` function can be used in the mapping rules as follows:

```yaml
repeated_text: 
  "@pipe":
    - ["{a.output.data.text}", 3]
    - ["{@string.repeat}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "repeated_text": "hellohellohello"
}
```
## string.slice
The `string.slice` function extracts a section of a string and returns it as a new string, without modifying the original string. It takes three parameters: the string to extract from, the start index, and the end index (optional).

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with a substring of the `sentence` string, starting from index 4 and ending at index 9. The `string.slice` function can be used in the mapping rules as follows:

```yaml
substring: 
  "@pipe":
    - ["{a.output.data.sentence}", 4, 9]
    - ["{@string.slice}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "substring": "quick"
}
```

## string.split
The `string.split` function splits a string into an array of substrings based on a specified delimiter. It takes two parameters: the string to split and the delimiter.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with an array of words from the `sentence` string. The `string.split` function can be used in the mapping rules as follows:

```yaml
words: 
  "@pipe":
    - ["{a.output.data.sentence}", " "]
    - ["{@string.split}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "words": ["The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog."]
}
```
## string.startsWith
The `string.startsWith` function determines whether a string begins with the characters of a specified string. It takes two parameters: the string to check and the string to search for at the start of the first string.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with a boolean value indicating if the `sentence` string starts with the word "The". The `string.startsWith` function can be used in the mapping rules as follows:

```yaml
starts_with_the: 
  "@pipe":
    - ["{a.output.data.sentence}", "The"]
    - ["{@string.startsWith}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "starts_with_the": true
}
```

## string.substring
The `string.substring` function returns a part of a string between the specified start and end index (end index optional). It takes three parameters: the string to extract from, the start index, and the end index (optional).

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox jumps over the lazy dog."
    }
  }
}
```

The goal is to create a new object with a substring of the `sentence` string, starting from index 4 and ending at index 9. The `string.substring` function can be used in the mapping rules as follows:

```yaml
substring: 
  "@pipe":
    - ["{a.output.data.sentence}", 4, 9]
    - ["{@string.substring}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "substring": "quick"
}
```

## string.toUpperCase

The `string.toUpperCase` function converts all the characters in a string to uppercase.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "sentence": "The quick brown fox."
    }
  }
}
```

The goal is to create a new object with the `sentence` string converted to uppercase. The `string.toUpperCase` function can be used in the mapping rules as follows:

```yaml
uppercase_sentence:
  "@pipe":
    - ["{a.output.data.sentence}"]
    - ["{@string.toUpperCase}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "uppercase_sentence": "THE QUICK BROWN FOX."
}
```

## string.toLowerCase

The `string.toLowerCase` function converts all the characters in a string to lowercase.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
"output": {
  "data": {
    "sentence": "The QUICK BROWN FOX."
    }
  }
}
```

The goal is to create a new object with the `sentence` string converted to lowercase. The `string.toLowerCase` function can be used in the mapping rules as follows:

```yaml
lowercase_sentence:
  "@pipe":
    - ["{a.output.data.sentence}"]
    - ["{@string.toLowerCase}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "lowercase_sentence": "the quick brown fox."
}
```

## string.trim
The `string.trim` function removes whitespace from both ends of a string. It takes one parameter: the string to trim.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "text": "   This is a sample text.   "
    }
  }
}
```

The goal is to create a new object with the trimmed `text` string. The `string.trim` function can be used in the mapping rules as follows:

```yaml
trimmed_text: 
  "@pipe":
    - ["{a.output.data.text}"]
    - ["{@string.trim}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "trimmed_text": "This is a sample text."
}
```

## string.trimEnd
The `string.trimEnd` function removes whitespace from the end of a string. It takes one parameter: the string to trim.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "text": "   This is a sample text.   "
    }
  }
}
```

The goal is to create a new object with the `text` string with whitespace removed only from the end. The `string.trimEnd` function can be used in the mapping rules as follows:

```yaml
trimmed_end_text: 
  "@pipe":
    - ["{a.output.data.text}"]
    - ["{@string.trimEnd}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "trimmed_end_text": "   This is a sample text."
}
```
## string.trimStart
The `string.trimStart` function removes whitespace from the beginning of a string. It takes one parameter: the string to trim.

### Example
Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "text": "   This is a sample text.   "
    }
  }
}
```

The goal is to create a new object with the `text` string with whitespace removed only from the beginning. The `string.trimStart` function can be used in the mapping rules as follows:

```yaml
trimmed_start_text: 
  "@pipe":
    - ["{a.output.data.text}"]
    - ["{@string.trimStart}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "trimmed_start_text": "This is a sample text.   "
}
```
