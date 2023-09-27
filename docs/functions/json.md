# JSON Functions

In this section, various JSON functions provided by the JsonHandler class will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate the manipulation and transformation of JSON data during the mapping process. The JsonHandler class consists of two functions: stringify and parse, which can be utilized through the @pipe system for a functional approach.

**Table of Contents**
- [json.stringify](#jsonstringify)
- [json.parse](#jsonparse)

## json.stringify

The `json.stringify` function converts a JavaScript object or value to a JSON string. It takes up to three parameters: the value to convert, an optional replacer function or array of strings and numbers, and an optional space parameter (a number or a string) for pretty-printing.

### Example

Suppose there is the following input JavaScript object:

**Object A:**
```javascript
{
  name: "John",
  age: 30,
  city: "New York"
}
```

The goal is to convert this JavaScript object into a JSON `string`. The `json.stringify` function can be used in the mapping rules as follows:

```yaml
json_string:
  "@pipe":
    - ["{a}", null, 2]
    - ["{@json.stringify}"]
```

After executing the mapping rules, the resulting JSON `string` will be:

```json
{
  "name": "John",
  "age": 30,
  "city": "New York"
}
```

## json.parse

The `json.parse` function converts a JSON `string` into a JavaScript object. It takes two parameters: the JSON `string` to parse and an optional reviver function to transform the resulting object's properties.

### Example

Suppose there is the following input JSON `string`:

**String A:**
```json
{
  "name": "John",
  "age": 30,
  "city": "New York"
}
```

The goal is to convert this JSON string into a JavaScript object. The `json.parse` function can be used in the mapping rules as follows:

```yaml
js_object:
  "@pipe":
    - ["{a}"]
    - ["{@json.parse}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  name: "John",
  age: 30,
  city: "New York"
}
```
