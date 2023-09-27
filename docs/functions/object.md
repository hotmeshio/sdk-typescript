# Object Functions

In this section, various Object functions will be explored, which are available for use in HotMesh mapping rules. The functions are designed to facilitate the manipulation and transformation of objects during the mapping process. Although inspired by and familiar to JavaScript developers, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

**Table of Contents**
- [object.keys](#objectkeys)
- [object.values](#objectvalues)
- [object.entries](#objectentries)
- [object.assign](#objectassign)
- [object.fromEntries](#objectfromentries)

## object.keys

The `object.keys` function retrieves an array of a given object's property names. It takes one parameter: the object whose property names are to be retrieved.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "color": "red",
      "size": "large",
      "shape": "circle"
    }
  }
}
```

The goal is to create a new object with an array containing the property names of the `data` object. The `object.keys` function can be used in the mapping rules as follows:

```yaml
property_names: 
  "@pipe":
    - ["{a.output.data}"]
    - ["{@object.keys}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "property_names": ["color", "size", "shape"]
}
```

## object.values

The `object.values` function retrieves an array of the values in the object. It takes one parameter: the object from which to extract the values.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": {
        "first": "red",
        "second": "green",
        "third": "blue"
      }
    }
  }
}
```

The goal is to create a new object with an array of the color values. The `object.values` function can be used in the mapping rules as follows:

```yaml
color_values:
  "@pipe":
  - ["{a.output.data.colors}"]
  - ["{@object.values}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "color_values": ["red", "green", "blue"]
}
```

## object.entries

The `object.entries` function retrieves an array of the object's key-value pairs. It takes one parameter: the object from which to extract the entries.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": {
        "first": "red",
        "second": "green",
        "third": "blue"
      }
    }
  }
}
```

The goal is to create a new object with an array of the key-value pairs. The `object.entries` function can be used in the mapping rules as follows:

```yaml
color_entries:
  "@pipe":
    - ["{a.output.data.colors}"]
    - ["{@object.entries}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "color_entries": [["first", "red"], ["second", "green"], ["third", "blue"]]
}
```

## object.assign

The `object.assign` function merges one or more source objects into a target object. It takes two or more parameters: the target object and the source objects.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "colors": {
        "first": "red",
        "second": "green"
      }
    }
  }
}
```

**Object B:**
```json
{
  "output": {
    "data": {
      "colors": {
        "third": "blue"
      }
    }
  }
}
```

The goal is to create a new object with the combined color properties. The `object.assign` function can be used in the mapping rules as follows:

```yaml
combined_colors:
  "@pipe":
    - ["{@symbol.object}", "{a.output.data.colors}", "{b.output.data.colors}"]
    - ["{@object.assign}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "combined_colors": {
    "first": "red",
    "second": "green",
    "third": "blue"
  }
}
```

## object.fromEntries

The `object.fromEntries` function transforms an array of key-value pairs into an object. It takes one parameter: an array of key-value pairs.

### Example

Suppose there are the following input JSON objects:

**Object A:**
```json
{
  "output": {
    "data": {
      "pairs": [["first", "red"], ["second", "green"], ["third", "blue"]]
    }
  }
}
```

The goal is to create a new object with color properties from the key-value pairs in the pairs array. The object.fromEntries function can be used in the mapping rules as follows:

```yaml
color_object:
  "@pipe":
    - ["{a.output.data.pairs}"]
    - ["{@object.fromEntries}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "color_object": {
    "first": "red",
    "second": "green",
    "third": "blue"
  }
}
```
