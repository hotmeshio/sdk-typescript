# Object Functions

In this section, various Object functions will be explored. Although inspired by JavaScript, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

**Table of Contents**
- [object.get](#objectget)
- [object.set](#objectset)
- [object.create](#objectcreate)
- [object.keys](#objectkeys)
- [object.values](#objectvalues)
- [object.entries](#objectentries)
- [object.assign](#objectassign)
- [object.fromEntries](#objectfromentries)
- [object.getOwnPropertyNames](#objectgetownpropertynames)
- [object.getOwnPropertySymbols](#objectgetownpropertysymbols)
- [object.getOwnPropertyDescriptor](#objectgetownpropertydescriptor)
- [object.defineProperty](#objectdefineproperty)
- [object.defineProperties](#objectdefineproperties)
- [object.freeze](#objectfreeze)
- [object.isFrozen](#objectisfrozen)
- [object.seal](#objectseal)
- [object.isSealed](#objectissealed)
- [object.preventExtensions](#objectpreventextensions)
- [object.isExtensible](#objectisextensible)
- [object.hasOwnProperty](#objecthasownproperty)
- [object.isPrototypeOf](#objectisprototypeof)
- [object.propertyIsEnumerable](#objectpropertyisenumerable)

## object.get

The `object.get` function retrieves an element from an object by its property name. It takes two parameters: the object from which to retrieve the property value and the name of the elepropertyproment.

### Example

Suppose there is the following input JSON object:

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

The goal is to get the value of the second color from the `colors` object. The `object.get` function can be used in the mapping rules as follows:

```yaml
second_color: 
  "@pipe":
    - ["{a.output.data.colors}", "second"]
    - ["{@object.get}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "second_color": "green"
}
```

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

Here is the generated README documentation for the remaining methods in the `ObjectHandler` class:

## object.set

The `object.set` function sets a property on an object with a specified value and returns the object. It takes three parameters: the object to set the property on, the name of the property, and the value to assign to the property. If the input object is `undefined` or `null`, a new object will be created.

### Example

Suppose there is the following input JSON object:

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

The goal is to set the value of a new property `"third"` to `"blue"` in the `colors` object. The `object.set` function can be used in the mapping rules as follows:

```yaml
output:
  "@pipe":
    - ["{a.output.data.colors}", "third", "blue"]
    - ["{@object.set}"]
```

After executing the mapping rules, the resulting JSON object will be:

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

## object.create

The `object.create` function creates a new object with specified key-value pairs. It takes a variable number of arguments, where each pair of arguments represents a key and its corresponding value. If no arguments are provided, an empty object will be created.

### Example

Suppose the goal is to create a new object with the following properties:

- `"name"` with the value `"John"`
- `"age"` with the value `30`
- `"city"` with the value `"New York"`

The `object.create` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["name", "John", "age", 30, "city", "New York"]
    - ["{@object.create}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

## object.getOwnPropertyNames

The `object.getOwnPropertyNames` function returns an array of all the enumerable and non-enumerable property names of an object. It takes one parameter: the object to retrieve the property names from. If the input object is `undefined` or `null`, an empty array will be returned.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

The goal is to retrieve all the property names of the `person` object. The `object.getOwnPropertyNames` function can be used in the mapping rules as follows:

```yaml
propertyNames:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.getOwnPropertyNames}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "propertyNames": ["name", "age", "city"]
}
```

## object.getOwnPropertySymbols

The `object.getOwnPropertySymbols` function returns an array of all the symbol properties of an object. It takes one parameter: the object to retrieve the symbol properties from. If the input object is `undefined` or `null`, an empty array will be returned.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York",
    "[Symbol('id')]": 1234
  }
}
```

The goal is to retrieve all the symbol properties of the `person` object. The `object.getOwnPropertySymbols` function can be used in the mapping rules as follows:

```yaml
symbolProperties:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.getOwnPropertySymbols}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "symbolProperties": [Symbol('id')]
}
```

## object.getOwnPropertyDescriptor

The `object.getOwnPropertyDescriptor` function returns a property descriptor for an own property of an object. It takes two parameters: the object to retrieve the property descriptor from and the name of the property. If the input object is `undefined` or `null`, `undefined` will be returned.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

The goal is to retrieve the property descriptor for the `"age"` property of the `person` object. The `object.getOwnPropertyDescriptor` function can be used in the mapping rules as follows:

```yaml
ageDescriptor:
  "@pipe":
    - ["{a.person}", "age"]
    - ["{@object.getOwnPropertyDescriptor}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "ageDescriptor": {
    "value": 30,
    "writable": true,
    "enumerable": true,
    "configurable": true
  }
}
```

## object.defineProperty

The `object.defineProperty` function defines a new property or modifies an existing property on an object and returns the object. It takes three parameters: the object to define the property on, the name of the property, and a property descriptor object.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to define a new read-only property `"city"` with the value `"New York"` on the `person` object. The `object.defineProperty` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["{a.person}", "city", {"value": "New York", "writable": false}]
    - ["{@object.defineProperty}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

The `"city"` property is now read-only and cannot be modified.

## object.defineProperties

The `object.defineProperties` function defines new properties or modifies existing properties on an object and returns the object. It takes two parameters: the object to define the properties on and an object containing the property descriptors.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John"
  }
}
```

The goal is to define multiple properties on the `person` object:

- `"age"` with the value `30`
- `"city"` with the value `"New York"` (read-only)

The `object.defineProperties` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["{a.person}", {"age": {"value": 30}, "city": {"value": "New York", "writable": false}}]
    - ["{@object.defineProperties}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30,
    "city": "New York"
  }
}
```

The `"age"` property is writable, while the `"city"` property is read-only.

## object.freeze

The `object.freeze` function freezes an object, making it immutable. It prevents new properties from being added, existing properties from being removed, and the values of existing properties from being modified. It takes one parameter: the object to freeze.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to freeze the `person` object to make it immutable. The `object.freeze` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.freeze}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The `person` object is now frozen and cannot be modified.

## object.isFrozen

The `object.isFrozen` function determines if an object is frozen. It takes one parameter: the object to check. It returns `true` if the object is frozen, otherwise `false`.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to check if the `person` object is frozen. The `object.isFrozen` function can be used in the mapping rules as follows:

```yaml
isFrozen:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.isFrozen}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "isFrozen": false
}
```

If the `person` object was previously frozen using `object.freeze`, the result would be `true`.

## object.seal

The `object.seal` function seals an object, preventing new properties from being added and marking all existing properties as non-configurable. However, the values of existing properties can still be modified. It takes one parameter: the object to seal.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to seal the `person` object. The `object.seal` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.seal}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The `person` object is now sealed. No new properties can be added, and existing properties cannot be deleted or reconfigured, but their values can still be modified.

## object.isSealed

The `object.isSealed` function determines if an object is sealed. It takes one parameter: the object to check. It returns `true` if the object is sealed, otherwise `false`.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to check if the `person` object is sealed. The `object.isSealed` function can be used in the mapping rules as follows:

```yaml
isSealed:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.isSealed}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "isSealed": false
}
```

If the `person` object was previously sealed using `object.seal`, the result would be `true`.

## object.preventExtensions

The `object.preventExtensions` function prevents new properties from being added to an object. It takes one parameter: the object to make non-extensible.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to prevent new properties from being added to the `person` object. The `object.preventExtensions` function can be used in the mapping rules as follows:

```yaml
person:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.preventExtensions}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The `person` object is now non-extensible. No new properties can be added, but existing properties can still be modified or deleted.

## object.isExtensible

The `object.isExtensible` function determines if an object is extensible (i.e., whether new properties can be added to it). It takes one parameter: the object to check. It returns `true` if the object is extensible, otherwise `false`.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to check if the `person` object is extensible. The `object.isExtensible` function can be used in the mapping rules as follows:

```yaml
isExtensible:
  "@pipe":
    - ["{a.person}"]
    - ["{@object.isExtensible}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "isExtensible": true
}
```

If the `person` object was previously made non-extensible using `object.preventExtensions`, the result would be `false`.

## object.hasOwnProperty

The `object.hasOwnProperty` function determines if an object has a specified property as its own property (as opposed to inheriting it). It takes two parameters: the object to check and the name of the property. It returns `true` if the object has the specified property as its own property, otherwise `false`.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to check if the `person` object has a property named `"age"` as its own property. The `object.hasOwnProperty` function can be used in the mapping rules as follows:

```yaml
hasAge:
  "@pipe":
    - ["{a.person}", "age"]
    - ["{@object.hasOwnProperty}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "hasAge": true
}
```

If the `"age"` property was not directly defined on the `person` object, but rather inherited from its prototype chain, the result would be `false`.

## object.isPrototypeOf

The `object.isPrototypeOf` function determines if an object exists in another object's prototype chain. It takes two parameters: the object to check and the prototype object. It returns `true` if the prototype object is found in the object's prototype chain, otherwise `false`.

### Example

Suppose there are the following input JSON objects:

```json
{
  "person": {
    "name": "John",
    "age": 30
  },
  "employee": {
    "job": "Developer",
    "salary": 50000
  }
}
```

The goal is to check if the `person` object is a prototype of the `employee` object. The `object.isPrototypeOf` function can be used in the mapping rules as follows:

```yaml
isPersonPrototypeOfEmployee:
  "@pipe":
    - ["{a.person}", "{a.employee}"]
    - ["{@object.isPrototypeOf}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "isPersonPrototypeOfEmployee": false
}
```

In this example, the `person` object is not a prototype of the `employee` object, so the result is `false`.

## object.propertyIsEnumerable

The `object.propertyIsEnumerable` function determines if a specified property is enumerable. It takes two parameters: the object to check and the name of the property. It returns `true` if the specified property is enumerable, otherwise `false`.

### Example

Suppose there is the following input JSON object:

```json
{
  "person": {
    "name": "John",
    "age": 30
  }
}
```

The goal is to check if the `"age"` property of the `person` object is enumerable. The `object.propertyIsEnumerable` function can be used in the mapping rules as follows:

```yaml
isAgeEnumerable:
  "@pipe":
    - ["{a.person}", "age"]
    - ["{@object.propertyIsEnumerable}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "isAgeEnumerable": true
}
```

By default, properties created via simple assignment or property initializer syntax are enumerable. If the `"age"` property was defined as non-enumerable using `Object.defineProperty` or `Object.defineProperties`, the result would be `false`.
