# Symbol Handler

The `SymbolHandler` class provides a set of useful methods for returning common symbolic values and objects, such as null, undefined, whitespace, object, array, and date. These methods can be used in a variety of contexts where it is necessary to represent these values or objects programmatically.

**Table of Contents**
- [null](#null): Returns null
- [undefined](#undefined): Returns undefined
- [whitespace](#whitespace): Returns a whitespace character as a string
- [object](#object): Returns an empty object
- [array](#array): Returns an empty array
- [posInfinity](#posinfinity): Returns the positive infinity value
- [negInfinity](#neginfinity): Returns the negative infinity value
- [NaN](#nan): Returns the not-a-number value
- [date](#date): Returns the current date and time

## null

The `null` method returns the value `null`, which represents a deliberate non-value or null value. It can be used to indicate the absence of an object or value.

### Example

The null method can be used in the mapping rules as follows:

```yaml
set_null:
  "@pipe":
    - ["{@symbol.null}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_null": null
}
```

>Note that in this example, we simply call the `null` method with no parameters, since it does not require any input. This is true of all *symbol* methods.```

## undefined

The `undefined` method returns the value `undefined`, which represents a variable that has not been assigned a value. It can be used to indicate the absence of an object or value.

### Example

The undefined method can be used in the mapping rules as follows:

```yaml
set_undefined:
  "@pipe":
    - ["{@symbol.undefined}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_undefined": undefined
}
```

## whitespace

The `whitespace` method returns a single whitespace character as a string. It can be used in various contexts where it is necessary to represent a whitespace character programmatically.

### Example

The whitespace method can be used in the mapping rules as follows:

```yaml
set_whitespace:
  "@pipe":
    - ["{@symbol.whitespace}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_whitespace": " "
}
```

## object

The `object` method returns an empty object (`{}`). It can be used in various contexts where it is necessary to represent an empty object programmatically.

### Example

The object method can be used in the mapping rules as follows:

```yaml
set_object:
  "@pipe":
    - ["{@symbol.object}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_object": {}
}
```

## array

The `array` method returns an empty array (`[]`). It can be used in various contexts where it is necessary to represent an empty array programmatically.

### Example

The array method can be used in the mapping rules as follows:

```yaml
set_array:
  "@pipe":
    - ["{@symbol.array}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_array": []
}
```

## posInfinity

The `posInfinity` method returns the positive infinity value (`Infinity`). It can be used in various contexts where it is necessary to represent a positive infinity value programmatically.

### Example

The posInfinity method can be used in the mapping rules as follows:

```yaml
set_pos_infinity:
  "@pipe":
    - ["{@symbol.posInfinity}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_pos_infinity": Infinity
}
```

## negInfinity

The `negInfinity` method returns the negative infinity value (`-Infinity`). It can be used in various contexts where it is necessary to represent a negative infinity value programmatically.

### Example

The negInfinity method can be used in the mapping rules as follows:

```yaml
set_neg_infinity:
  "@pipe":
    - ["{@symbol.negInfinity}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_neg_infinity": -Infinity
}
```

## NaN

The `NaN` method returns the not-a-number value (`NaN`). It can be used in various contexts where it is necessary to represent an invalid number value programmatically.

### Example

The NaN method can be used in the mapping rules as follows:

```yaml
set_nan:
  "@pipe":
    - ["{@symbol.NaN}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "set_nan": NaN
}
```

## date

The `date` method returns the current date and time. It can be used in various contexts where it is necessary to represent the current date and time programmatically.

### Date Example 1

The date method can be used in the mapping rules as follows:

```yaml
set_date:
  "@pipe":
    - ["{@symbol.date}"]
    - ["{@json.stringify}"]
```

After executing the mapping rules, the resulting JSON object will have a key "set_date" with the current date and time as the value (the stringified representation):

```json
{
  "set_date": "2023-04-02T10:20:30.000Z"
}
```

### Date Example 2

>This example serializes the Date object using `date.toString` as opposed to `json.stringify` whih was used in **Example 1*.

The date method can be used in the mapping rules as follows:

```yaml
set_date:
  "@pipe":
    - ["{@symbol.date}"]
    - ["{@date.toString}"]
```

After executing the mapping rules, the resulting JSON object will have a key "set_date" with the current date and time as the value (the stringified representation):

```json
{
  "set_date": "Tue Apr 04 2023 14:15:57 GMT-0700 (Pacific Daylight Time)"
}
```

