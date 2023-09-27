# Data Mapping | (@Pipes)

This tutorial covers various data mapping functions available in HotMesh. The functions are organized based on their data type categories, such as array, object, number, string, etc., which should be familiar to JavaScript developers. However, it's essential to understand that the approach in HotMesh is functional. Each transformation is a function that expects one or more input parameters from the prior row in the mapping rules.

 - [Array Functions](./functions/array.md)
 - [Bitwise Functions](./functions/bitwise.md)
 - [Conditional Functions](./functions/conditional.md)
 - [Date/Time Functions](./functions/date.md)
 - [JSON Functions](./functions/json.md)
 - [Math Functions](./functions/math.md)
 - [Number Functions](./functions/number.md)
 - [Object Functions](./functions/object.md)
 - [String Functions](./functions/string.md)
 - [Symbol Functions](./functions/symbol.md)
 - [Unary Functions](./functions/unary.md)

Feel free to explore these function categories and learn more about how to use them effectively in your HotMesh mapping rules.

## Quick Start
This section will help you quickly get started with data mapping principles, demonstrating how to map data from multiple source objects to a single destination object.

### Overview
In  HotMesh, the receiver (object C in our example), drives the transformation. It combines and maps the data from objects A and B (the sources). Let's consider the following example:

<small>**Object A**</small>
```json
{
  "id": "123",
  "first_name": "John",
  "last_name": "Doe"
}
```

<small>**Object B**</small>

```json
{
  "email": "john.doe@example.com",
  "age": 30
}
```

Given these source objects, we want to create a new object, C, with the following structure:

 1. `first` - Rename and map the `first_name` field from object A.
 2. `last` - Rename and map the `last_name` field from object A.
 3. `email` - Use the `email` field from object B.
 4. `age` - Use the `age` field from object B.
 5. `is_employee` - Set a static boolean value.
 6. `company` - Set a static string value.
 7. `bonus` - Set a static number value.

Most fields can be mapped using a combination of object notation ({}) and static data. We use curly braces to reference and map the data from objects A and B into the corresponding fields of object C. We also include static values for `is_employee`, `company`, and `bonus`:

```yaml
first: "{a.output.data.first_name}"
last: "{a.output.data.last_name}"
email: "{b.output.data.email}"
age: "{b.output.data.age}"
is_employee: true
company: "ACME Corp"
bonus: 500
```

With this YAML configuration, HotMesh's data mapping solution will generate a new object C with the desired structure:

```json
{
  "first": "John",
  "last": "Doe",
  "email": "john.doe@example.com",
  "age": 30,
  "is_employee": true,
  "company": "ACME Corp",
  "bonus": 500
}
```

## Not-So-Quick Start
In some cases, you might need more complex mapping transformations than what's achievable through simple one-to-one mappings. In this section, we'll demonstrate how to use the `@pipe` syntax in HotMesh to create more sophisticated mapping rules. 

### Example 1) Ternary Syntax

HotMesh functional approach for transforming data unifies method syntax, standardizing how transformations are invoked. Although syntactic sugar is useful, it makes it difficult to reason about functional transformations given the variability of the language. For example, consider the standard JavaScript syntax for a ternary expression.

```javascript
const sound = isDog ? "bark" : "meow";
```

The functional equivalent is as follows.

```javascript
const sound = ternary(isDog, "bark", "meow");
```

### Example 2) Instance Property Syntax

Consider a second example that uses *property-based* access to return the `length` of a string.

```javascript
const length = someString.length
```

The functional approach remains syntactically consistent.

```javascript
const length = stringLength(someString);
```

### Example 3) Instance Method Syntax

Consider a third example that `joins` an array by calling an *instance method*.

```javascript
const someString = someArray.join(' ');
```

The functional approach, by design, remains syntactically consistent.

```javascript
const someString = join(someArray, ' ');
```

### Overview of `@pipe`
`@pipe` is the central mechanism for invoking functional transformations. It is executed at runtime as an array of arrays, where each row represents a transformation step. The transformed/resolved data is then used as input for the next step as necessary until all transformations have been run. This pattern works, because it executes all ECMA Script commands as functions with inputs, reducing the semantic variability of the language.

There are three types of data that can be used within `@pipe`:
1. **Static**: fixed values such as numbers, strings, and booleans.
2. **Dynamic**: values dynamically resolved from input JSON data using `{}`, e.g., `{a.output.data.temperature}`.
3. **Function**: functions for data manipulation and transformation, e.g., `{@string.split}` or `{@number.isEven}`.

### Rules for `@pipe`

1. `@pipe` must be an array, with each item in the array being an array (rows of cells).
2. As each row is processed, the values for the cells in the row are resolved (either static, dynamic, or function types).
3. The resolved cell values will serve as the input parameters for the function in cell 1 on the next row.
4. The function will then be resolved, and any following cells (whether dynamic or static) will also be resolved.
5. The resolved cell values will be passed as parameters to the next row's function in cell 1, and so on.

To better illustrate this concept, let's use a visual representation. Notice how the resolved values from a row, feed into the function in the following row as input parameters:

```
@pipe:
  ┌─────────────────────────────────┐
  │ Row 0: [param1, param2, paramN] │
  |           |       |       |     |
  |           ┌───────┘       |     |
  |           ┌───────────────┘     |
  ├───────────|─────────────────────┤
  |           v       
  │ Row 1: [function1, paramN]      │
  |           |          |          |
  |           ┌──────────┘          |
  |           |                     |
  ├───────────|─────────────────────┤
  |           v       
  │ Row 2: [function1, paramN]      │
  |           |          |          |
  |           ┌──────────┘          |
  |           |                     |
  ├───────────|─────────────────────┤
  |           v                     |
  │ ...                             │
  └─────────────────────────────────┘
```

Here's how the `@pipe` processes the rows:

1. Resolve the cells in Row 0. They could be *static* or *dynamic* values or can be a *nullary function* that expects no upstream input (like {@date.now}).
2. Pass the resolved values from Row 0 as input parameters to the *function* in cell 1 of Row 1.
3. Resolve the *function* in Row 1 with the input parameters, and resolve any following cells. The following cells could be *static* or *dynamic* values or can be a *nullary function* that expects no upstream input (like {@date.now}).
4. Pass the resolved values from Row 1 as input parameters to the *function* in cell 1 of Row 2.
5. Repeat the process until the last row is processed.

### Example 1: user_name
Let's start with an example that demonstrates how to create a `user_name` by concatenating the first character of the first name and the full last name in lowercase. For example, the user, `John Doe`, should have the user_name, `jdoe`.

Consider the `{@string.charAt}` function below. It expects two parameters: a *string* and the character *index*. This is why the row above it has two cells. These are the two input parameters. This row will now produce `J` and `Doe` which will be passed as the two input parameters to the following row, etc. The pattern suffices for any linear set of transformations.

```yaml
user_name:
  "@pipe":
    - ["{a.output.data.first_name}", 0]
    - ["{@string.charAt}", "{a.output.data.last_name}"]
    - ["{@string.concat}"]
    - ["{@string.toLowerCase}"]
```

### Example 2: status
Let's now look at another example where we determine the `status` of an employee based on their age. If the employee is 40 years old or older, their status is "Senior", otherwise, it's "Junior". Here's the YAML configuration for this transformation:

```yaml
status:
  "@pipe":
    - ["{b.output.data.age}", 40]
    - ["{@number.gte}", "Senior", "Junior"]
    - ["{@conditional.ternary}"]
```

In this example, we follow the same essential pattern as before. We first provide the age field from object B and the value 40 as inputs. Then, we call the {@number.gte} function using the inputs from row 0, and provide "Senior" and "Junior" as additional inputs. Finally, we call the {@conditional.ternary} function, which sets the status field to "Senior" if the output from row 1 is true, and "Junior" otherwise.

### Example 3: Nested Pipes
In this example, we'll demonstrate how to create a `user_initials` field by extracting the first letter of the first and last names and concatenating them. We'll use *nested pipes* as this is a non-linear mapping transformation with two parallel steps: *get the first initial*, *get the last initial*. Nested pipes support a fan-out/fan-in pattern. Each nested pipe is a fan-out, and its corresponding fan-in will be the first "standard" row that follows and isn't a pipe (in the ruleset below, the nested `pipes` represent the "fan-out", operating in parallel while `string.concat` serves as the "fan-in", combining the results as input parameters).

The `full_name` is first split into an array of first and last names using the `{@array.split}` function (note how a single space (" ") is passed as the delimiter when splitting). Next, two nested pipes are utilized to extract the first character of both the first and last names. Within each nested pipe, the `{@array.get}` function retrieves the respective name (`first` or `last`) from the array, followed by the `{@string.charAt}` function to extract the first character. Lastly, the `{@string.concat}` function concatenates the initials.

```yaml
initials:
  "@pipe":
    - ["{a2.output.data.full_name}", " "]
    - "@pipe":
      - ["{@array.split}", 0]
      - ["{@array.get}", 0]
      - ["{@string.charAt}"]
    - "@pipe":
      - ["{@array.split}", 1]
      - ["{@array.get}", 0]
      - ["{@string.charAt}"]
    - ["{@string.concat}"]
```

### Rules for Nested `@pipes`
If any row is *not an Array* and is instead an object with one field named `@pipe`, resolve the output returned from executing the `@pipe` and then pass it as input to the next row *that is an Array*. The first cell of the first row of a nested array of mappings can be a function, and if it is, it will receive any input from the closest previous sibling that is not a `@pipe`.

The above approach creates a new field, initials, containing the first and last initials of the user. Utilizing nested pipes in HotMesh enables the creation of advanced, non-linear mapping rules and the handling of a wide range of data transformation scenarios.

## Conclusion

Here is the final mapping ruleset for all fields described in this guide, including dynamic, static, and @pipe fields:

```yaml
x-maps:
  first_name:
    "@pipe":
      - ["{a.output.data.full_name}"]
      - ["{@string.split}", 0]
      - ["{@array.get}"]
  last_name:
    "@pipe":
      - ["{a.output.data.full_name}"]
      - ["{@string.split}", 1]
      - ["{@array.get}"]

  user_name:
    "@pipe":
      - ["{a.output.data.first_name}", 0]
      - ["{@string.charAt}", "{a.output.data.last_name}"]
      - ["{@string.concat}"]
      - ["{@string.toLowerCase}"]
  status:
    "@pipe":
      - ["{b.output.data.age}", 40]
      - ["{@number.gte}", "Senior", "Junior"]
      - ["{@conditional.ternary}"]
  initials:
    "@pipe":
      - ["{a2.output.data.full_name}", " "]
      - "@pipe":
        - ["{@array.split}", 0]
        - ["{@array.get}", 0]
        - ["{@string.charAt}"]
      - "@pipe":
        - ["{@array.split}", 1]
        - ["{@array.get}", 0]
        - ["{@string.charAt}"]
      - ["{@string.concat}"]
```
