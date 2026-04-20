/**
 * Provides functional transformations for JavaScript objects within
 * HotMesh mapping rules. Although inspired by JavaScript, these methods
 * have been adapted to follow a functional approach. Each transformation
 * is a function that expects one or more input parameters from the prior
 * row in the `@pipe` structure.
 *
 * @remarks Invoked via {@object.<method>} in YAML mapping rules.
 */
class ObjectHandler {
  /**
   * Retrieves a property value from an object by its property name.
   *
   * @param {object} obj - The object from which to retrieve the property value
   * @param {string | symbol} prop - The name of the property to retrieve
   * @returns {any} The value of the specified property, or undefined if the object is nullish
   * @example
   * ```yaml
   * second_color:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "second"]
   *     - ["{@object.get}"]
   * ```
   */
  get(obj: object, prop: string | symbol): any {
    return obj?.[prop];
  }

  /**
   * Sets a property on an object with a specified value and returns the
   * object. If the input object is `undefined` or `null`, a new object
   * will be created.
   *
   * @param {object} obj - The object to set the property on
   * @param {string | symbol} prop - The name of the property to set
   * @param {any} value - The value to assign to the property
   * @returns {any} The modified object with the new property value
   * @example
   * ```yaml
   * output:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "third", "blue"]
   *     - ["{@object.set}"]
   * ```
   */
  set(obj: object, prop: string | symbol, value: any): any {
    if (!obj) obj = {};
    obj[prop] = value;
    return obj;
  }

  /**
   * Creates a new object with specified key-value pairs. Arguments are
   * provided as alternating key-value pairs. If no arguments are provided,
   * an empty object will be created.
   *
   * @param {...any[]} args - Alternating key-value pairs (key1, value1, key2, value2, ...)
   * @returns {object} A new object constructed from the provided key-value pairs
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["name", "John", "age", 30, "city", "New York"]
   *     - ["{@object.create}"]
   * ```
   */
  create(...args: any[]): object {
    const obj = {};
    if (args.length === 0) return obj;
    for (let i = 0; i < args.length; i += 2) {
      obj[args[i]] = args[i + 1];
    }
    return obj;
  }

  /**
   * Retrieves an array of a given object's own enumerable property names.
   *
   * @param {object} obj - The object whose property names are to be retrieved
   * @returns {string[]} An array of the object's own enumerable property names
   * @example
   * ```yaml
   * property_names:
   *   "@pipe":
   *     - ["{a.output.data}"]
   *     - ["{@object.keys}"]
   * ```
   */
  keys(obj: object): string[] {
    return obj && Object.keys(obj) || [];
  }

  /**
   * Retrieves an array of a given object's own enumerable property values.
   *
   * @param {object} obj - The object from which to extract the values
   * @returns {any[]} An array of the object's own enumerable property values
   * @example
   * ```yaml
   * color_values:
   *   "@pipe":
   *     - ["{a.output.data.colors}"]
   *     - ["{@object.values}"]
   * ```
   */
  values(obj: object): any[] {
    return obj && Object.values(obj) || [];
  }

  /**
   * Retrieves an array of the object's own enumerable key-value pairs.
   *
   * @param {object} obj - The object from which to extract the entries
   * @returns {[string, any][]} An array of [key, value] pairs
   * @example
   * ```yaml
   * color_entries:
   *   "@pipe":
   *     - ["{a.output.data.colors}"]
   *     - ["{@object.entries}"]
   * ```
   */
  entries(obj: object): [string, any][] {
    return obj && Object.entries(obj) || [];
  }

  /**
   * Transforms an array of key-value pairs into an object.
   *
   * @param {Iterable<[string, any]>} iterable - An iterable of key-value pairs (e.g., an array of [key, value] arrays)
   * @returns {object} A new object constructed from the key-value pairs
   * @example
   * ```yaml
   * color_object:
   *   "@pipe":
   *     - ["{a.output.data.pairs}"]
   *     - ["{@object.fromEntries}"]
   * ```
   */
  fromEntries(iterable: Iterable<[string, any]>): object {
    return Object.fromEntries(iterable);
  }

  /**
   * Merges one or more source objects into a target object. Properties
   * in later sources overwrite earlier ones.
   *
   * @param {object} target - The target object to merge into
   * @param {...object[]} sources - One or more source objects to merge from
   * @returns {object} The target object with all source properties merged in
   * @example
   * ```yaml
   * combined_colors:
   *   "@pipe":
   *     - ["{@symbol.object}", "{a.output.data.colors}", "{b.output.data.colors}"]
   *     - ["{@object.assign}"]
   * ```
   */
  assign(target: object, ...sources: object[]): object {
    return Object.assign(target || {}, ...sources);
  }

  /**
   * Returns an array of all property names (including non-enumerable) of
   * an object.
   *
   * @param {object} obj - The object to retrieve property names from
   * @returns {string[]} An array of all own property names
   * @example
   * ```yaml
   * propertyNames:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.getOwnPropertyNames}"]
   * ```
   */
  getOwnPropertyNames(obj: object): string[] {
    return Object.getOwnPropertyNames(obj || {});
  }

  /**
   * Returns an array of all symbol properties of an object.
   *
   * @param {object} obj - The object to retrieve symbol properties from
   * @returns {symbol[]} An array of all own symbol properties
   * @example
   * ```yaml
   * symbolProperties:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.getOwnPropertySymbols}"]
   * ```
   */
  getOwnPropertySymbols(obj: object): symbol[] {
    return Object.getOwnPropertySymbols(obj || {});
  }

  /**
   * Returns a property descriptor for an own property of an object.
   *
   * @param {object} obj - The object to retrieve the property descriptor from
   * @param {string | symbol} prop - The name of the property
   * @returns {PropertyDescriptor | undefined} The property descriptor, or undefined if the property does not exist
   * @example
   * ```yaml
   * ageDescriptor:
   *   "@pipe":
   *     - ["{a.person}", "age"]
   *     - ["{@object.getOwnPropertyDescriptor}"]
   * ```
   */
  getOwnPropertyDescriptor(
    obj: object,
    prop: string | symbol,
  ): PropertyDescriptor | undefined {
    return Object.getOwnPropertyDescriptor(obj || {}, prop);
  }

  /**
   * Defines a new property or modifies an existing property on an object
   * and returns the object.
   *
   * @param {object} obj - The object to define the property on
   * @param {string | symbol} prop - The name of the property to define or modify
   * @param {PropertyDescriptor} descriptor - The descriptor for the property being defined or modified
   * @returns {object} The object with the defined property
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["{a.person}", "city", {"value": "New York", "writable": false}]
   *     - ["{@object.defineProperty}"]
   * ```
   */
  defineProperty(
    obj: object,
    prop: string | symbol,
    descriptor: PropertyDescriptor,
  ): object {
    return Object.defineProperty(obj, prop, descriptor);
  }

  /**
   * Defines new properties or modifies existing properties on an object
   * and returns the object.
   *
   * @param {object} obj - The object to define properties on
   * @param {PropertyDescriptorMap} props - An object whose keys represent property names and whose values are property descriptors
   * @returns {object} The object with the defined properties
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["{a.person}", {"age": {"value": 30}, "city": {"value": "New York", "writable": false}}]
   *     - ["{@object.defineProperties}"]
   * ```
   */
  defineProperties(obj: object, props: PropertyDescriptorMap): object {
    return Object.defineProperties(obj, props);
  }

  /**
   * Freezes an object, making it immutable. Prevents new properties from
   * being added, existing properties from being removed, and values of
   * existing properties from being modified.
   *
   * @param {object} obj - The object to freeze
   * @returns {object} The frozen object
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.freeze}"]
   * ```
   */
  freeze(obj: object): object {
    return Object.freeze(obj);
  }

  /**
   * Determines if an object is frozen.
   *
   * @param {object} obj - The object to check
   * @returns {boolean} True if the object is frozen, otherwise false
   * @example
   * ```yaml
   * isFrozen:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.isFrozen}"]
   * ```
   */
  isFrozen(obj: object): boolean {
    return Object.isFrozen(obj);
  }

  /**
   * Seals an object, preventing new properties from being added and
   * marking all existing properties as non-configurable. Values of
   * existing properties can still be modified.
   *
   * @param {object} obj - The object to seal
   * @returns {object} The sealed object
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.seal}"]
   * ```
   */
  seal(obj: object): object {
    return Object.seal(obj);
  }

  /**
   * Determines if an object is sealed.
   *
   * @param {object} obj - The object to check
   * @returns {boolean} True if the object is sealed, otherwise false
   * @example
   * ```yaml
   * isSealed:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.isSealed}"]
   * ```
   */
  isSealed(obj: object): boolean {
    return Object.isSealed(obj);
  }

  /**
   * Prevents new properties from being added to an object. Existing
   * properties can still be modified or deleted.
   *
   * @param {object} obj - The object to make non-extensible
   * @returns {object} The non-extensible object
   * @example
   * ```yaml
   * person:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.preventExtensions}"]
   * ```
   */
  preventExtensions(obj: object): object {
    return Object.preventExtensions(obj);
  }

  /**
   * Determines if an object is extensible (i.e., whether new properties
   * can be added to it).
   *
   * @param {object} obj - The object to check
   * @returns {boolean} True if the object is extensible, otherwise false
   * @example
   * ```yaml
   * isExtensible:
   *   "@pipe":
   *     - ["{a.person}"]
   *     - ["{@object.isExtensible}"]
   * ```
   */
  isExtensible(obj: object): boolean {
    return Object.isExtensible(obj);
  }

  /**
   * Determines if an object has a specified property as its own property
   * (as opposed to inheriting it from the prototype chain).
   *
   * @param {object} obj - The object to check
   * @param {string | symbol} prop - The name of the property to test
   * @returns {boolean} True if the object has the specified own property, otherwise false
   * @example
   * ```yaml
   * hasAge:
   *   "@pipe":
   *     - ["{a.person}", "age"]
   *     - ["{@object.hasOwnProperty}"]
   * ```
   */
  hasOwnProperty(obj: object, prop: string | symbol): boolean {
    return Object.prototype.hasOwnProperty.call(obj, prop);
  }

  /**
   * Determines if an object exists in another object's prototype chain.
   *
   * @param {object} obj - The object whose prototype chain is to be checked
   * @param {object} prototypeObj - The prototype object to search for
   * @returns {boolean} True if the prototype object is found in the object's prototype chain, otherwise false
   * @example
   * ```yaml
   * isPersonPrototypeOfEmployee:
   *   "@pipe":
   *     - ["{a.person}", "{a.employee}"]
   *     - ["{@object.isPrototypeOf}"]
   * ```
   */
  isPrototypeOf(obj: object, prototypeObj: object): boolean {
    return Object.prototype.isPrototypeOf.call(obj, prototypeObj);
  }

  /**
   * Determines if a specified property on an object is enumerable.
   *
   * @param {object} obj - The object to check
   * @param {string | symbol} prop - The name of the property to test
   * @returns {boolean} True if the specified property is enumerable, otherwise false
   * @example
   * ```yaml
   * isAgeEnumerable:
   *   "@pipe":
   *     - ["{a.person}", "age"]
   *     - ["{@object.propertyIsEnumerable}"]
   * ```
   */
  propertyIsEnumerable(obj: object, prop: string | symbol): boolean {
    return Object.prototype.propertyIsEnumerable.call(obj, prop);
  }
}

export { ObjectHandler };
