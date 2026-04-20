/**
 * Provides functional transformations for arrays within HotMesh mapping
 * rules. Although inspired by JavaScript, these methods have been adapted
 * to follow a functional approach. Each transformation is a function that
 * expects one or more input parameters from the prior row in the `@pipe`
 * structure.
 *
 * @remarks Invoked via `{@array.<method>}` in YAML mapping rules.
 */
class ArrayHandler {
  /**
   * Retrieves an element from an array by its index.
   *
   * @param {any[]} array - The array from which to retrieve the element
   * @param {number} index - The zero-based index of the element to retrieve
   * @returns {any} The element at the specified index, or undefined if the array is nullish
   * @example
   * ```yaml
   * second_color:
   *   "@pipe":
   *     - ["{a.output.data.colors}", 1]
   *     - ["{@array.get}"]
   * ```
   */
  get(array: any[], index: number): any {
    return array?.[index || 0];
  }

  /**
   * Returns the number of elements in an array.
   *
   * @param {any[]} array - The array whose length is to be determined
   * @returns {any} The number of elements in the array
   * @example
   * ```yaml
   * num_colors:
   *   "@pipe":
   *     - ["{a.output.data.colors}"]
   *     - ["{@array.length}"]
   * ```
   */
  length(array: any[]): any {
    return array?.length;
  }

  /**
   * Concatenates two arrays and returns a new array containing the
   * elements of both.
   *
   * @param {any[]} array1 - The first array
   * @param {any[]} array2 - The second array to concatenate
   * @returns {any[]} A new array containing elements from both input arrays
   * @example
   * ```yaml
   * concatenated_colors:
   *   "@pipe":
   *     - ["{a.output.data.colors1}", "{a.output.data.colors2}"]
   *     - ["{@array.concat}"]
   * ```
   */
  concat(array1: any[], array2: any[]): any[] {
    return array1.concat(array2);
  }

  /**
   * Returns the first index at which a given element can be found in
   * the array, or -1 if the element is not present.
   *
   * @param {any[]} array - The array to search
   * @param {any} searchElement - The element to locate in the array
   * @param {number} [fromIndex] - The index to start the search from
   * @returns {number} The first index of the element, or -1 if not found
   * @example
   * ```yaml
   * green_index:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "green"]
   *     - ["{@array.indexOf}"]
   * ```
   */
  indexOf(array: any[], searchElement: any, fromIndex?: number): number {
    return array.indexOf(searchElement, fromIndex);
  }

  /**
   * Joins all elements of an array into a string, using a specified
   * delimiter between each element.
   *
   * @param {any[]} array - The array whose elements are to be joined
   * @param {string} separator - The string used to separate each element
   * @returns {string} A string with all array elements joined by the separator
   * @example
   * ```yaml
   * sentence:
   *   "@pipe":
   *     - ["{a.output.data.words}", " "]
   *     - ["{@array.join}"]
   * ```
   */
  join(array: any[], separator: string): string {
    return array.join(separator);
  }

  /**
   * Returns the last index at which a given element can be found in
   * the array, or -1 if it is not present. The array is searched
   * backwards.
   *
   * @param {any[]} array - The array to search
   * @param {any} searchElement - The element to locate in the array
   * @param {number} [fromIndex] - The index at which to start searching backwards
   * @returns {number} The last index of the element, or -1 if not found
   * @example
   * ```yaml
   * last_green_index:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "green"]
   *     - ["{@array.lastIndexOf}"]
   * ```
   */
  lastIndexOf(array: any[], searchElement: any, fromIndex?: number): number {
    return array.lastIndexOf(searchElement, fromIndex);
  }

  /**
   * Removes the last element from an array and returns that element.
   *
   * @param {any[]} array - The array from which to remove the last element
   * @returns {any} The removed element, or undefined if the array is empty
   * @example
   * ```yaml
   * last_color:
   *   "@pipe":
   *     - ["{a.output.data.colors}"]
   *     - ["{@array.pop}"]
   * ```
   */
  pop(array: any[]): any {
    return array.pop();
  }

  /**
   * Adds one or more elements to the end of an array and returns the
   * modified array.
   *
   * @param {any[]} array - The array to add elements to
   * @param {...any[]} items - The elements to add to the end of the array
   * @returns {any[]} The modified array with the new elements appended
   * @example
   * ```yaml
   * updated_colors:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "yellow"]
   *     - ["{@array.push}"]
   * ```
   */
  push(array: any[], ...items: any[]): any[] {
    array.push(...items);
    return array;
  }

  /**
   * Reverses the order of the elements in an array in place and returns
   * the reversed array.
   *
   * @param {any[]} array - The array to reverse
   * @returns {any[]} The reversed array
   * @example
   * ```yaml
   * reversed_numbers:
   *   "@pipe":
   *     - ["{a.output.data.numbers}"]
   *     - ["{@array.reverse}"]
   * ```
   */
  reverse(array: any[]): any[] {
    return array.reverse();
  }

  /**
   * Removes the first element from an array and returns that element.
   *
   * @param {any[]} array - The array from which to remove the first element
   * @returns {any} The removed element, or undefined if the array is empty
   * @example
   * ```yaml
   * first_color:
   *   "@pipe":
   *     - ["{a.output.data.colors}"]
   *     - ["{@array.shift}"]
   * ```
   */
  shift(array: any[]): any {
    return array.shift();
  }

  /**
   * Returns a shallow copy of a portion of an array selected from the
   * start index up to, but not including, the end index.
   *
   * @param {any[]} array - The array to slice
   * @param {number} [start] - The beginning index of the slice (inclusive)
   * @param {number} [end] - The end index of the slice (exclusive); if omitted, slices to the end
   * @returns {any[]} A new array containing the extracted elements
   * @example
   * ```yaml
   * numbers_slice:
   *   "@pipe":
   *     - ["{a.output.data.numbers}", 1, 3]
   *     - ["{@array.slice}"]
   * ```
   */
  slice(array: any[], start?: number, end?: number): any[] {
    return array.slice(start, end);
  }

  /**
   * Sorts the elements of an array in place and returns the sorted array.
   * Supports ascending (default) and descending sort orders. Handles
   * null/undefined values and uses locale-aware comparison for strings.
   *
   * @param {any[]} array - The array to sort
   * @param {'ASCENDING' | 'DESCENDING'} [order='ASCENDING'] - The sort order
   * @returns {any[]} The sorted array
   * @example
   * ```yaml
   * sorted_numbers:
   *   "@pipe":
   *     - ["{a.output.data.numbers}"]
   *     - ["{@array.sort}"]
   * ```
   */
  sort(array: any[], order: 'ASCENDING' | 'DESCENDING' = 'ASCENDING'): any[] {
    return array.sort((a, b) => {
      if (order === 'ASCENDING') {
        if (a === b) return 0;
        if (a === null || a === undefined) return -1;
        if (b === null || b === undefined) return 1;
        if (typeof a === 'string' && typeof b === 'string') {
          return a.localeCompare(b);
        }
        return a < b ? -1 : 1;
      } else {
        if (a === b) return 0;
        if (a === null || a === undefined) return 1;
        if (b === null || b === undefined) return -1;
        if (typeof a === 'string' && typeof b === 'string') {
          return b.localeCompare(a);
        }
        return a > b ? -1 : 1;
      }
    });
  }

  /**
   * Changes the contents of an array by removing or replacing existing
   * elements and/or adding new elements in place. Returns the array
   * of removed elements.
   *
   * @param {any[]} array - The array to modify
   * @param {number} start - The index at which to start changing the array
   * @param {number} [deleteCount] - The number of elements to remove
   * @param {...any[]} items - The elements to add at the start position
   * @returns {any[]} An array containing the removed elements
   * @example
   * ```yaml
   * modified_colors:
   *   "@pipe":
   *     - ["{a.output.data.colors}", 1, 2, "orange"]
   *     - ["{@array.splice}"]
   * ```
   */
  splice(
    array: any[],
    start: number,
    deleteCount?: number,
    ...items: any[]
  ): any[] {
    return array.splice(start, deleteCount, ...items);
  }

  /**
   * Adds one or more elements to the beginning of an array and returns
   * the new length of the array.
   *
   * @param {any[]} array - The array to add elements to
   * @param {...any[]} items - The elements to add to the front of the array
   * @returns {number} The new length of the array
   * @example
   * ```yaml
   * updated_colors:
   *   "@pipe":
   *     - ["{a.output.data.colors}", "yellow"]
   *     - ["{@array.unshift}"]
   * ```
   */
  unshift(array: any[], ...items: any[]): number {
    return array.unshift(...items);
  }
}

export { ArrayHandler };
