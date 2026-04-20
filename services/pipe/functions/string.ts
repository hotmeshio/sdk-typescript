/**
 * Provides string manipulation functions for use in HotMesh mapping rules.
 * Although inspired by JavaScript's String API, these methods follow a
 * functional approach where each transformation expects one or more input
 * parameters from the prior row in the @pipe structure.
 *
 * @remarks Methods are invoked with the syntax `{@string.<method>}`, e.g., `{@string.trim}` or `{@string.toUpperCase}`.
 */
class StringHandler {
  /**
   * Splits a string into an array of substrings based on a specified
   * delimiter.
   *
   * @param {string} input - The string to split.
   * @param {string} delimiter - The delimiter to split on.
   * @returns {string[]} An array of substrings.
   * @example
   * ```yaml
   * words:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", " "]
   *     - ["{@string.split}"]
   * ```
   */
  split(input: string, delimiter: string): string[] {
    return input.split(delimiter);
  }

  /**
   * Retrieves the character at a specified index in a string.
   *
   * @param {string} input - The string to retrieve the character from.
   * @param {number} index - The zero-based index of the character.
   * @returns {string} The character at the specified index.
   * @example
   * ```yaml
   * first_letter:
   *   "@pipe":
   *     - ["{a.output.data.word}", 0]
   *     - ["{@string.charAt}"]
   * ```
   */
  charAt(input: string, index: number): string {
    return input.charAt(index);
  }

  /**
   * Concatenates two or more strings into a single string.
   *
   * @param {...string[]} strings - The strings to concatenate.
   * @returns {string} The concatenated result.
   * @example
   * ```yaml
   * full_name:
   *   "@pipe":
   *     - ["{a.output.data.first_name}", " ", "{a.output.data.last_name}"]
   *     - ["{@string.concat}"]
   * ```
   */
  concat(...strings: string[]): string {
    return strings.join('');
  }

  /**
   * Checks if a string contains a specified substring, optionally starting
   * the search at a given position.
   *
   * @param {string} input - The string to search within.
   * @param {string} searchString - The substring to search for.
   * @param {number} [position] - Optional position to start searching from.
   * @returns {boolean} `true` if the substring is found, `false` otherwise.
   * @example
   * ```yaml
   * contains_fox:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", "fox"]
   *     - ["{@string.includes}"]
   * ```
   */
  includes(input: string, searchString: string, position?: number): boolean {
    return input.includes(searchString, position);
  }

  /**
   * Returns the index of the first occurrence of a specified substring in a
   * string, or -1 if not found.
   *
   * @param {string} input - The string to search within.
   * @param {string} searchString - The substring to search for.
   * @param {number} [fromIndex] - Optional index to start searching from.
   * @returns {number} The index of the first occurrence, or -1.
   * @example
   * ```yaml
   * fox_index:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", "fox"]
   *     - ["{@string.indexOf}"]
   * ```
   */
  indexOf(input: string, searchString: string, fromIndex?: number): number {
    return input.indexOf(searchString, fromIndex);
  }

  /**
   * Returns the index of the last occurrence of a specified substring in a
   * string, or -1 if not found.
   *
   * @param {string} input - The string to search within.
   * @param {string} searchString - The substring to search for.
   * @param {number} [fromIndex] - Optional index to start searching backward from.
   * @returns {number} The index of the last occurrence, or -1.
   * @example
   * ```yaml
   * last_quick_index:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", "quick"]
   *     - ["{@string.lastIndexOf}"]
   * ```
   */
  lastIndexOf(input: string, searchString: string, fromIndex?: number): number {
    return input.lastIndexOf(searchString, fromIndex);
  }

  /**
   * Extracts a section of a string and returns it as a new string, without
   * modifying the original.
   *
   * @param {string} input - The string to extract from.
   * @param {number} [start] - The zero-based start index (inclusive).
   * @param {number} [end] - The zero-based end index (exclusive).
   * @returns {string} The extracted substring.
   * @example
   * ```yaml
   * substring:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", 4, 9]
   *     - ["{@string.slice}"]
   * ```
   */
  slice(input: string, start?: number, end?: number): string {
    return input.slice(start, end);
  }

  /**
   * Converts all characters in a string to lowercase.
   *
   * @param {string} input - The string to convert.
   * @returns {string} The lowercase string.
   * @example
   * ```yaml
   * lowercase_sentence:
   *   "@pipe":
   *     - ["{a.output.data.sentence}"]
   *     - ["{@string.toLowerCase}"]
   * ```
   */
  toLowerCase(input: string): string {
    return input.toLowerCase();
  }

  /**
   * Converts all characters in a string to uppercase.
   *
   * @param {string} input - The string to convert.
   * @returns {string} The uppercase string.
   * @example
   * ```yaml
   * uppercase_sentence:
   *   "@pipe":
   *     - ["{a.output.data.sentence}"]
   *     - ["{@string.toUpperCase}"]
   * ```
   */
  toUpperCase(input: string): string {
    return input.toUpperCase();
  }

  /**
   * Removes whitespace from both ends of a string.
   *
   * @param {string} input - The string to trim.
   * @returns {string} The trimmed string.
   * @example
   * ```yaml
   * trimmed_text:
   *   "@pipe":
   *     - ["{a.output.data.text}"]
   *     - ["{@string.trim}"]
   * ```
   */
  trim(input: string): string {
    return input.trim();
  }

  /**
   * Removes whitespace from the beginning of a string.
   *
   * @param {string} input - The string to trim.
   * @returns {string} The string with leading whitespace removed.
   * @example
   * ```yaml
   * trimmed_start_text:
   *   "@pipe":
   *     - ["{a.output.data.text}"]
   *     - ["{@string.trimStart}"]
   * ```
   */
  trimStart(input: string): string {
    return input.trimStart();
  }

  /**
   * Removes whitespace from the end of a string.
   *
   * @param {string} input - The string to trim.
   * @returns {string} The string with trailing whitespace removed.
   * @example
   * ```yaml
   * trimmed_end_text:
   *   "@pipe":
   *     - ["{a.output.data.text}"]
   *     - ["{@string.trimEnd}"]
   * ```
   */
  trimEnd(input: string): string {
    return input.trimEnd();
  }

  /**
   * Pads the start of a string with a given pad string (repeated if needed)
   * until the resulting string reaches the specified maximum length.
   *
   * @param {string} input - The string to pad.
   * @param {number} maxLength - The target length of the resulting string.
   * @param {string} [padString] - The string to pad with (defaults to spaces).
   * @returns {string} The padded string.
   * @example
   * ```yaml
   * padded:
   *   "@pipe":
   *     - ["{a.output.data.value}", 5, "0"]
   *     - ["{@string.padStart}"]
   * ```
   */
  padStart(input: string, maxLength: number, padString?: string): string {
    return input.padStart(maxLength, padString);
  }

  /**
   * Pads the end of a string with a given pad string (repeated if needed)
   * until the resulting string reaches the specified maximum length.
   *
   * @param {string} input - The string to pad.
   * @param {number} maxLength - The target length of the resulting string.
   * @param {string} [padString] - The string to pad with (defaults to spaces).
   * @returns {string} The padded string.
   * @example
   * ```yaml
   * padded:
   *   "@pipe":
   *     - ["{a.output.data.value}", 10, "."]
   *     - ["{@string.padEnd}"]
   * ```
   */
  padEnd(input: string, maxLength: number, padString?: string): string {
    return input.padEnd(maxLength, padString);
  }

  /**
   * Replaces the first occurrence of a search value (string or RegExp) in a
   * string with a replacement string.
   *
   * @param {string} input - The string to search within.
   * @param {string | RegExp} searchValue - The value to search for.
   * @param {string} replaceValue - The replacement string.
   * @returns {string} The string with the first match replaced.
   * @example
   * ```yaml
   * replaced:
   *   "@pipe":
   *     - ["{a.output.data.text}", "old", "new"]
   *     - ["{@string.replace}"]
   * ```
   */
  replace(
    input: string,
    searchValue: string | RegExp,
    replaceValue: string,
  ): string {
    return input.replace(searchValue, replaceValue);
  }

  /**
   * Searches a string for a match against a regular expression and returns
   * the index of the first match, or -1 if no match is found.
   *
   * @param {string} input - The string to search within.
   * @param {RegExp} regexp - The regular expression to search for.
   * @returns {number} The index of the first match, or -1.
   * @example
   * ```yaml
   * match_index:
   *   "@pipe":
   *     - ["{a.output.data.text}", "/\\d+/"]
   *     - ["{@string.search}"]
   * ```
   */
  search(input: string, regexp: RegExp): number {
    return input.search(regexp);
  }

  /**
   * Returns a portion of a string between the specified start and end
   * indices. Unlike `slice`, negative indices are treated as 0 and arguments
   * are swapped if start is greater than end.
   *
   * @param {string} input - The string to extract from.
   * @param {number} start - The start index (inclusive).
   * @param {number} [end] - The end index (exclusive).
   * @returns {string} The extracted substring.
   * @example
   * ```yaml
   * substring:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", 4, 9]
   *     - ["{@string.substring}"]
   * ```
   */
  substring(input: string, start: number, end?: number): string {
    return input.substring(start, end);
  }

  /**
   * Determines whether a string begins with the characters of a specified
   * string, optionally starting the check at a given position.
   *
   * @param {string} str - The string to check.
   * @param {string} searchString - The characters to search for at the start.
   * @param {number} [position] - Optional position to begin searching from.
   * @returns {boolean} `true` if the string starts with the search string, `false` otherwise.
   * @example
   * ```yaml
   * starts_with_the:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", "The"]
   *     - ["{@string.startsWith}"]
   * ```
   */
  startsWith(str: string, searchString: string, position?: number): boolean {
    return str.startsWith(searchString, position);
  }

  /**
   * Determines whether a string ends with the characters of a specified
   * string, optionally limiting the check to the first N characters.
   *
   * @param {string} str - The string to check.
   * @param {string} searchString - The characters to search for at the end.
   * @param {number} [length] - Optional length of the string to consider.
   * @returns {boolean} `true` if the string ends with the search string, `false` otherwise.
   * @example
   * ```yaml
   * ends_with_dot:
   *   "@pipe":
   *     - ["{a.output.data.sentence}", "."]
   *     - ["{@string.endsWith}"]
   * ```
   */
  endsWith(str: string, searchString: string, length?: number): boolean {
    return str.endsWith(searchString, length);
  }

  /**
   * Creates a new string by repeating the given string a specified number of
   * times. The count must be a non-negative finite number.
   *
   * @param {string} str - The string to repeat.
   * @param {number} count - The number of times to repeat (must be >= 0 and finite).
   * @returns {string} The repeated string.
   * @example
   * ```yaml
   * repeated_text:
   *   "@pipe":
   *     - ["{a.output.data.text}", 3]
   *     - ["{@string.repeat}"]
   * ```
   */
  repeat(str: string, count: number): string {
    if (count < 0 || count === Infinity) {
      throw new RangeError(
        'Invalid repeat count. Must be a positive finite number.',
      );
    }
    return str.repeat(count);
  }
}

export { StringHandler };
