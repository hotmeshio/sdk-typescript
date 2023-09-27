class StringHandler {
  split(input: string, delimiter: string): string[] {
    return input.split(delimiter);
  }

  charAt(input: string, index: number): string {
    return input.charAt(index);
  }

  concat(...strings: string[]): string {
    return strings.join('');
  }

  includes(input: string, searchString: string, position?: number): boolean {
    return input.includes(searchString, position);
  }

  indexOf(input: string, searchString: string, fromIndex?: number): number {
    return input.indexOf(searchString, fromIndex);
  }

  lastIndexOf(input: string, searchString: string, fromIndex?: number): number {
    return input.lastIndexOf(searchString, fromIndex);
  }

  slice(input: string, start?: number, end?: number): string {
    return input.slice(start, end);
  }

  toLowerCase(input: string): string {
    return input.toLowerCase();
  }

  toUpperCase(input: string): string {
    return input.toUpperCase();
  }

  trim(input: string): string {
    return input.trim();
  }

  trimStart(input: string): string {
    return input.trimStart();
  }

  trimEnd(input: string): string {
    return input.trimEnd();
  }

  padStart(input: string, maxLength: number, padString?: string): string {
    return input.padStart(maxLength, padString);
  }

  padEnd(input: string, maxLength: number, padString?: string): string {
    return input.padEnd(maxLength, padString);
  }

  replace(input: string, searchValue: string | RegExp, replaceValue: string): string {
    return input.replace(searchValue, replaceValue);
  }

  search(input: string, regexp: RegExp): number {
    return input.search(regexp);
  }

  substring(input: string, start: number, end?: number): string {
    return input.substring(start, end);
  }

  startsWith(str: string, searchString: string, position?: number): boolean {
    return str.startsWith(searchString, position);
  }

  endsWith(str: string, searchString: string, length?: number): boolean {
    return str.endsWith(searchString, length);
  }

  repeat(str: string, count: number): string {
    if (count < 0 || count === Infinity) {
      throw new RangeError('Invalid repeat count. Must be a positive finite number.');
    }
    return str.repeat(count);
  }
}
  
export { StringHandler };
