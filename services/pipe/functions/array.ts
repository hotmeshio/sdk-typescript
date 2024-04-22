class ArrayHandler {
  get(array: any[], index: number): any {
    return array?.[index || 0];
  }

  length(array: any[]): any {
    return array?.length;
  }

  concat(array1: any[], array2: any[]): any[] {
    return array1.concat(array2);
  }

  indexOf(array: any[], searchElement: any, fromIndex?: number): number {
    return array.indexOf(searchElement, fromIndex);
  }

  join(array: any[], separator: string): string {
    return array.join(separator);
  }

  lastIndexOf(array: any[], searchElement: any, fromIndex?: number): number {
    return array.lastIndexOf(searchElement, fromIndex);
  }

  pop(array: any[]): any {
    return array.pop();
  }

  push(array: any[], ...items: any[]): any[] {
    array.push(...items);
    return array;
  }

  reverse(array: any[]): any[] {
    return array.reverse();
  }

  shift(array: any[]): any {
    return array.shift();
  }

  slice(array: any[], start?: number, end?: number): any[] {
    return array.slice(start, end);
  }

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

  splice(array: any[], start: number, deleteCount?: number, ...items: any[]): any[] {
    return array.splice(start, deleteCount, ...items);
  }

  unshift(array: any[], ...items: any[]): number {
    return array.unshift(...items);
  }
}

export { ArrayHandler };
