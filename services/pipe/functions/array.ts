class ArrayHandler {
  get(array: any[], index: number): any {
    return array[index];
  }

  length(array: any[]): any {
    return array?.length;
  }

  concat(array1: any[], array2: any[]): any[] {
    return array1.concat(array2);
  }

  every(array: any[], callback: (value: any, index: number, array: any[]) => boolean): boolean {
    return array.every(callback);
  }

  filter(array: any[], callback: (value: any, index: number, array: any[]) => boolean): any[] {
    return array.filter(callback);
  }

  find(array: any[], callback: (value: any, index: number, array: any[]) => boolean): any {
    return array.find(callback);
  }

  findIndex(array: any[], callback: (value: any, index: number, array: any[]) => boolean): number {
    return array.findIndex(callback);
  }

  forEach(array: any[], callback: (value: any, index: number, array: any[]) => void): void {
    array.forEach(callback);
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

  map(array: any[], callback: (value: any, index: number, array: any[]) => any): any[] {
    return array.map(callback);
  }

  pop(array: any[]): any {
    return array.pop();
  }

  push(array: any[], ...items: any[]): number {
    return array.push(...items);
  }

  reduce(array: any[], callback: (accumulator: any, currentValue: any, currentIndex: number, array: any[]) => any, initialValue?: any): any {
    return array.reduce(callback, initialValue);
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

  some(array: any[], callback: (value: any, index: number, array: any[]) => boolean): boolean {
    return array.some(callback);
  }

  sort(array: any[], compareFunction?: (a: any, b: any) => number): any[] {
    return array.sort(compareFunction);
  }

  splice(array: any[], start: number, deleteCount?: number, ...items: any[]): any[] {
    return array.splice(start, deleteCount, ...items);
  }

  unshift(array: any[], ...items: any[]): number {
    return array.unshift(...items);
  }
}

export { ArrayHandler };
