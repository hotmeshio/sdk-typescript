class JsonHandler {
  stringify(value: any, replacer?: (this: any, key: string, value: any) => any | (number | string)[] | null, space?: string | number): string {
    return JSON.stringify(value, replacer, space);
  }

  parse(text: string, reviver?: (this: any, key: string, value: any) => any): any {
    return JSON.parse(text, reviver);
  }
}

export { JsonHandler };
