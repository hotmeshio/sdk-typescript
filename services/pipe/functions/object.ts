class ObjectHandler {
  keys(obj: object): string[] {
    return Object.keys(obj);
  }

  values(obj: object): any[] {
    return Object.values(obj);
  }

  entries(obj: object): [string, any][] {
    return Object.entries(obj);
  }

  fromEntries(iterable: Iterable<[string, any]>): object {
    return Object.fromEntries(iterable);
  }

  assign(target: object, ...sources: object[]): object {
    return Object.assign(target, ...sources);
  }

  getOwnPropertyNames(obj: object): string[] {
    return Object.getOwnPropertyNames(obj);
  }

  getOwnPropertySymbols(obj: object): symbol[] {
    return Object.getOwnPropertySymbols(obj);
  }

  getOwnPropertyDescriptor(obj: object, prop: string | symbol): PropertyDescriptor | undefined {
    return Object.getOwnPropertyDescriptor(obj, prop);
  }

  defineProperty(obj: object, prop: string | symbol, descriptor: PropertyDescriptor): object {
    return Object.defineProperty(obj, prop, descriptor);
  }

  defineProperties(obj: object, props: PropertyDescriptorMap): object {
    return Object.defineProperties(obj, props);
  }

  freeze(obj: object): object {
    return Object.freeze(obj);
  }

  isFrozen(obj: object): boolean {
    return Object.isFrozen(obj);
  }

  seal(obj: object): object {
    return Object.seal(obj);
  }

  isSealed(obj: object): boolean {
    return Object.isSealed(obj);
  }

  preventExtensions(obj: object): object {
    return Object.preventExtensions(obj);
  }

  isExtensible(obj: object): boolean {
    return Object.isExtensible(obj);
  }

  hasOwnProperty(obj: object, prop: string | symbol): boolean {
    return Object.prototype.hasOwnProperty.call(obj, prop);
  }

  isPrototypeOf(obj: object, prototypeObj: object): boolean {
    return Object.prototype.isPrototypeOf.call(obj, prototypeObj);
  }

  propertyIsEnumerable(obj: object, prop: string | symbol): boolean {
    return Object.prototype.propertyIsEnumerable.call(obj, prop);
  }
}

export { ObjectHandler };
