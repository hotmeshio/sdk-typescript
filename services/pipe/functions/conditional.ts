class ConditionalHandler {
  ternary(condition: boolean, valueIfTrue: any, valueIfFalse: any): any {
    return condition ? valueIfTrue : valueIfFalse;
  }

  equality(value1: any, value2: any): boolean {
    return value1 == value2;
  }

  strict_equality(value1: any, value2: any): boolean {
    return value1 === value2;
  }

  inequality(value1: any, value2: any): boolean {
    return value1 != value2;
  }

  strict_inequality(value1: any, value2: any): boolean {
    return value1 !== value2;
  }

  greater_than(value1: number, value2: number): boolean {
    return value1 > value2;
  }

  less_than(value1: number, value2: number): boolean {
    return value1 < value2;
  }

  greater_than_or_equal(value1: number, value2: number): boolean {
    return value1 >= value2;
  }

  less_than_or_equal(value1: number, value2: number): boolean {
    return value1 <= value2;
  }

  nullish(value1: any, value2: any): any {
    return value1 ?? value2;
  }
}

export { ConditionalHandler };
