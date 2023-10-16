class NumberHandler {
  isFinite(input: number): boolean {
    return Number.isFinite(input);
  }

  isEven(input: number): boolean {
    return !isNaN(input) && input % 2 === 0;
  }

  isOdd(input: number): boolean {
    return !isNaN(input) && input % 2 !== 0;
  }

  isInteger(input: number): boolean {
    return Number.isInteger(input);
  }

  isNaN(input: number): boolean {
    return Number.isNaN(input);
  }

  parseFloat(input: string): number {
    return parseFloat(input);
  }

  parseInt(input: string, radix?: number): number {
    return parseInt(input, radix);
  }

  toFixed(input: number, digits?: number): string {
    return input.toFixed(digits);
  }

  toExponential(input: number, fractionalDigits?: number): string {
    return input.toExponential(fractionalDigits);
  }

  toPrecision(input: number, precision?: number): string {
    return input.toPrecision(precision);
  }

  gte(input: number, compareValue: number): boolean {
    return input >= compareValue;
  }

  lte(input: number, compareValue: number): boolean {
    return input <= compareValue;
  }

  gt(input: number, compareValue: number): boolean {
    return input > compareValue;
  }

  lt(input: number, compareValue: number): boolean {
    return input < compareValue;
  }

  max(...values: number[]): number {
    return Math.max(...values);
  }

  min(...values: number[]): number {
    return Math.min(...values);
  }

  pow(base: number, exponent: number): number {
    return Math.pow(base, exponent);
  }

  round(input: number): number {
    return Math.round(input);
  }
}

export { NumberHandler };
