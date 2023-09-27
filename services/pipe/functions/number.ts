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

  add(...operands: (number | number[])[]): number {
    // @ts-ignore
    return operands.reduce((a: number, b: number | number[]) => {
      if (Array.isArray(b)) {
        return a + this.add(...b);
      } else {
        return a + b;
      }
    }, 0);
  }

  subtract(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }
    let flatOperands: number[] = [];
    operands.forEach((op: number | number[]) => {
      if (Array.isArray(op)) {
        flatOperands = [...flatOperands, ...op];
      } else {
        flatOperands.push(op);
      }
    });
    if (flatOperands.length === 0) {
      throw new Error('At least one operand is required after flattening.');
    }
    const result = flatOperands.reduce((a: number, b: number, i: number) => {
      return i === 0 ? a : a - b;
    });
    return result;
  }
  
  
  multiply(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }
  
    // @ts-ignore
    return operands.reduce((a: number, b: number | number[]) => {
      if (Array.isArray(b)) {
        return a * this.multiply(...b);
      } else {
        return a * b;
      }
    }, 1);
  }
  
  divide(...operands: (number | number[])[]): number {
    if (operands.length === 0) {
      throw new Error('At least one operand is required.');
    }
    let flatOperands: number[] = [];
    operands.forEach((op: number | number[]) => {
      if (Array.isArray(op)) {
        flatOperands = [...flatOperands, ...op];
      } else {
        flatOperands.push(op);
      }
    });
    if (flatOperands.length === 0) {
      throw new Error('At least one operand is required after flattening.');
    }
    const result = flatOperands.reduce((a: number, b: number, i: number) => {
      if (b === 0) {
        return NaN;
      }
      return i === 0 ? a : a / b;
    });
    if (isNaN(result)) {
      return NaN;
    }
    return result;
  }  
}

export { NumberHandler };
