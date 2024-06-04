class MathHandler {
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

  abs(x: number): number {
    return Math.abs(x);
  }

  acos(x: number): number {
    return Math.acos(x);
  }

  acosh(x: number): number {
    return Math.acosh(x);
  }

  asin(x: number): number {
    return Math.asin(x);
  }

  asinh(x: number): number {
    return Math.asinh(x);
  }

  atan(x: number): number {
    return Math.atan(x);
  }

  atan2(y: number, x: number): number {
    return Math.atan2(y, x);
  }

  atanh(x: number): number {
    return Math.atanh(x);
  }

  cbrt(x: number): number {
    return Math.cbrt(x);
  }

  ceil(x: number): number {
    return Math.ceil(x);
  }

  clz32(x: number): number {
    return Math.clz32(x);
  }

  cos(x: number): number {
    return Math.cos(x);
  }

  cosh(x: number): number {
    return Math.cosh(x);
  }

  exp(x: number): number {
    return Math.exp(x);
  }

  expm1(x: number): number {
    return Math.expm1(x);
  }

  floor(x: number): number {
    return Math.floor(x);
  }

  fround(x: number): number {
    return Math.fround(x);
  }

  hypot(...values: number[]): number {
    return Math.hypot(...values);
  }

  imul(x: number, y: number): number {
    return Math.imul(x, y);
  }

  log(x: number): number {
    return Math.log(x);
  }

  log10(x: number): number {
    return Math.log10(x);
  }

  log1p(x: number): number {
    return Math.log1p(x);
  }

  log2(x: number): number {
    return Math.log2(x);
  }

  max(...values: number[]): number {
    return Math.max(...values);
  }

  min(...values: number[]): number {
    return Math.min(...values);
  }

  pow(x: number, y: number): number {
    return Math.pow(x, y);
  }

  random(): number {
    return Math.random();
  }

  round(x: number): number {
    return Math.round(x);
  }

  sign(x: number): number {
    return Math.sign(x);
  }

  sin(x: number): number {
    return Math.sin(x);
  }

  sinh(x: number): number {
    return Math.sinh(x);
  }

  sqrt(x: number): number {
    return Math.sqrt(x);
  }

  tan(x: number): number {
    return Math.tan(x);
  }

  tanh(x: number): number {
    return Math.tanh(x);
  }

  trunc(x: number): number {
    return Math.trunc(x);
  }
}

export { MathHandler };
