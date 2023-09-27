class BitwiseHandler {
  and(a: number, b: number): number {
    return a & b;
  }

  or(a: number, b: number): number {
    return a | b;
  }

  xor(a: number, b: number): number {
    return a ^ b;
  }

  leftShift(a: number, b: number): number {
    return a << b;
  }

  rightShift(a: number, b: number): number {
    return a >> b;
  }

  unsignedRightShift(a: number, b: number): number {
    return a >>> b;
  }
}

export { BitwiseHandler };
