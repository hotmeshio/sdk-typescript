class UnaryHandler {
  not(value: boolean): boolean {
    return !value;
  }

  positive(value: number): number {
    return +value;
  }

  negative(value: number): number {
    return -value;
  }

  bitwise_not(value: number): number {
    return ~value;
  }
}

export { UnaryHandler };
