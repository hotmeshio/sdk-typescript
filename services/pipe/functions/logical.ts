class LogicalHandler {
  and(firstValue: boolean, secondValue: boolean): boolean {
    return firstValue && secondValue;
  }

  or(firstValue: boolean, secondValue: boolean): boolean {
    return firstValue || secondValue;
  }
}

export { LogicalHandler };
