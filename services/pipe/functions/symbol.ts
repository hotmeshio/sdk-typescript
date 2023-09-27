class SymbolHandler {
  null(): null {
    return null;
  }

  undefined(): undefined {
    return undefined;
  }

  whitespace(): string {
    return ' ';
  }

  object(): object {
    return {};
  }

  array(): any[] {
    return [];
  }

  posInfinity(): number {
    return Infinity;
  }

  negInfinity(): number {
    return -Infinity;
  }

  NaN(): number {
    return NaN;
  }

  date(): Date {
    return new Date();
  }
}

export { SymbolHandler };
