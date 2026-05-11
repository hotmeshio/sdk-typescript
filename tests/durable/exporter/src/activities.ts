// Activity implementations for exporter enrichment tests

export async function greet(name: string): Promise<string> {
  return `Hello, ${name}!`;
}

export async function doubleValue(value: number): Promise<number> {
  return value * 2;
}

export async function formatResult(greeting: string, doubled: number): Promise<string> {
  return `${greeting} (value=${doubled})`;
}
