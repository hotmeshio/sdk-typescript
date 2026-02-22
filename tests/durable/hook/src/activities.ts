export async function greet(name: string): Promise<string> {
  return `Hello, ${name}!`;
}

export async function bye(name: string, sleepForMS = 0): Promise<string> {
  if (sleepForMS > 0) {
    await new Promise((resolve) => setTimeout(resolve, sleepForMS));
  }
  return `Goodbye, ${name}!`;
}
