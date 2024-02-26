export async function childActivity(name: string): Promise<string> {
  return `interrupt childActivity, ${name}!`;
}
