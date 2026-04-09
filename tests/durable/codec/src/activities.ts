export async function processData(
  name: string,
  value: number,
): Promise<Record<string, any>> {
  return {
    processed: true,
    name,
    doubled: value * 2,
    nested: { deep: { secret: 'encoded-at-rest' } },
    timestamp: new Date().toISOString(),
  };
}
