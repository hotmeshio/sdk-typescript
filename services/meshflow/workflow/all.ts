/**
 * A workflow-safe version of Promise.all, currently limited to certain concurrency.
 * @private
 * @param {...Promise<T>[]} promises - An array of promises.
 * @returns {Promise<T[]>} A promise resolving to an array of results.
 */
export async function all<T>(...promises: Promise<T>[]): Promise<T[]> {
  // a micro-delay to ensure correct sequencing
  await new Promise((resolve) => setTimeout(resolve, 1));
  return await Promise.all(promises);
}
