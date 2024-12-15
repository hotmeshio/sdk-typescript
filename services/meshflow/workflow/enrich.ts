import { StringStringType } from './common';
import { search } from './searchMethods';

/**
 * Adds custom user data to the backend workflow record (writes to HASH).
 * Runs exactly once during workflow execution.
 *
 * @param {StringStringType} fields - Key-value fields to enrich the workflow record.
 * @returns {Promise<boolean>} True when enrichment is completed.
 */
export async function enrich(fields: StringStringType): Promise<boolean> {
  const searchSession = await search();
  await searchSession.set(fields);
  return true;
}
