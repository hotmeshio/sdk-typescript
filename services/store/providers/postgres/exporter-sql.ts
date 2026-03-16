/**
 * SQL queries for exporter enrichment (Postgres provider).
 * These queries support the exporter's input enrichment and direct query features.
 */

/**
 * Fetch job record by key.
 */
export const GET_JOB_BY_KEY = `
  SELECT id, key, status, created_at, updated_at, expired_at, is_live
  FROM {schema}.jobs
  WHERE key = $1
  LIMIT 1
`;

/**
 * Fetch all attributes for a job.
 */
export const GET_JOB_ATTRIBUTES = `
  SELECT field, value
  FROM {schema}.jobs_attributes
  WHERE job_id = $1
  ORDER BY field
`;

/**
 * Fetch activity inputs for a workflow.
 * Matches all activity jobs for the given workflow and extracts their input arguments.
 */
export const GET_ACTIVITY_INPUTS = `
  SELECT j.key, ja.value
  FROM {schema}.jobs j
  JOIN {schema}.jobs_attributes ja ON ja.job_id = j.id
  WHERE j.key LIKE $1
    AND ja.field = $2
`;

/**
 * Fetch child workflow inputs in batch.
 * Uses parameterized IN clause for exact-match efficiency.
 * Note: This query template must be built dynamically with the correct number of placeholders.
 */
export function buildChildWorkflowInputsQuery(childCount: number, schema: string): string {
  const placeholders = Array.from({ length: childCount }, (_, i) => `$${i + 1}`).join(',');
  return `
    SELECT j.key, ja.value
    FROM ${schema}.jobs j
    JOIN ${schema}.jobs_attributes ja ON ja.job_id = j.id
    WHERE j.key IN (${placeholders})
      AND ja.field = $${childCount + 1}
  `;
}
