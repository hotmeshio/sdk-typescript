import { PostgresJobEnumType } from './types';

/**
 * Determines if a table name represents a jobs table
 */
export function isJobsTable(tableName: string): boolean {
  return tableName.endsWith('jobs');
}

/**
 * Derives the enumerated `type` value based on the field name when
 * setting a field in a jobs table (a 'jobshash' table type).
 */
export function deriveType(fieldName: string): PostgresJobEnumType {
  if (fieldName === ':') {
    return 'status';
  } else if (fieldName.startsWith('_')) {
    return 'udata';
  } else if (fieldName.startsWith('-')) {
    return fieldName.includes(',') ? 'hmark' : 'jmark';
  } else if (fieldName.length === 3) {
    return 'jdata';
  } else if (fieldName.includes(',')) {
    return 'adata';
  } else {
    return 'other';
  }
}

/**
 * Processes rows from hmget and hgetall queries to map status and context fields
 */
export function processJobsRows(rows: any[], fields?: string[]): any {
  let statusValue: string | null = null;
  let contextValue: string | null = null;
  const fieldValueMap = new Map<string, string | null>();

  for (const row of rows) {
    if (row.field === 'status') {
      statusValue = row.value;
      fieldValueMap.set(':', row.value); // Map status to ':'
    } else if (row.field === 'context') {
      contextValue = row.value;
      fieldValueMap.set('@', row.value); // Map context to '@'
    } else if (row.field !== ':' && row.field !== '@') {
      // Ignore old format fields
      fieldValueMap.set(row.field, row.value);
    }
  }

  // Ensure ':' and '@' are present in the map with their values
  if (statusValue !== null) {
    fieldValueMap.set(':', statusValue);
  }
  if (contextValue !== null) {
    fieldValueMap.set('@', contextValue);
  }

  if (fields) {
    // For hmget - return array in same order as requested fields
    return fields.map((field) => fieldValueMap.get(field) || null);
  } else {
    // For hgetall - return object with all fields
    const result: Record<string, string> = {};
    for (const [field, value] of fieldValueMap) {
      if (value !== null) {
        result[field] = value;
      }
    }
    return result;
  }
}

/**
 * Processes rows from regular (non-jobs) tables for hgetall
 */
export function processRegularRows(rows: any[]): Record<string, string> {
  const result: Record<string, string> = {};
  for (const row of rows) {
    result[row.field] = row.value;
  }
  return result;
}
