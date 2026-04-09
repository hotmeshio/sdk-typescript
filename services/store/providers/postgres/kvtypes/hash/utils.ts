import { PostgresJobEnumType } from './types';

/**
 * Determines if a table name represents a jobs table
 */
export function isJobsTable(tableName: string): boolean {
  return tableName.endsWith('jobs');
}

/**
 * Splits a merged field string into its symbol and dimension components.
 * The first comma separates symbol from dimension.
 * If no comma is present, dimension is empty string.
 *
 * Examples:
 *   'ab,0,1,0'   → { symbol: 'ab',       dimension: ',0,1,0' }
 *   '-proxy,0,0'  → { symbol: '-proxy',   dimension: ',0,0' }
 *   '_email'       → { symbol: '_email',   dimension: '' }
 *   'jid'          → { symbol: 'jid',      dimension: '' }
 *   ':'            → { symbol: ':',        dimension: '' }
 */
export function splitField(field: string): { symbol: string; dimension: string } {
  const i = field.indexOf(',');
  if (i === -1) return { symbol: field, dimension: '' };
  return { symbol: field.substring(0, i), dimension: field.substring(i) };
}

/**
 * Derives the enumerated `type` value based on the field name when
 * setting a field in a jobs table (a 'jobshash' table type).
 */
export function deriveType(fieldName: string): PostgresJobEnumType {
  const { symbol, dimension } = splitField(fieldName);
  if (symbol === ':') return 'status';
  if (symbol.startsWith('_')) return 'udata';
  if (symbol.startsWith('-')) return dimension ? 'hmark' : 'jmark';
  if (dimension) return 'adata';
  if (symbol.length === 3) return 'jdata';
  return 'other';
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
