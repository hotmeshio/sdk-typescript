import { PostgresJobEnumType } from '../../../../../../types/postgres';
import {
  HScanResult,
  HSetOptions,
  ProviderTransaction,
  SetOptions,
} from '../../../../../../types/provider';
import type { KVSQL } from '../../kvsql';

export interface Multi extends ProviderTransaction {
  addCommand: (
    sql: string,
    params: any[],
    returnType: string,
    transform?: (rows: any[]) => any,
  ) => void;
}

export interface HashContext {
  context: KVSQL;
}

export interface SqlResult {
  sql: string;
  params: any[];
}

export interface JsonbOperation {
  path?: string;
  value?: any;
  index?: number;
}

export {
  PostgresJobEnumType,
  HScanResult,
  HSetOptions,
  ProviderTransaction,
  SetOptions,
  KVSQL,
};
