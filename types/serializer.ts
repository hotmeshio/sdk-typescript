export interface JSONSchema {
  type?: string;
  enum?: string[];
  examples?: any[];
  properties?: { [key: string]: JSONSchema };
  items?: JSONSchema;
  description?: string;
  'x-train'?: boolean; //extension property to mark item `values` as not being trainable (ssn, dob, guids are examples of fields that should never have their `values` trained)
}

export type SymbolRanges = { //keyname is <ns>:<app>:symbols:
  [key: string]: number; //eg: {"$": 0, "a1": 26, "a2" 39, "$metadata_cursor": 39, "$data_cursor": 2704} (job ($) holds range 0-26; every other activity has a number that increments by 13; up to 200+ unique activities may be modeled; the :cursor fields are used by the sytem to track the next reserved tranche using hincrby
}

export type Symbols = { //keyname is <ns>:<app>:symbols:<aid> (where aid can be $ for job or a1, a2, etc. for activities)
  [key: string]: string; //eg: {"operation/name": "26", "a2" 39, ":cursor": 39} (job holds range 0-26; every other activity has a number that increments by 13; up to 200 activity ranges may be listed; one field called $count is used by the sytem to track the next reserved tranche using hincrby; job always seeds with 26
}

export type SymbolSets = {
  [key: string]: {
    [key: string]: string;
  }
}

export type StringStringType = {
  [key: string]: string;
};

export type StringAnyType = {
  [key: string]: any;
};

export type StringScalarType = {
  [key: string]: boolean | number | string;
};

export type SymbolMap = Map<string, string>;
export type SymbolMaps = Map<string, SymbolMap>;
