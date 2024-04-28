import { getSymVal } from '../../modules/utils';
import { Consumes } from '../../types/activity';
import {
  StringStringType,
  StringAnyType,
  SymbolMap,
  SymbolMaps,
  SymbolSets, 
  Symbols } from '../../types/serializer';

const dateReg = /^"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z)?"$/;

export const MDATA_SYMBOLS = {
  SLOTS: 26,
  ACTIVITY: {
    KEYS: ['aid', 'dad', 'as', 'atp', 'stp', 'ac', 'au', 'err','l1s','l2s']
  },
  ACTIVITY_UPDATE: {
    KEYS: ['au', 'err', 'l2s']
  },
  JOB: {
    KEYS: ['ngn', 'tpc', 'pj', 'pg', 'pd', 'px', 'pa', 'key', 'app', 'vrs', 'jid', 'gid', 'aid', 'ts', 'jc', 'ju', 'js', 'err', 'trc']
  },
  JOB_UPDATE: {
    KEYS: ['ju', 'err']
  }
};

export class SerializerService {
  dIds: StringStringType;
  symKeys: SymbolMaps;
  symReverseKeys: SymbolMaps;
  symValMaps: SymbolMap;
  symValReverseMaps: SymbolMap;

  constructor() {
    this.resetSymbols({}, {}, {});
  }

  abbreviate(consumes: Consumes, symbolNames: string[], fields: string[] = []): string[] {
    for (const symbolName of symbolNames) {
      const symbolSet = this.symKeys.get(symbolName);
      const symbolPaths = consumes[symbolName];
      for (const symbolPath of symbolPaths) {
        const abbreviation = symbolSet.get(symbolPath);
        if (abbreviation) {
          const dimensionalIndex = this.resolveDimensionalIndex(symbolPath);
          fields.push(`${abbreviation}${dimensionalIndex}`);
        } else {
          fields.push(symbolPath);
        }
      }
    }
    return fields;
  }

  resolveDimensionalIndex(path: string): string {
    if (this.isJobPath(path)) {
      return '';
    } else {
      const [activityId] = path.split('/');
      if (activityId in this.dIds) {
        return this.dIds[activityId];
      } else if ('$ADJACENT' in this.dIds) {
        //else=> pre-authorizing adjacent activity entry
        return this.dIds['$ADJACENT'];
      }
      return ',0';
    }
  }

  isJobPath(path: string): boolean {
    return path.startsWith('data/') || path.startsWith('metadata/');
  }

  resetSymbols(symKeys: SymbolSets, symVals: Symbols, dIds: StringStringType): void {
    this.symKeys = new Map();
    this.symReverseKeys = new Map();
    for (const id in symKeys) {
      this.symKeys.set(id, new Map(Object.entries(symKeys[id])));
    }
    this.symValMaps = new Map(Object.entries(symVals));
    this.symValReverseMaps = this.getReverseValueMap(this.symValMaps);
    this.dIds = dIds;
  }

  getReverseKeyMap(keyMap: SymbolMap, id?: string): SymbolMap {
    let map = this.symReverseKeys.get(id);
    if (!map) {
      map = new Map();
      for (let [key, val] of keyMap.entries()) {
        map.set(val, key);
      }
      this.symReverseKeys.set(id, map);
    }
    return map;
  }

  getReverseValueMap(valueMap: SymbolMap): SymbolMap {
    const map = new Map();
    for (let [key, val] of valueMap.entries()) {
      map.set(val, key);
    }
    return map;
  }

  static filterSymVals(startIndex: number, maxIndex: number, existingSymbolValues: Symbols,  proposedValues: Set<string>): Symbols {
    let newSymbolValues: Symbols = {};
    let currentSymbolValues: Symbols = { ...existingSymbolValues };
    let currentValuesSet: Set<string> = new Set(Object.values(currentSymbolValues));
    for (let value of  proposedValues) {
      if (!currentValuesSet.has(value)) {
        if (startIndex > maxIndex) {
          return newSymbolValues;
        }
        const symbol = getSymVal(startIndex);
        startIndex++;
        newSymbolValues[symbol] = value;
        currentValuesSet.add(value);
      }
    }
    return newSymbolValues;
  }

  compress(document: StringStringType, ids: string[]): StringStringType {
    if (this.symKeys.size === 0) {
      return document;
    }
    let source: StringStringType = { ...document };
    let result: StringStringType = { };

    const compressWithMap = (abbreviationMap: SymbolMap, id: string) => {
      for (let key in source) {
        if (key.startsWith(`${id}/`) || (id.startsWith('$') && ['data', 'metadata'].includes(key.split('/')[0]))) {
          const dimensionalIndex = this.resolveDimensionalIndex(key);
          let shortKey = abbreviationMap.get(key) || key;
          const shortDimensionalKey = `${shortKey}${dimensionalIndex}`;
          result[shortDimensionalKey] = source[key];
        } else if (!(key in result) && this.isLiteralKeyType(key)) {
          //mark (-) and search (_)
          result[key] = source[key];
        }  
      }
    };
    for (let id of ids) {
      const abbreviationMap = this.symKeys.get(id);
      if (abbreviationMap) {
        compressWithMap(abbreviationMap, id);
      }
    }
    return result;
  }

  isLiteralKeyType(key: string): boolean {
    return key.startsWith('-') || key.startsWith('_');
  }

  decompress(document: StringStringType, ids: string[]): StringStringType {
    if (this.symKeys.size === 0) {
      return document;
    }
    let result: StringStringType = { ...document };

    const inflateWithMap = (abbreviationMap: SymbolMap, id: string) => {
      const reversedAbbreviationMap = this.getReverseKeyMap(abbreviationMap, id);
      for (let key in result) {
        //strip dimensional index from key
        const shortKey = key.split(',')[0];
        let longKey = reversedAbbreviationMap.get(shortKey);
        if (longKey) {
          result[longKey] = result[key];
          delete result[key];
        }
      }
    };
    for (let id of ids) {
      const abbreviationMap = this.symKeys.get(id);
      if (abbreviationMap) {
        inflateWithMap(abbreviationMap, id);
      }
    }
    return result;
  }

  //stringify: convert a multi-dimensional document to a 2-d hash
  stringify(document: Record<string, any>): StringStringType {
    let result: StringStringType = {};
    for (let key in document) {
      let value = SerializerService.toString(document[key]);
      if (value) {
        if (/^:*[a-zA-Z]{2}$/.test(value)) {
          value = ':' + value;
        } else if (this.symValReverseMaps.has(value)) {
          value = this.symValReverseMaps.get(value);
        }
        result[key] = value;
      }
    }
    return result;
  }

  //parse: convert a 2-d hash to a multi-dimensional document
  parse(document: StringStringType): any {
    let result: any = {};
    for (let [key, value] of Object.entries(document)) {
      if (value === undefined || value === null) continue;
      if (/^:+[a-zA-Z]{2}$/.test(value)) {
        result[key] = value.slice(1);
      } else {
        if (value?.length === 2 && this.symValMaps.has(value)) {
          value = this.symValMaps.get(value);
        }
        result[key] = SerializerService.fromString(value);
      }
    }
    return result;
  }

  static toString(value: any): string|undefined {
    switch (typeof value) {
      case 'string':
        break;
      case 'boolean':
        value = value ? '/t' : '/f';
        break;
      case 'number':
        value = '/d' + value.toString();
        break;
      case 'undefined':
        return undefined;
      case 'object':
        if (value === null) {
          value = '/n';
        } else {
          value = '/s' + JSON.stringify(value);
        }
        break;
    }
    return value;
  }

  static fromString(value: string|undefined): any {
    if (typeof value !== 'string') return undefined;
    const prefix = value.slice(0, 2);
    const rest = value.slice(2);
    switch (prefix) {
      case '/t': // boolean true
        return true;
      case '/f': // boolean false
        return false;
      case '/d': // number
        return Number(rest);
      case '/n': // null
        return null;
      case '/s': // object (JSON string)
        if (dateReg.exec(rest)) {
          return new Date(JSON.parse(rest));
        }
        return JSON.parse(rest);
      default: // string
        return value;
    }
  }

  public package(multiDimensionalDocument: StringAnyType, ids: string[]): StringStringType {
    const flatDocument = this.stringify(multiDimensionalDocument);
    return this.compress(flatDocument, ids);
  }

  public unpackage(document: StringStringType, ids: string[]): StringAnyType {
    const multiDimensionalDocument = this.decompress(document, ids);
    return this.parse(multiDimensionalDocument);
  }

  public export(): SymbolSets {
    const obj: {[key: string]: StringStringType} = {};
    for (const [id, map] of this.symKeys.entries()) {
      obj[id] = {};
      for (const [key, value] of map.entries()) {
        obj[id][key] = value;
      }
    }
    return obj;
  }
}
