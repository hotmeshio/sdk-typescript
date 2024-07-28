import { restoreHierarchy } from '../../modules/utils';
import { ILogger } from '../logger';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import {
  ExportOptions,
  MeshFlowJobExport,
  TimelineType,
  TransitionType,
  ExportFields,
} from '../../types/exporter';
import { RedisClient, RedisMulti } from '../../types/redis';
import {
  StringAnyType,
  StringStringType,
  Symbols,
} from '../../types/serializer';

class ExporterService {
  appId: string;
  logger: ILogger;
  store: StoreService<RedisClient, RedisMulti>;
  symbols: Promise<Symbols> | Symbols;
  private static symbols: Map<string, Symbols> = new Map();

  constructor(
    appId: string,
    store: StoreService<RedisClient, RedisMulti>,
    logger: ILogger,
  ) {
    this.appId = appId;
    this.logger = logger;
    this.store = store;
  }

  /**
   * Convert the job hash from its compiles format into a MeshFlowJobExport object with
   * facets that describe the workflow in terms relevant to narrative storytelling.
   */
  async export(
    jobId: string,
    options: ExportOptions = {},
  ): Promise<MeshFlowJobExport> {
    if (!ExporterService.symbols.has(this.appId)) {
      const symbols: Symbols | Promise<Symbols> = this.store.getAllSymbols();
      ExporterService.symbols.set(this.appId, await symbols);
    }
    const jobData = await this.store.getRaw(jobId);
    const jobExport = this.inflate(jobData, options);
    return jobExport;
  }

  /**
   * Inflates the job data from Redis into a MeshFlowJobExport object
   * @param jobHash - the job data from Redis
   * @param dependencyList - the list of dependencies for the job
   * @returns - the inflated job data
   */
  inflate(
    jobHash: StringStringType,
    options: ExportOptions,
  ): MeshFlowJobExport {
    const timeline: TimelineType[] = [];
    const state: StringAnyType = {};
    const data: StringStringType = {};
    const transitionsObject: Record<string, TransitionType> = {};
    const regex = /^([a-zA-Z]{3}),(\d+(?:,\d+)*)/;

    Object.entries(jobHash).forEach(([key, value]) => {
      const match = key.match(regex);

      if (match) {
        //transitions
        this.inflateTransition(match, value, transitionsObject);
      } else if (key.startsWith('_')) {
        //data
        data[key.substring(1)] = value;
      } else if (key.startsWith('-')) {
        //timeline
        const keyParts = this.keyToObject(key);
        timeline.push({
          ...keyParts,
          key,
          value: this.resolveValue(value, options.values),
        });
      } else if (key.length === 3) {
        //state
        state[this.inflateKey(key)] = SerializerService.fromString(value);
      }
    });

    return this.filterFields(
      {
        data: restoreHierarchy(data),
        state: Object.entries(restoreHierarchy(state))[0][1],
        status: parseInt(jobHash[':'], 10),
        timeline: this.sortParts(timeline),
        transitions: this.sortEntriesByCreated(transitionsObject),
      },
      options.block,
      options.allow,
    );
  }

  resolveValue(
    raw: string,
    withValues: boolean,
  ): Record<string, any> | string | number | null {
    const resolved = SerializerService.fromString(raw);
    if (withValues !== false) {
      return resolved;
    }
    if (resolved && typeof resolved === 'object') {
      if ('data' in resolved) {
        resolved.data = {};
      }
      if ('$error' in resolved) {
        resolved.$error = {};
      }
    }
    return resolved;
  }

  /**
   * Inflates the key from Redis, 3-character symbol
   * into a human-readable JSON path, reflecting the
   * tree-like structure of the unidimensional Hash
   * @private
   */
  inflateKey(key: string): string {
    const symbols = ExporterService.symbols.get(this.appId);
    if (key in symbols) {
      const path = symbols[key];
      const parts = path.split('/');
      return parts.join('/');
    }
    return key;
  }

  filterFields(
    fullObject: MeshFlowJobExport,
    block: ExportFields[] = [],
    allow: ExportFields[] = [],
  ): Partial<MeshFlowJobExport> {
    let result: Partial<MeshFlowJobExport> = {};
    if (allow && allow.length > 0) {
      allow.forEach((field) => {
        if (field in fullObject) {
          result[field] = fullObject[field] as StringAnyType &
            number &
            TimelineType[] &
            TransitionType[];
        }
      });
    } else {
      result = { ...fullObject };
    }
    if (block && block.length > 0) {
      block.forEach((field) => {
        if (field in result) {
          delete result[field];
        }
      });
    }
    return result as MeshFlowJobExport;
  }

  inflateTransition(
    match: RegExpMatchArray,
    value: string,
    transitionsObject: Record<string, TransitionType>,
  ) {
    const [_, letters, dimensions] = match;
    const path = this.inflateKey(letters);
    const parts = path.split('/');
    const activity = parts[0];
    const isCreate = path.endsWith('/output/metadata/ac');
    const isUpdate = path.endsWith('/output/metadata/au');
    //for now only export activity start/stop; activity data would also be interesting
    if (isCreate || isUpdate) {
      const targetName = `${activity},${dimensions}`;
      const target = transitionsObject[targetName];
      if (!target) {
        transitionsObject[targetName] = {
          activity,
          dimensions,
          created: isCreate ? value : null,
          updated: isUpdate ? value : null,
        };
      } else {
        target[isCreate ? 'created' : 'updated'] = value;
      }
    }
  }

  sortEntriesByCreated(obj: {
    [key: string]: TransitionType;
  }): TransitionType[] {
    const entriesArray: TransitionType[] = Object.values(obj);
    entriesArray.sort((a, b) => {
      return (a.created || a.updated).localeCompare(b.created || b.updated);
    });
    return entriesArray;
  }

  /**
   * marker names are overloaded with details like sequence, type, etc
   */
  keyToObject(key: string): {
    index: number;
    dimension?: string;
    secondary?: number;
  } {
    function extractDimension(label: string): string {
      const parts = label.split(',');
      if (parts.length > 1) {
        parts.shift();
        return parts.join(',');
      }
    }
    const parts = key.split('-');
    if (parts.length === 4) {
      //-proxy-5-     -search-1-1-
      return {
        index: parseInt(parts[2], 10),
        dimension: extractDimension(parts[1]),
      };
    } else {
      //-search,0,0-1-1-    -proxy,0,0-1-
      return {
        index: parseInt(parts[2], 10),
        secondary: parseInt(parts[3], 10),
        dimension: extractDimension(parts[1]),
      };
    }
  }

  /**
   * idem list has a complicated sort order based on indexes and dimensions
   */
  sortParts(parts: TimelineType[]): TimelineType[] {
    return parts.sort((a, b) => {
      const { dimension: aDim, index: aIdx, secondary: aSec } = a;
      const { dimension: bDim, index: bIdx, secondary: bSec } = b;

      if (aDim === undefined && bDim !== undefined) return -1;
      if (aDim !== undefined && bDim === undefined) return 1;
      if (aDim !== undefined && bDim !== undefined) {
        if (aDim < bDim) return -1;
        if (aDim > bDim) return 1;
      }

      if (aIdx < bIdx) return -1;
      if (aIdx > bIdx) return 1;

      if (aSec === undefined && bSec !== undefined) return -1;
      if (aSec !== undefined && bSec === undefined) return 1;
      if (aSec !== undefined && bSec !== undefined) {
        if (aSec < bSec) return -1;
        if (aSec > bSec) return 1;
      }

      return 0;
    });
  }
}

export { ExporterService };
