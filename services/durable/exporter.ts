import { VALSEP } from '../../modules/key';
import { restoreHierarchy } from '../../modules/utils';
import { ILogger } from '../logger';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import {
  ExportItem,
  ExportOptions,
  DurableJobExport, 
  IdemType,
  TimelineEntry } from '../../types/exporter';
import { RedisClient, RedisMulti } from '../../types/redis';
import { StringStringType, Symbols } from "../../types/serializer";

class ExporterService {
  appId: string;
  logger: ILogger;
  store: StoreService<RedisClient, RedisMulti>;
  symbols: Promise<Symbols> | Symbols;

  constructor(appId: string, store: StoreService<RedisClient, RedisMulti>, logger: ILogger) {
    this.appId = appId;
    this.logger = logger;
    this.store = store;
  }

  /**
   * Convert the job hash from its compiles format into a DurableJobExport object with
   * facets that describe the workflow in terms relevant to narrative storytelling.
   */
  async export(jobId: string, options: ExportOptions = {}): Promise<DurableJobExport> {
    if (!this.symbols) {
      this.symbols = this.store.getAllSymbols();
      this.symbols = await this.symbols;
    }
    const jobData = await this.store.getRaw(jobId);
    const jobExport = this.inflate(jobData/*, depData*/);
    return jobExport;
  }

  /**
   * Inflates the key from Redis, 3-character symbol
   * into a human-readable JSON path, reflecting the
   * tree-like structure of the unidimensional Hash
   */
  inflateKey(key: string): string {
    if (key in this.symbols) {
      const path = this.symbols[key];
      const parts = path.split('/');
      return parts.join('/');
    }
    return key;
  }

  /**
   * Inflates the dependency data from Redis into a DurableJobExport object by
   * organizing the dimensional isolate in sch a way asto interleave
   * into a story
   * @param data - the dependency data from Redis
   * @returns - the organized dependency data
   */
  inflateDependencyData(data: string[]): Record<string, any>[] {
    return data.map((dependency, index: number): Record<string, any> => {
      const [action, topic, gid, dimension, ...jid] = dependency.split(VALSEP);
      const job_id = jid.join(VALSEP);
      return {
        index,
        action,
        topic,
        gid,
        dimension,
        job_id,
      }
    });
  }

  /**
   * Inflates the job data from Redis into a DurableJobExport object
   * @param jobHash - the job data from Redis
   * @param dependencyList - the list of dependencies for the job
   * @returns - the inflated job data
   */
  inflate(jobHash: StringStringType): DurableJobExport {
    const idempotents: IdemType[] = [];
    const state: StringStringType = {};
    const data: StringStringType = {};
    const other: ExportItem[] = [];
    const replay: Record<string, TimelineEntry> = {};
    const regex = /^([a-zA-Z]{3}),(\d+(?:,\d+)*)/;

    Object.entries(jobHash).forEach(([key, value]) => {
      const match = key.match(regex);
      if (match) {
        //activity process state
        this.inflateProcess(match, value, replay);
      } else if (key.length === 3) {
        //job state
        state[this.inflateKey(key)] = SerializerService.fromString(value);
      } else if (key.startsWith('_')) {
        //job data
        data[key.substring(1)] = value;
      } else if (key.startsWith('-')) {
        //actions with side effect (replayable)
        idempotents.push({
          key,
          value: SerializerService.fromString(value),
          parts: extractParts(key),
        });
      } else {
        //collator guids, etc
        other.push([null, key, value]);
      }
    });

    const sortEntriesByCreated = (obj: { [key: string]: TimelineEntry }): TimelineEntry[]  => {
      const entriesArray: TimelineEntry[] = Object.values(obj);
      entriesArray.sort((a, b) => {
        return (a.created || a.updated).localeCompare(b.created || b.updated);
      });
      return entriesArray;
    }

    /**
     * idem list has a complicated sort order based on indexes and dimensions
     */
    const sortParts = (parts: IdemType[]): IdemType[]=> {
      return parts.sort((a, b) => {
        const { dimension: aDim, index: aIdx, secondary: aSec } = a.parts;
        const { dimension: bDim, index: bIdx, secondary: bSec } = b.parts;
    
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
    };
    
    function extractParts(key: string): {index: number, dimension?: string, secondary?: number} {
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
        }
      } else {
        //-search,0,0-1-1-    -proxy,0,0-1-
        return {
          index: parseInt(parts[2], 10),
          secondary: parseInt(parts[3], 10),
          dimension: extractDimension(parts[1]),
        }
      }
    }

    return {
      data: restoreHierarchy(data),
      idempotents: sortParts(idempotents),
      state: Object.entries(restoreHierarchy(state))[0][1],
      status: jobHash[':'],
      replay: sortEntriesByCreated(replay),
    };
  }

  inflateProcess(match: RegExpMatchArray, value: string, replay: Record<string, Record<string, any>>) {
    const [_, letters, dimensions] = match;
    const path = this.inflateKey(letters);
    const parts = path.split('/');
    const activity = parts[0];
    const isCreate = path.endsWith('/output/metadata/ac');
    const isUpdate = path.endsWith('/output/metadata/au');
    if (isCreate || isUpdate) {
      const targetName = `${activity},${dimensions}`;
      let target = replay[targetName];
      if (!target) {
        replay[targetName] = {
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
}

export { ExporterService };
