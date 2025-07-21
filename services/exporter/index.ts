import { VALSEP } from '../../modules/key';
import { ILogger } from '../logger';
import { restoreHierarchy } from '../../modules/utils';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import {
  DependencyExport,
  ExportOptions,
  JobActionExport,
  JobExport,
} from '../../types/exporter';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  StringAnyType,
  StringStringType,
  Symbols,
} from '../../types/serializer';

/**
 * Downloads job data and expands process data and
 * includes dependency list
 */
class ExporterService {
  appId: string;
  logger: ILogger;
  store: StoreService<ProviderClient, ProviderTransaction>;
  symbols: Promise<Symbols> | Symbols;

  constructor(
    appId: string,
    store: StoreService<ProviderClient, ProviderTransaction>,
    logger: ILogger,
  ) {
    this.appId = appId;
    this.logger = logger;
    this.store = store;
  }

  /**
   * Convert the job hash into a JobExport object.
   * This object contains various facets that describe the interaction
   * in terms relevant to narrative storytelling.
   */
  async export(jobId: string, options: ExportOptions = {}): Promise<JobExport> {
    if (!this.symbols) {
      this.symbols = this.store.getAllSymbols();
      this.symbols = await this.symbols;
    }
    const depData = []; // await this.store.getDependencies(jobId);
    const jobData = await this.store.getRaw(jobId);
    const jobExport = this.inflate(jobData, depData);
    return jobExport;
  }

  /**
   * Inflates the key
   * into a human-readable JSON path, reflecting the
   * tree-like structure of the unidimensional Hash
   */
  inflateKey(key: string): string {
    return key in this.symbols ? this.symbols[key] : key;
  }

  /**
   * Inflates the job data into a JobExport object
   * @param jobHash - the job data
   * @param dependencyList - the list of dependencies for the job
   * @returns - the inflated job data
   */
  inflate(jobHash: StringStringType, dependencyList: string[]): JobExport {
    //the list of actions taken in the workflow and hook functions
    const actions: JobActionExport = {
      hooks: {},
      main: {
        cursor: -1,
        items: [],
      },
    };
    const process: StringAnyType = {};
    const dependencies = this.inflateDependencyData(dependencyList, actions);
    const regex = /^([a-zA-Z]{3}),(\d+(?:,\d+)*)/;

    Object.entries(jobHash).forEach(([key, value]) => {
      const match = key.match(regex);
      if (match) {
        //activity process state
        const [_, letters, numbers] = match;
        const path = this.inflateKey(letters);
        const dimensions = `${numbers.replace(/,/g, '/')}`;
        const resolved = SerializerService.fromString(value);
        process[`${dimensions}/${path}`] = resolved;
      } else if (key.length === 3) {
        //job state
        process[this.inflateKey(key)] = SerializerService.fromString(value);
      }
    });

    return {
      dependencies,
      process: restoreHierarchy(process),
      status: jobHash[':'],
    };
  }

  /**
   * Inflates the dependency data into a JobExport object by
   * organizing the dimensional isolate in such a way as to interleave
   * into a story
   * @param data - the dependency data
   * @returns - the organized dependency data
   */
  inflateDependencyData(
    data: string[],
    actions: JobActionExport,
  ): DependencyExport[] {
    const hookReg = /([0-9,]+)-(\d+)$/;
    const flowReg = /-(\d+)$/;
    return data.map((dependency, index: number): DependencyExport => {
      const [action, topic, gid, _pd, ...jid] = dependency.split(VALSEP);
      const jobId = jid.join(VALSEP);
      const match = jobId.match(hookReg);
      let prefix: string;
      let type: 'hook' | 'flow' | 'other';
      let dimensionKey = '';

      if (match) {
        //hook-originating dependency
        const [_, dimension, counter] = match;
        dimensionKey = dimension.split(',').join('/');
        prefix = `${dimensionKey}[${counter}]`;
        type = 'hook';
      } else {
        const match = jobId.match(flowReg);
        if (match) {
          //main workflow-originating dependency
          const [_, counter] = match;
          prefix = `[${counter}]`;
          type = 'flow';
        } else {
          //'other' types like signal cleanup
          prefix = '/';
          type = 'other';
        }
      }
      return {
        type: action,
        topic,
        gid,
        jid: jobId,
      } as unknown as DependencyExport;
    });
  }
}

export { ExporterService };
