import { VALSEP } from '../../modules/key';
import { ILogger } from '../logger';
import { restoreHierarchy } from '../../modules/utils';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import {
  ActivityDetail,
  DependencyExport,
  ExportOptions,
  JobActionExport,
  JobExport,
  StreamHistoryEntry,
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
  /** @hidden */
  store: StoreService<ProviderClient, ProviderTransaction>;
  symbols: Promise<Symbols> | Symbols;

  /** @hidden */
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

    if (options.enrich_inputs && this.store.getStreamHistory) {
      const streamHistory = await this.store.getStreamHistory(jobId);
      jobExport.activities = this.buildActivities(
        jobExport.process,
        streamHistory,
      );
    }

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
   * Build structured activity details by correlating stream messages
   * (inputs, timing, retries) with the process hierarchy (outputs).
   *
   * Stream messages carry the raw data that flowed through each activity:
   * - `data` contains the activity input arguments
   * - `dad` (dimensional address) reveals cycle iterations (e.g., ,0,1,0 = 2nd cycle)
   * - `created_at` / `expired_at` give precise timing
   * - `retry_attempt` tracks retries
   *
   * The process hierarchy carries activity outputs organized by dimension.
   * This method merges both into a flat, dashboard-friendly list.
   */
  buildActivities(
    process: StringAnyType,
    streamHistory: StreamHistoryEntry[],
  ): ActivityDetail[] {
    const activities: ActivityDetail[] = [];

    for (const entry of streamHistory) {
      // Parse dimensional address: ",0,1,0,0" → ["0","1","0","0"]
      const dimParts = (entry.dad || '').split(',').filter(Boolean);
      const dimension = dimParts.join('/');

      // Detect cycle iteration from dimensional address
      // In a cycling workflow, the 2nd dimension component increments per cycle
      const cycleIteration = dimParts.length > 1 ? parseInt(dimParts[1]) || 0 : 0;

      // Look up the corresponding output from the process hierarchy
      // Process keys are like: process[dimension][activityName].output.data
      let output: Record<string, any> | undefined;
      let activityName = entry.aid;

      // Walk the process hierarchy using the dimension path
      let node = process;
      for (const part of dimParts) {
        if (node && typeof node === 'object' && node[part]) {
          node = node[part];
        } else {
          node = undefined;
          break;
        }
      }
      if (node && typeof node === 'object') {
        // node is now at the dimensional level, look for the activity
        if (node[activityName]?.output?.data) {
          output = node[activityName].output.data;
        }
      }

      // Compute timing
      const startedAt = entry.created_at;
      const completedAt = entry.expired_at;
      let durationMs: number | undefined;
      if (startedAt && completedAt) {
        durationMs = new Date(completedAt).getTime() - new Date(startedAt).getTime();
      }

      activities.push({
        name: activityName,
        type: entry.aid,
        dimension,
        input: entry.data,
        output,
        started_at: startedAt,
        completed_at: completedAt,
        duration_ms: durationMs,
        retry_attempt: entry.code === undefined ? 0 : undefined,
        cycle_iteration: cycleIteration > 0 ? cycleIteration : undefined,
        error: null,
      });
    }

    // Sort by time, then by dimension for cycle ordering
    activities.sort((a, b) => {
      const timeA = a.started_at || '';
      const timeB = b.started_at || '';
      return timeA.localeCompare(timeB);
    });

    return activities;
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
