import { ILogger } from '../logger';
import { StoreService } from '../store';
import { StringStringType, Symbols } from "../../types";
import { RedisClient, RedisMulti } from '../../types/redis';
import {
  ActivityAction,
  DependencyExport,
  ExportItem,
  ExportOptions,
  JobAction,
  JobActionExport,
  DurableJobExport,
  JobTimeline } from '../../types/exporter';
import { SerializerService } from '../serializer';
import { restoreHierarchy } from '../../modules/utils';
import { VALSEP } from '../../modules/key';

/**
 * Downloads job data from Redis (hscan, hmget, hgetall)
 * Splits, Inflates, and Sorts the job data for use in durable contexts
 */
class ExporterService {
  appId: string;
  logger: ILogger;
  serializer: SerializerService
  store: StoreService<RedisClient, RedisMulti>;
  symbols: Promise<Symbols> | Symbols;

  /**
   * Friendly names for the activity ids
   */
  activitySymbols: Symbols = {
    t1: 'trigger',
    a1: 'pivot',
    w1: 'worker',
    a592: 'sleeper',
    a594: 'awaiter',
    a599: 'retryer',
    c592: 'sleep_cycler',
    c594: 'await_cycler',
    c599: 'retry_cycler',
    s5: 'scrubber',
    sig: 'hook',
    siga1: 'hook_pivot',
    sigw1: 'hook_worker',
    siga592: 'hook_sleeper',
    siga594: 'hook_awaiter',
    siga599: 'hook_retryer',
    sigc592: 'hook_sleep_cycler',
    sigc594: 'hook_await_cycler',
    sigc599: 'hook_retry_cycler',
  }

  //adjacent transitions
  transitions = {
    trigger: ['pivot', 'hook'],
    pivot: ['worker'],
    worker: ['sleeper', 'awaiter', 'retryer', 'scrubber'],
    sleeper: ['sleep_cycler'],
    awaiter: ['await_cycler'],
    retryer: ['retry_cycler'],
    hook: ['hook_pivot'],
    hook_pivot: ['hook_worker'],
    hook_worker: ['hook_sleeper', 'hook_awaiter', 'hook_retryer'],
    hook_sleeper: ['hook_sleep_cycler'],
    hook_awaiter: ['hook_await_cycler'],
    hook_retryer: ['hook_retry_cycler'],
  };

  //goto transitions
  cycles = {
    sleep_cycler: ['pivot'],
    await_cycler: ['pivot'],
    retry_cycler: ['pivot'],
    hook_sleep_cycler: ['hook_pivot'],
    hook_await_cycler: ['hook_pivot'],
    hook_retry_cycler: ['hook_pivot'],
  }

  constructor(appId: string, store: StoreService<RedisClient, RedisMulti>, logger: ILogger) {
    this.appId = appId;
    this.logger = logger;
    this.store = store;
    this.serializer = new SerializerService();
  }

  /**
   * Convert the job hash and dependency list into a DurableJobExport object.
   * This object contains various facets that describe the interaction
   * in terms relevant to narrative storytelling.
   */
  async export(jobId: string, options: ExportOptions = {}): Promise<DurableJobExport> {
    if (!this.symbols) {
      this.symbols = this.store.getAllSymbols();
      this.symbols = await this.symbols;
    }
    const depData = await this.store.getDependencies(jobId);
    const jobData = await this.store.getRaw(jobId);
    const jobExport = this.inflate(jobData, depData);
    return jobExport;
  }

  /**
   * Interleave actions into the replay timeline to create
   * a time-ordered timeline of the entire interaction, beginning
   * with the entry trigger and concluding with the scrubber
   * activity. Using the returned timeline, it is possible to
   * create an animated narrative of the job, highlighting
   * activities in the graph according to the timeline's 
   * activity-created (/ac) and activity-updated (/au) entries.
   */
  createTimeline(replay: ExportItem[], actions: JobActionExport): JobTimeline[] {
    const timeline: JobTimeline[] = [];
    replay.forEach((item) => {
      const dimensions = item[0];
      const parts = dimensions.split('/');
      const activityName = item[1].split('/')[0];
      const duplex = item[1].endsWith('/ac') ? 'entry' : 'exit';
      const timestamp = item[2];
      let event: JobTimeline = {
        activity: activityName,
        duplex: duplex as 'entry' | 'exit',
        dimension: dimensions,
        timestamp,
        created: timestamp,
        updated: timestamp,
      };
      const prior = timeline[timeline.length - 1];
      if (prior && prior.activity === event.activity && prior.duplex !== event.duplex && prior.dimension === event.dimension) {
        if (event.duplex === 'exit') {
          prior.updated = event.timestamp;
        } else {
          prior.created = event.timestamp;
        }
        event = prior;
      } else {
        timeline.push(event);
      }

      if (this.isMainEntry(item[1])) {
        event.actions = [] as ActivityAction[];
        this.interleaveActions(actions.main, event.actions);
      } else if (this.isHookEntry(item[1])) {
        const hookDimension = `/${parts[1]}/${parts[2]}`;
        const hookActions = actions.hooks[hookDimension];
        event.actions = [] as ActivityAction[];
        this.interleaveActions(hookActions, event.actions);
      }
    });
    return timeline;
  }

  /**
   * Interleave actions into the 'worker' and 'hook_worker'
   * activities (between their /ac and /au entries)
   */
  interleaveActions(target: JobAction, actions: ActivityAction[]) {
    if (target) {
      for (let i = target.cursor + 1; i < target.items.length; i++) {
        const [_, actionType, jobOrIndex] = target.items[i];
        actions.push({ action: actionType, target: jobOrIndex });
        target.cursor = i;
        if (this.isPausingAction(actionType)) {
          break;
        }
      }
    }
  }

  isPausingAction(actionType: string): boolean {
    return actionType === 'sleep' || actionType === 'waitForSignal';
  }

  isMainEntry(key: string): boolean {
    return key.startsWith('worker/') && key.endsWith('/ac');
  }

  isHookEntry(key: string): boolean {
    return key.startsWith('hook_worker/') && key.endsWith('/ac');
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
      if (parts[0] in this.activitySymbols) {
        parts[0] = this.activitySymbols[parts[0]];
      }
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
  inflateDependencyData(data: string[], actions: JobActionExport): DependencyExport[] {
    const hookReg = /([0-9,]+)-(\d+)$/;
    const flowReg = /-(\d+)$/;
    return data.map((dependency, index: number): DependencyExport => {
      const [action, topic, gid, _pd, ...jid] = dependency.split(VALSEP);
      const jobId = jid.join(VALSEP);
      const match = jobId.match(hookReg);
      let prefix: string;
      let type: 'hook' | 'flow' | 'other';
      let dimensionKey: string = '';
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
      this.seedActions(
        type,
        action,
        topic,
        dependency,
        prefix,
        dimensionKey,
        actions,
        jobId,
      );
      return {
        type: action,
        topic,
        gid,
        jid: jobId,
      } as unknown as DependencyExport;
    });
  }

  /**
   * Adds historical actions (proxyActivity, executeChild)
   * using the `dependency list` to determine
   * after-the-fact what happened within the 'black-box'
   * worker function. This is necessary to interleave the
   * actions into the replay timeline, given that it isn't
   * really possible to know the inner-workings of the user's
   * function
   *  
   */
  seedActions(type: 'flow'|'hook'|'other', action: string, topic: string, dep: string, prefix: string, dimensionKey: string, actions: JobActionExport, jobId: string) {
    if (type !== 'other' && action === 'expire-child') {
      let depType: string;
      if (topic == `${this.appId}.activity.execute`) {
        depType = 'proxyActivity';
      } else if (topic == `${this.appId}.execute`) {
        depType = 'executeChild';
      } else if (topic == `${this.appId}.wfsc.execute`) {
        depType = 'waitForSignal';
      }
      
      if (depType) {
        if (type === 'flow') {
          actions.main.items.push([prefix, depType, jobId]);
        } else if (type === 'hook') {
          if (!actions.hooks[dimensionKey]) {
            actions.hooks[dimensionKey] = {
              cursor: -1,
              items: [],
            };
          }
          actions.hooks[dimensionKey].items.push([prefix, depType, jobId]);
        }
      }
    }
  }

  /**
   * Inflates the job data from Redis into a DurableJobExport object
   * @param jobHash - the job data from Redis
   * @param dependencyList - the list of dependencies for the job
   * @returns - the inflated job data
   */
  inflate(jobHash: StringStringType, dependencyList: string[]): DurableJobExport {
    //the list of actions taken in the workflow and hook functions
    const actions: JobActionExport = {
      hooks: {},
      main: { cursor: -1, items: [] },
    };
    const dependencies = this.inflateDependencyData(dependencyList, actions);
    const state: StringStringType = {};
    const data: StringStringType = {};
    const other: ExportItem[] = [];
    const replay: ExportItem[] = [];
    const regex = /^([a-zA-Z]{3}),(\d+(?:,\d+)*)/;

    Object.entries(jobHash).forEach(([key, value]) => {
      const match = key.match(regex);
      if (match) {
        //activity process state
        this.inflateProcess(match, value, replay);
      } else if (key.length === 3) {
        //job state
        state[this.inflateKey(key)] = this.serializer.fromString(value);
      } else if (key.startsWith('_')) {
        //job data
        data[key.substring(1)] = value;
      } else if (key.startsWith('-')) {
        //actions with side effect (replayable)
        this.inflateActions(key, value, actions);
      } else {
        //collator guids, etc
        other.push([null, key, value]);
      }
    });

    replay.sort(this.dateSort)
    actions.main.items.sort(this.reverseSort);
    Object.entries(actions.hooks).forEach(([key, value]) => {
      value.items.sort(this.reverseSort);
    });
    
    return {
      data: restoreHierarchy(data),
      dependencies,
      state: Object.entries(restoreHierarchy(state))[0][1],
      status: jobHash[':'],
      timeline: this.createTimeline(replay, actions),
      transitions: { ...this.transitions },
      cycles: { ...this.cycles },
    };
  }

  inflateProcess(match: RegExpMatchArray, value: string, replay: ExportItem[]) {
    const [_, letters, numbers] = match;
    const path = this.inflateKey(letters);
    if (path.endsWith('/output/metadata/ac') ||
        path.endsWith('/output/metadata/au')) {
      const dimensions = `/${numbers.replace(/,/g, '/')}`;
      const resolved = this.serializer.fromString(value);
      replay.push([
        dimensions,
        path,
        resolved,
      ]);
    }
  }

  inflateActions(key: string, value: string, actions: JobActionExport) {
    let [_, dimensionalType, counter, subcounter] = key.split('-');
    if (subcounter) {
      counter = `${counter}.${subcounter}`;
    }
    const [type, ...dimensions] = dimensionalType.split(',');
    let dimensionKey = '';
    let isHook = false;
    if (dimensions.length > 0) {
      dimensionKey = `/${dimensions.join('/')}`;
      isHook = true;
    }
    let targetList: ExportItem[];
    if (isHook) {
      if (!actions.hooks[dimensionKey]) {
        actions.hooks[dimensionKey] = {
          cursor: -1,
          items: [],
        };
      }
      targetList = actions.hooks[dimensionKey].items;
    } else {
      targetList = actions.main.items;
    }
    targetList.push([
      `${dimensionKey}[${counter}]`,
      type,
      value,
    ]);
  }

  reverseSort(aKey: ExportItem, bKey: ExportItem) {
    if (aKey[0] > bKey[0]) {
      return 1;
    } else if (aKey[0] < bKey[0]) {
      return -1;
    } else {
      if (aKey[1] > bKey[1]) {
        return 1;
      } else if (aKey[1] < bKey[1]) {
        return -1;
      }
      return 0;
    }
  }

  dateSort(aKey: ExportItem, bKey: ExportItem) {
    if (aKey[2] > bKey[2]) {
      return 1;
    } else if (aKey[2] < bKey[2]) {
      return -1;
    } else {
      return 0;
    }
  }
}

export { ExporterService };
