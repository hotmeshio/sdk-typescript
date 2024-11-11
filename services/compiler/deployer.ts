import { KeyStoreParams, KeyType } from '../../modules/key';
import { getSymKey } from '../../modules/utils';
import { CollatorService } from '../collator';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import { ActivityType } from '../../types/activity';
import { HookRule } from '../../types/hook';
import { HotMeshGraph, HotMeshManifest } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import { StringAnyType, Symbols } from '../../types/serializer';
import { Pipe } from '../pipe';
import { StreamService } from '../stream';

import { Validator } from './validator';

const DEFAULT_METADATA_RANGE_SIZE = 26; //metadata is 26 slots ([a-z] * 1)
const DEFAULT_DATA_RANGE_SIZE = 260; //data is 260 slots ([a-zA-Z] * 5)
const DEFAULT_RANGE_SIZE =
  DEFAULT_METADATA_RANGE_SIZE + DEFAULT_DATA_RANGE_SIZE;

class Deployer {
  manifest: HotMeshManifest | null = null;
  store: StoreService<ProviderClient, ProviderTransaction> | null;
  stream: StreamService<ProviderClient, ProviderTransaction> | null;

  constructor(manifest: HotMeshManifest) {
    this.manifest = manifest;
  }

  async deploy(
    store: StoreService<ProviderClient, ProviderTransaction>,
    stream: StreamService<ProviderClient, ProviderTransaction>,
  ) {
    this.store = store;
    this.stream = stream;
    CollatorService.compile(this.manifest.app.graphs);
    this.convertActivitiesToHooks();
    this.convertTopicsToTypes();
    this.copyJobSchemas();
    this.bindBackRefs();
    this.bindParents();
    this.bindCycleTarget();
    this.resolveMappingDependencies();
    this.resolveJobMapsPaths();
    await this.generateSymKeys();
    await this.generateSymVals();
    await this.deployHookPatterns();
    await this.deployActivitySchemas();
    await this.deploySubscriptions();
    await this.deployTransitions();
    await this.deployConsumerGroups();
  }

  getVID() {
    return {
      id: this.manifest.app.id,
      version: this.manifest.app.version,
    };
  }

  async generateSymKeys() {
    //note: symbol ranges are additive (per version); path assignments are immutable
    for (const graph of this.manifest.app.graphs) {
      //generate JOB symbols
      const [, trigger] = this.findTrigger(graph);
      const topic = trigger.subscribes;
      const [lower, upper, symbols] = await this.store.reserveSymbolRange(
        `$${topic}`,
        DEFAULT_RANGE_SIZE,
        'JOB',
      );
      const prefix = ''; //job meta/data is NOT namespaced
      const newSymbols = this.bindSymbols(
        lower,
        upper,
        symbols,
        prefix,
        trigger.PRODUCES,
      );
      if (Object.keys(newSymbols).length) {
        await this.store.addSymbols(`$${topic}`, newSymbols);
      }
      //generate ACTIVITY symbols
      for (const [activityId, activity] of Object.entries(graph.activities)) {
        const [lower, upper, symbols] = await this.store.reserveSymbolRange(
          activityId,
          DEFAULT_RANGE_SIZE,
          'ACTIVITY',
        );
        const prefix = `${activityId}/`; //activity meta/data is namespaced
        this.bindSelf(activity.consumes, activity.produces, activityId);
        const newSymbols = this.bindSymbols(
          lower,
          upper,
          symbols,
          prefix,
          activity.produces,
        );
        if (Object.keys(newSymbols).length) {
          await this.store.addSymbols(activityId, newSymbols);
        }
      }
    }
  }

  bindSelf(
    consumes: Record<string, string[]>,
    produces: string[],
    activityId: string,
  ) {
    //bind self-referential mappings
    for (const selfId of [activityId, '$self']) {
      const selfConsumes = consumes[selfId];
      if (selfConsumes) {
        for (const path of selfConsumes) {
          if (!produces.includes(path)) {
            produces.push(path);
          }
        }
      }
    }
  }

  bindSymbols(
    startIndex: number,
    maxIndex: number,
    existingSymbols: Symbols,
    prefix: string,
    produces: string[],
  ): Symbols {
    const newSymbols: Symbols = {};
    const currentSymbols: Symbols = { ...existingSymbols };
    for (const path of produces) {
      const fullPath = `${prefix}${path}`;
      if (!currentSymbols[fullPath]) {
        if (startIndex > maxIndex) {
          throw new Error('Symbol index out of bounds');
        }
        const symbol = getSymKey(startIndex);
        startIndex++;
        newSymbols[fullPath] = symbol;
        currentSymbols[fullPath] = symbol; // update the currentSymbols to include this new symbol
      }
    }
    return newSymbols;
  }

  copyJobSchemas() {
    const graphs = this.manifest!.app.graphs;
    for (const graph of graphs) {
      const jobSchema = graph.output?.schema;
      const outputSchema = graph.input?.schema;
      if (!jobSchema && !outputSchema) continue;
      const activities = graph.activities;
      // Find the trigger activity and bind the job schema to it
      // at execution time, the trigger is a standin for the job
      for (const activityKey in activities) {
        if (activities[activityKey].type === 'trigger') {
          const trigger = activities[activityKey];
          if (jobSchema) {
            //possible for trigger to have job mappings
            if (!trigger.job) {
              trigger.job = {};
            }
            trigger.job.schema = jobSchema;
          }
          if (outputSchema) {
            //impossible for trigger to have output mappings.
            trigger.output = { schema: outputSchema };
          }
        }
      }
    }
  }

  bindBackRefs() {
    for (const graph of this.manifest!.app.graphs) {
      const activities = graph.activities;
      const triggerId = this.findTrigger(graph)[0];
      for (const activityKey in activities) {
        activities[activityKey].trigger = triggerId;
        activities[activityKey].subscribes = graph.subscribes;
        if (graph.publishes) {
          activities[activityKey].publishes = graph.publishes;
        }
        activities[activityKey].expire = graph.expire ?? undefined;
        activities[activityKey].persistent = graph.persistent ?? undefined;
      }
    }
  }

  //the cycle/goto activity includes and ancestor target;
  //update with the cycle flag, so it can be rerun
  bindCycleTarget() {
    for (const graph of this.manifest!.app.graphs) {
      const activities = graph.activities;
      for (const activityKey in activities) {
        const activity = activities[activityKey];
        if (activity.type === 'cycle') {
          activities[activity.ancestor].cycle = true;
        }
      }
    }
  }

  //it's more intuitive for SDK users to use 'topic',
  //but the compiler is desiged to be generic and uses the attribute, 'subtypes'
  convertTopicsToTypes() {
    for (const graph of this.manifest!.app.graphs) {
      const activities = graph.activities;
      for (const activityKey in activities) {
        const activity = activities[activityKey];
        if (
          ['worker', 'await'].includes(activity.type) &&
          activity.topic &&
          !activity.subtype
        ) {
          activity.subtype = activity.topic;
        }
      }
    }
  }

  //legacy; remove at beta (assume no legacy refs to 'activity' at that point)
  convertActivitiesToHooks() {
    for (const graph of this.manifest!.app.graphs) {
      const activities = graph.activities;
      for (const activityKey in activities) {
        const activity = activities[activityKey];
        if (['activity'].includes(activity.type)) {
          activity.type = 'hook';
        }
      }
    }
  }

  async bindParents() {
    const graphs = this.manifest.app.graphs;
    for (const graph of graphs) {
      if (graph.transitions) {
        for (const fromActivity in graph.transitions) {
          const toTransitions = graph.transitions[fromActivity];
          for (const transition of toTransitions) {
            const to = transition.to;
            //DAGs have one parent; easy to optimize for
            graph.activities[to].parent = fromActivity;
          }
          //temporarily bind the transitions to the parent activity,
          // so the consumer/producer registrar picks up the bindings
          graph.activities[fromActivity].transitions = toTransitions;
        }
      }
    }
  }

  collectValues(schema: Record<string, any>, values: Set<string>) {
    for (const [key, value] of Object.entries(schema)) {
      if (key === 'enum' || key === 'examples' || key === 'default') {
        if (Array.isArray(value)) {
          for (const v of value) {
            if (typeof v === 'string' && v.length > 5) {
              values.add(v);
            }
          }
        } else if (typeof value === 'string' && value.length > 5) {
          values.add(value);
        }
      } else if (typeof value === 'object') {
        this.collectValues(value, values);
      }
    }
  }

  traverse(obj: any, values: Set<string>) {
    for (const value of Object.values(obj)) {
      if (typeof value === 'object') {
        if ('schema' in value) {
          this.collectValues(value.schema, values);
        } else {
          this.traverse(value, values);
        }
      }
    }
  }

  async generateSymVals() {
    const uniqueStrings = new Set<string>();
    for (const graph of this.manifest!.app.graphs) {
      this.traverse(graph, uniqueStrings);
    }
    const existingSymbols = await this.store.getSymbolValues();
    const startIndex = Object.keys(existingSymbols).length;
    const maxIndex = Math.pow(52, 2) - 1;
    const newSymbols = SerializerService.filterSymVals(
      startIndex,
      maxIndex,
      existingSymbols,
      uniqueStrings,
    );
    await this.store.addSymbolValues(newSymbols);
  }

  resolveJobMapsPaths() {
    function parsePaths(obj: StringAnyType): string[] {
      const result = [];
      function traverse(obj: StringAnyType, path = []) {
        for (const key in obj) {
          if (
            typeof obj[key] === 'object' &&
            obj[key] !== null &&
            !('@pipe' in obj[key])
          ) {
            const newPath = [...path, key];
            traverse(obj[key], newPath);
          } else {
            //wildcard mapping (e.g., 'friends[25]')
            //when this is resolved, it will be expanded to
            //`'friends/0', ..., 'friends/24'`, providing 25 dynamic
            //slots in the flow's output data
            const pathName = [...path, key].join('/');
            if (!pathName.includes('[')) {
              const finalPath = `data/${pathName}`;
              if (!result.includes(finalPath)) {
                result.push(finalPath);
              }
            } else {
              const [left, right] = pathName.split('[');
              //check if this variable isLiteralKeyType (#, -, or _)
              const [amount, _] = right.split(']');
              if (!isNaN(parseInt(amount))) {
                //loop to create all possible paths (0 to amount)
                for (let i = 0; i < parseInt(amount); i++) {
                  const finalPath = `data/${left}/${i}`;
                  if (!result.includes(finalPath)) {
                    result.push(finalPath);
                  }
                }
              } //else ignore (amount might be '-' or '_')  `-` is marker data;   `_` is job data;
            }
          }
        }
      }
      if (obj) {
        traverse(obj);
      }
      return result;
    }

    for (const graph of this.manifest.app.graphs) {
      let results: string[] = [];
      const [, trigger] = this.findTrigger(graph);
      for (const activityKey in graph.activities) {
        const activity = graph.activities[activityKey];
        results = results.concat(parsePaths(activity.job?.maps));
      }
      trigger.PRODUCES = results;
    }
  }

  resolveMappingDependencies() {
    const dynamicMappingRules: string[] = [];
    //recursive function to descend into the object and find all dynamic mapping rules
    function traverse(obj: StringAnyType, consumes: string[]): void {
      for (const key in obj) {
        if (typeof obj[key] === 'string') {
          const stringValue = obj[key] as string;
          const dynamicMappingRuleMatch = stringValue.match(/^\{[^@].*}$/);
          if (
            dynamicMappingRuleMatch &&
            !Validator.CONTEXT_VARS.includes(stringValue)
          ) {
            if (stringValue.split('.')[1] !== 'input') {
              dynamicMappingRules.push(stringValue);
              consumes.push(stringValue);
            }
          }
        } else if (typeof obj[key] === 'object' && obj[key] !== null) {
          traverse(obj[key], consumes);
        }
      }
    }
    const graphs = this.manifest.app.graphs;
    for (const graph of graphs) {
      const activities = graph.activities;
      for (const activityId in activities) {
        const activity = activities[activityId];
        activity.consumes = [];
        traverse(activity, activity.consumes);
        activity.consumes = this.groupMappingRules(activity.consumes);
      }
    }
    const groupedRules = this.groupMappingRules(dynamicMappingRules);
    // Iterate through the graph and add 'produces' field to each activity
    for (const graph of graphs) {
      const activities = graph.activities;
      for (const activityId in activities) {
        const activity = activities[activityId];
        activity.produces = groupedRules[`${activityId}`] || [];
      }
    }
  }

  groupMappingRules(rules: string[]): Record<string, string[]> {
    rules = Array.from(new Set(rules)).sort();
    // Group by the first symbol before the period (this is the activity name)
    const groupedRules: { [key: string]: string[] } = {};
    for (const rule of rules) {
      const [group, resolved] = this.resolveMappableValue(rule);
      if (!groupedRules[group]) {
        groupedRules[group] = [];
      }
      groupedRules[group].push(resolved);
    }
    return groupedRules;
  }

  resolveMappableValue(mappable: string): [string, string] {
    mappable = mappable.substring(1, mappable.length - 1);
    const parts = mappable.split('.');
    if (parts[0] === '$job') {
      const [group, ...path] = parts;
      return [group, path.join('/')];
    } else {
      //normalize paths to be relative to the activity
      const [group, type, subtype, ...path] = parts;
      const prefix = {
        hook: 'hook/data',
        input: 'input/data',
        output: subtype === 'data' ? 'output/data' : 'output/metadata',
      }[type];
      return [group, `${prefix}/${path.join('/')}`];
    }
  }

  async deployActivitySchemas() {
    const graphs = this.manifest!.app.graphs;
    const activitySchemas: Record<string, ActivityType> = {};
    for (const graph of graphs) {
      const activities = graph.activities;
      for (const activityKey in activities) {
        const target = activities[activityKey];
        //remove transitions; no longer necessary for runtime
        delete target.transitions;
        activitySchemas[activityKey] = target;
      }
    }
    await this.store.setSchemas(activitySchemas, this.getVID());
  }

  async deploySubscriptions() {
    const graphs = this.manifest!.app.graphs;
    const publicSubscriptions: { [key: string]: string } = {};
    for (const graph of graphs) {
      const activities = graph.activities;
      const subscribesTopic = graph.subscribes;
      // Find the activity ID associated with the subscribes topic
      for (const activityKey in activities) {
        if (activities[activityKey].type === 'trigger') {
          publicSubscriptions[subscribesTopic] = activityKey;
          break;
        }
      }
    }
    await this.store.setSubscriptions(publicSubscriptions, this.getVID());
  }

  findTrigger(graph: HotMeshGraph): [string, Record<string, any>] | null {
    for (const activityKey in graph.activities) {
      const activity = graph.activities[activityKey];
      if (activity.type === 'trigger') {
        return [activityKey, activity];
      }
    }
    return null;
  }

  async deployTransitions() {
    const graphs = this.manifest!.app.graphs;
    const privateSubscriptions: { [key: string]: any } = {};
    for (const graph of graphs) {
      if (graph.subscribes && graph.subscribes.startsWith('.')) {
        const [triggerId] = this.findTrigger(graph);
        if (triggerId) {
          privateSubscriptions[graph.subscribes] = { [triggerId]: true };
        }
      }
      if (graph.transitions) {
        for (const fromActivity in graph.transitions) {
          const toTransitions = graph.transitions[fromActivity];
          const toValues: { [key: string]: any } = {};
          for (const transition of toTransitions) {
            const to = transition.to;
            if (transition.conditions) {
              toValues[to] = transition.conditions;
            } else {
              toValues[to] = true;
            }
          }
          if (Object.keys(toValues).length > 0) {
            privateSubscriptions['.' + fromActivity] = toValues;
          }
        }
      }
    }
    await this.store.setTransitions(privateSubscriptions, this.getVID());
  }

  async deployHookPatterns() {
    const graphs = this.manifest!.app.graphs;
    const hookRules: Record<string, HookRule[]> = {};
    for (const graph of graphs) {
      if (graph.hooks) {
        for (const topic in graph.hooks) {
          hookRules[topic] = graph.hooks[topic];
          const activityId = graph.hooks[topic][0].to;
          const targetActivity = graph.activities[activityId];
          if (targetActivity) {
            if (!targetActivity.hook) {
              targetActivity.hook = {};
            }
            //create back-reference to the hook topic
            targetActivity.hook.topic = topic;
          }
        }
      }
    }
    await this.store.setHookRules(hookRules);
  }

  async deployConsumerGroups() {
    //create one engine group
    const params: KeyStoreParams = { appId: this.manifest.app.id };
    const key = this.store.mintKey(KeyType.STREAMS, params);
    await this.deployConsumerGroup(key, 'ENGINE');
    for (const graph of this.manifest.app.graphs) {
      const activities = graph.activities;
      for (const activityKey in activities) {
        const activity = activities[activityKey];
        //only precreate if the topic is concrete and not `mappable`
        if (
          activity.type === 'worker' &&
          Pipe.resolve(activity.subtype, {}) === activity.subtype
        ) {
          params.topic = activity.subtype;
          const key = this.store.mintKey(KeyType.STREAMS, params);
          //create one worker group per unique activity subtype (the topic)
          await this.deployConsumerGroup(key, 'WORKER');
        }
      }
    }
  }

  async deployConsumerGroup(stream: string, group: string) {
    try {
      await this.stream.createConsumerGroup(stream, group);
    } catch (err) {
      this.store.logger.info('router-stream-group-exists', { stream, group });
    }
  }
}

export { Deployer };
