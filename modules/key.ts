import { KeyStoreParams, KeyType } from '../types/hotmesh';

/**
 * Keys
 *
 * hmsh ->                                            {hash}    hotmesh config {version: "0.0.1", namespace: "hmsh"}
 * hmsh:a:<appid> ->                                  {hash}    app profile { "id": "appid", "version": "2", "versions/1": "GMT", "versions/2": "GMT"}
 * hmsh:<appid>:r: ->                                 {hash}    throttle rates {':': '23', 'topic.thing': '555'} => {':i': 'all', 'topic.thing': '555seconds'}
 * hmsh:<appid>:w: ->                                 {zset}    work items/tasks an engine must do like garbage collect or hook a set of matching records (hookAll)
 * hmsh:<appid>:t: ->                                 {zset}    an ordered set of list (work lists) ids
 * hmsh:<appid>:t:<timeValue?> ->                     {list}    a worklist of `jobId+activityId` items that should be awakened
 * hmsh:<appid>:q: ->                                 {hash}    quorum-wide messages
 * hmsh:<appid>:q:<ngnid> ->                          {hash}    engine-targeted messages (targeted quorum-oriented message)
 * hmsh:<appid>:j:<jobid> ->                          {hash}    job data
 * hmsh:<appid>:s:<jobkey>:<dateTime> ->              {hash}    job stats (general)
 * hmsh:<appid>:s:<jobkey>:<dateTime>:mdn:<field/path>:<fieldvalue> ->      {zset}    job stats (median)
 * hmsh:<appid>:s:<jobkey>:<dateTime>:index:<field/path>:<fieldvalue> ->    {list}    job stats (index of jobid[])
 * hmsh:<appid>:v:<version>:activities ->             {hash}    schemas [cache]
 * hmsh:<appid>:v:<version>:transitions ->            {hash}    transitions [cache]
 * hmsh:<appid>:v:<version>:subscriptions ->          {hash}    subscriptions [cache]
 * hmsh:<appid>:x: ->                                 {xstream} when an engine is sent or reads a buffered task (engines read from their custom topic)
 * hmsh:<appid>:x:<topic> ->                          {xstream} when a worker is sent or reads a buffered task (workers read from their custom topic)
 * hmsh:<appid>:hooks ->                              {hash}    hook patterns/rules; set at compile time
 * hmsh:<appid>:signals ->                            {string}  dynamic hook signals (hget/hdel); expirable
 * hmsh:<appid>:sym:keys: ->                          {hash}    list of symbol ranges and :cursor assigned at version deploy time for job keys
 * hmsh:<appid>:sym:keys:<activityid|$subscribes> ->  {hash}    list of symbols based upon schema enums (initially) and adaptively optimized (later) during runtime; if '$subscribes' is used as the activityid, it is a top-level `job` symbol set (for job keys)
 * hmsh:<appid>:sym:vals: ->                          {hash}    list of symbols for job values across all app versions
 */

const HMNS = 'hmsh';

const KEYSEP = ':'; // default delimiter for keys
const VALSEP = '::'; // default delimiter for vals
const WEBSEP = '::'; // default delimiter for webhook vals
const TYPSEP = '::'; // delimiter for ZSET task typing (how should a list be used?)

class KeyService {
  /**
   * Returns a key that can be used to access a value in the key/value store
   * appropriate for the given key type; the keys have an implicit hierarchy
   * and are used to organize data in the store in a tree-like structure
   * via the use of colons as separators.
   * @param namespace
   * @param keyType
   * @param params
   * @returns {string}
   */
  static mintKey(
    namespace: string,
    keyType: KeyType,
    params: KeyStoreParams,
  ): string {
    switch (keyType) {
      case KeyType.HOTMESH:
        return namespace;
      case KeyType.THROTTLE_RATE:
        return `${namespace}:${params.appId}:r:`;
      case KeyType.WORK_ITEMS:
        return `${namespace}:${params.appId}:w:${params.scoutType || ''}`;
      case KeyType.TIME_RANGE:
        return `${namespace}:${params.appId}:t:${params.timeValue || ''}`;
      case KeyType.APP:
        return `${namespace}:a:${params.appId || ''}`;
      case KeyType.QUORUM:
        return `${namespace}:${params.appId}:q:${params.engineId || ''}`;
      case KeyType.JOB_STATE:
        return `${namespace}:${params.appId}:j:${params.jobId}`;
      case KeyType.JOB_DEPENDENTS:
        return `${namespace}:${params.appId}:d:${params.jobId}`;
      case KeyType.JOB_STATS_GENERAL:
        return `${namespace}:${params.appId}:s:${params.jobKey}:${params.dateTime}`;
      case KeyType.JOB_STATS_MEDIAN:
        return `${namespace}:${params.appId}:s:${params.jobKey}:${params.dateTime}:${params.facet}`;
      case KeyType.JOB_STATS_INDEX:
        return `${namespace}:${params.appId}:s:${params.jobKey}:${params.dateTime}:${params.facet}`;
      case KeyType.SCHEMAS:
        return `${namespace}:${params.appId}:v:${params.appVersion}:schemas`;
      case KeyType.SUBSCRIPTIONS:
        return `${namespace}:${params.appId}:v:${params.appVersion}:subscriptions`;
      case KeyType.SUBSCRIPTION_PATTERNS:
        return `${namespace}:${params.appId}:v:${params.appVersion}:transitions`;
      case KeyType.HOOKS:
        return `${namespace}:${params.appId}:hooks`;
      case KeyType.SIGNALS:
        return `${namespace}:${params.appId}:signals`;
      case KeyType.SYMKEYS:
        return `${namespace}:${params.appId}:sym:keys:${params.activityId || ''}`;
      case KeyType.SYMVALS:
        return `${namespace}:${params.appId}:sym:vals:`;
      case KeyType.STREAMS:
        return `${namespace}:${params.appId || ''}:x:${params.topic || ''}`;
      default:
        throw new Error('Invalid key type.');
    }
  }

  /**
   * Extracts the parts of a given key string, safely handling cases where
   * the 'id' portion may contain additional colons.
   * @param key - The key to parse.
   * @returns An object with the parsed key parts.
   */
  static parseKey(key: string): Record<string, string | undefined> {
    const [namespace, appId, entity, ...rest] = key.split(KEYSEP);
    const id = rest.join(KEYSEP) || ''; // Join remaining parts to reconstruct the id

    return {
      namespace,
      app: entity === 'a' ? appId : undefined,
      entity,
      id,
    };
  }

  /**
   * Reconstructs a key string from its parts.
   * @param parts - An object with the key parts.
   * @returns The reconstructed key string.
   */
  static reconstituteKey(parts: Record<string, string | undefined>): string {
    const { namespace, app, entity, id } = parts;
    return `${namespace}${KEYSEP}${app}${KEYSEP}${entity}${KEYSEP}${id || ''}`;
  }

  /**
   * Resolves an entity type abbreviation to a table-friendly name.
   * @param abbreviation - The abbreviated entity type.
   * @returns The long-form entity name.
   */
  static resolveEntityType(abbreviation: string, id = ''): string {
    switch (abbreviation) {
      case 'a':
        return 'applications';
      case 'r':
        return 'throttles';
      case 'w':
        return id === '' ? 'task_priorities' : 'roles';
      case 't':
        return id === '' ? 'task_schedules' : 'task_lists';
      case 'q':
        return 'events';
      case 'j':
        return 'jobs';
      case 's':
        return 'stats';
      case 'v':
        return 'versions';
      case 'x':
        return id === '' ? 'streams' : 'stream_topics';
      case 'hooks':
        return 'signal_patterns';
      case 'signals':
        return 'signal_registry';
      case 'sym':
        return 'symbols';
      default:
        return 'unknown_entity';
    }
  }

  static resolveAbbreviation(entity: string): string {
    switch (entity) {
      case 'applications':
        return 'a';
      case 'throttles':
        return 'r';
      case 'roles':
        return 'w';
      case 'task_schedules':
        return 't';
      case 'task_lists':
        return 't';
      case 'events':
        return 'q';
      case 'jobs':
        return 'j';
      case 'stats':
        return 's';
      case 'versions':
        return 'v';
      case 'streams':
        return 'x';
      case 'signal_patterns':
        return 'hooks';
      case 'signal_registry':
        return 'signals';
      case 'symbols':
        return 'sym';
      default:
        return 'unknown_entity';
    }
  }
}

export {
  KeyService,
  KeyType,
  KeyStoreParams,
  HMNS,
  KEYSEP,
  TYPSEP,
  WEBSEP,
  VALSEP,
};
