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
 * hmsh:<appid>:signals ->                            {hash}    dynamic hook signals (hget/hdel) when resolving (always self-clean); added/removed at runtime
 * hmsh:<appid>:sym:keys: ->                          {hash}    list of symbol ranges and :cursor assigned at version deploy time for job keys
 * hmsh:<appid>:sym:keys:<activityid|$subscribes> ->  {hash}    list of symbols based upon schema enums (initially) and adaptively optimized (later) during runtime; if '$subscribes' is used as the activityid, it is a top-level `job` symbol set (for job keys)
 * hmsh:<appid>:sym:vals: ->                          {hash}    list of symbols for job values across all app versions
 */

const HMNS = 'hmsh';

const KEYSEP = ':'; //default delimiter for keys
const VALSEP = '::'; //default delimiter for vals
const WEBSEP = '::'; //default delimiter for webhook vals
const TYPSEP = '::'; //delimiter for ZSET task typing (how should a list be used?)

class KeyService {
  /**
   * returns a key that can be used to access a value in the key/value store
   * appropriate for the given key type; the keys have an implicit hierarchy
   * and are used to organize data in the store in a tree-like structure
   * via the use of colons as separators. The top-level entity is the hmsh manifest.
   * This file will reveal the full scope of what is on the server (apps, versions, etc)
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
        //`hooks` provide the pattern to resolve a value
        return `${namespace}:${params.appId}:hooks`;
      case KeyType.SIGNALS:
        //`signals` provide the registry of resolved values that link back to paused jobs
        return `${namespace}:${params.appId}:signals`;
      case KeyType.SYMKEYS:
        //`symbol keys` provide the registry of replacement values for job keys
        return `${namespace}:${params.appId}:sym:keys:${params.activityId || ''}`;
      case KeyType.SYMVALS:
        //`symbol vals` provide the registry of replacement values for job vals
        return `${namespace}:${params.appId}:sym:vals:`;
      case KeyType.STREAMS:
        return `${namespace}:${params.appId || ''}:x:${params.topic || ''}`;
      default:
        throw new Error('Invalid key type.');
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
