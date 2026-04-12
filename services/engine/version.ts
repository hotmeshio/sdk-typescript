/**
 * App version resolution and distributed cache management.
 *
 * The engine caches the deployed app version locally to avoid
 * hitting the store on every message. During hot-deploys the cache
 * switches to `nocache` mode until the target version appears,
 * then resumes caching.
 */

import { HMSH_QUORUM_DELAY_MS } from '../../modules/enums';
import { sleepFor } from '../../modules/utils';
import { ILogger } from '../logger';
import { StoreService } from '../store';
import { AppVID } from '../../types/app';
import { CacheMode } from '../../types/cache';
import { HotMeshApps } from '../../types/hotmesh';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

interface VersionContext {
  appId: string;
  guid: string;
  apps: HotMeshApps | null;
  cacheMode: CacheMode;
  untilVersion: string | null;
  store: StoreService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
}

/**
 * Resolves the distributed executable version, retrying with a delay
 * to allow deployment race conditions to settle.
 */
export async function fetchAndVerifyVID(
  instance: VersionContext,
  vid: AppVID,
  count = 0,
): Promise<AppVID> {
  if (isNaN(Number(vid.version))) {
    const app = await instance.store.getApp(vid.id, true);
    if (!isNaN(Number(app.version))) {
      if (!instance.apps) instance.apps = {};
      instance.apps[vid.id] = app;
      return { id: vid.id, version: app.version };
    } else if (count < 10) {
      await sleepFor(HMSH_QUORUM_DELAY_MS * 2);
      return await fetchAndVerifyVID(instance, vid, count + 1);
    } else {
      instance.logger.error('engine-vid-resolution-error', {
        id: vid.id,
        guid: instance.guid,
      });
    }
  }
  return vid;
}

export async function getVID(
  instance: VersionContext,
  vid?: AppVID,
): Promise<AppVID> {
  if (instance.cacheMode === 'nocache') {
    const app = await instance.store.getApp(instance.appId, true);
    if (app.version.toString() === instance.untilVersion.toString()) {
      if (!instance.apps) instance.apps = {};
      instance.apps[instance.appId] = app;
      setCacheMode(instance, 'cache', app.version.toString());
    }
    return { id: instance.appId, version: app.version };
  } else if (!instance.apps && vid) {
    instance.apps = {};
    instance.apps[instance.appId] = vid;
    return vid;
  } else {
    return await fetchAndVerifyVID(instance, {
      id: instance.appId,
      version: instance.apps?.[instance.appId].version,
    });
  }
}

export function setCacheMode(
  instance: VersionContext,
  cacheMode: CacheMode,
  untilVersion: string,
) {
  instance.logger.info(`engine-executable-cache`, {
    mode: cacheMode,
    [cacheMode === 'cache' ? 'target' : 'until']: untilVersion,
  });
  instance.cacheMode = cacheMode;
  instance.untilVersion = untilVersion;
}
