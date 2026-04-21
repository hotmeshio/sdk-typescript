/**
 * Activity schema lookup and handler instantiation.
 *
 * Every stream message targets an activity node in the compiled DAG.
 * This module resolves the activity's schema (from the store) and
 * creates the correct handler subclass (Trigger, Worker, Hook, …).
 */

import { formatISODate } from '../../modules/utils';
import Activities from '../activities';
import { StoreService } from '../store';
import { ILogger } from '../logger';
import { AppVID } from '../../types/app';
import {
  ActivityMetadata,
  ActivityType,
} from '../../types/activity';
import { JobState, JobData } from '../../types/job';
import { ProviderClient, ProviderTransaction } from '../../types/provider';

interface SchemaContext {
  appId: string;
  store: StoreService<ProviderClient, ProviderTransaction>;
  logger: ILogger;
  getVID(vid?: AppVID): Promise<AppVID>;
}

export async function initActivity(
  instance: SchemaContext,
  topic: string,
  data: JobData = {},
  context?: JobState,
) {
  const [activityId, schema] = await getSchema(instance, topic);
  if (!schema) {
    const err = new Error(
      `Activity schema not found for "${activityId}" (topic: ${topic}) in app ${instance.appId}. ` +
      `This is typically caused by a worker activity whose topic collides with the graph subscribes topic. ` +
      `Redeploy with a distinct worker topic.`,
    );
    (err as any).code = 598;
    throw err;
  }
  const ActivityHandler = Activities[schema.type];
  if (ActivityHandler) {
    const utc = formatISODate(new Date());
    const metadata: ActivityMetadata = {
      aid: activityId,
      atp: schema.type,
      stp: schema.subtype,
      ac: utc,
      au: utc,
    };
    const hook = null;
    return new ActivityHandler(schema, data, metadata, hook, instance, context);
  } else {
    throw new Error(`activity type ${schema.type} not found`);
  }
}

export async function getSchema(
  instance: SchemaContext,
  topic: string,
): Promise<[activityId: string, schema: ActivityType]> {
  const app = (await instance.store.getApp(instance.appId)) as AppVID;
  if (!app) {
    throw new Error(`no app found for id ${instance.appId}`);
  }
  if (isPrivate(topic)) {
    const activityId = topic.substring(1);
    const schema = await instance.store.getSchema(
      activityId,
      await instance.getVID(app),
    );
    return [activityId, schema];
  } else {
    const activityId = await instance.store.getSubscription(
      topic,
      await instance.getVID(app),
    );
    if (activityId) {
      const schema = await instance.store.getSchema(
        activityId,
        await instance.getVID(app),
      );
      return [activityId, schema];
    }
  }
  throw new Error(
    `no subscription found for topic ${topic} in app ${instance.appId} for app version ${app.version}`,
  );
}

export function isPrivate(topic: string): boolean {
  return topic.startsWith('.');
}
