/**
 * The Cache is a key/value store and used to store commonly accessed Redis metadata
 * (mainly the execution rules for the app) to save time accessing them as they
 * are immutable per verison. Rules are only ejected when a new version
 * (a new distributed executable) is deployed to the quorum
 * and the cache is invalidated/cleared of the prior version.
 */

import { ActivityType } from "../../types/activity";
import { HookRule } from "../../types/hook";
import { HotMeshApp, HotMeshSettings } from "../../types/hotmesh";
import { Symbols } from "../../types/serializer";
import { Transitions } from "../../types/transition";

class Cache {
  settings: HotMeshSettings;
  appId: string;
  apps: Record<string, HotMeshApp>;
  schemas: Record<string, ActivityType>;
  subscriptions: Record<string, Record<string, string>>;
  symbols: Record<string, Symbols>;
  symvals: Record<string, Symbols>;
  transitions: Record<string, Record<string, unknown>>;
  hookRules: Record<string, Record<string, HookRule[]>>;
  workItems: Record<string, string>;

  /**
   * The cache is ALWAYS initialized with HotMeshSettings. The other parameters are optional.
   * @param settings 
   * @param apps 
   * @param schemas 
   * @param subscriptions 
   * @param transitions 
   * @param hookRules 
   */
  constructor(appId: string, settings: HotMeshSettings, apps: Record<string, HotMeshApp> = {}, schemas: Record<string, ActivityType> = {}, subscriptions: Record<string, Record<string, string>> = {}, symbols: Record<string, Symbols> = {}, symvals: Record<string, Symbols> = {}, transitions: Record<string, Record<string, unknown>> = {}, hookRules: Record<string, Record<string, HookRule[]>> = {}, workItems: Record<string, string> = {}) {
    this.appId = appId;
    this.settings = settings;
    this.apps = apps;
    this.schemas = schemas;
    this.subscriptions = subscriptions;
    this.symbols = symbols;
    this.symvals = symvals;
    this.transitions = transitions;
    this.hookRules = hookRules;
    this.workItems = workItems;
  }

  /**
   * invalidate the cache; settings are not invalidated!
   */
  invalidate(): void {
    this.apps = {} as Record<string, HotMeshApp>;
    this.schemas = {};
    this.subscriptions = {};
    this.transitions = {};
    this.hookRules = {};
  }

  getSettings(): HotMeshSettings {
    return this.settings;
  }

  setSettings(settings: HotMeshSettings): void {
    this.settings = settings;
  }

  getApps(): Record<string, HotMeshApp> {
    return this.apps;
  }

  getApp(appId: string): HotMeshApp {
    return this.apps[appId] as HotMeshApp;
  }

  setApps(apps: Record<string, HotMeshApp>): void {
    this.apps = apps;
  }

  setApp(appId: string, app: HotMeshApp): void {
    this.apps[appId] = app;
  }

  getSchemas(appId: string, version: string): Record<string, ActivityType> {
    return this.schemas[`${appId}/${version}`] as unknown as Record<string, ActivityType>;
  }

  getSchema(appId: string, version: string, activityId: string): ActivityType {
    return this.schemas?.[`${appId}/${version}`]?.[activityId] as ActivityType;
  }

  setSchemas(appId: string, version: string, schemas: Record<string, ActivityType>): void {
    this.schemas[`${appId}/${version}`] = schemas as unknown as Record<string, ActivityType>;
  }

  setSchema(appId: string, version: string, activityId: string, schema: ActivityType): void {
    this.schemas[`${appId}/${version}`][activityId] = schema;
  }

  getSubscriptions(appId: string, version: string): Record<string, string> {
    return this.subscriptions[`${appId}/${version}`];
  }

  getSubscription(appId: string, version: string, topic: string): unknown {
    return this.subscriptions?.[`${appId}/${version}`]?.[topic];
  }

  setSubscriptions(appId: string, version: string, subscriptions: Record<string, string>): void {
    this.subscriptions[`${appId}/${version}`] = subscriptions;
  }

  getSymbols(appId: string, targetEntityId: string): Symbols {
    return this.symbols[`${appId}/${targetEntityId}`] as Symbols;
  }

  setSymbols(appId: string, targetEntityId: string, symbols: Symbols): void {
    this.symbols[`${appId}/${targetEntityId}`] = symbols;
  }

  deleteSymbols(appId: string, targetEntityId: string): void {
    delete this.symbols[`${appId}/${targetEntityId}`];
  }

  getSymbolValues(appId: string): Symbols {
    return this.symvals[`${appId}`] as Symbols;
  }

  setSymbolValues(appId: string, symvals: Symbols): void {
    this.symvals[`${appId}`] = symvals;
  }

  deleteSymbolValues(appId: string): void {
    delete this.symvals[`${appId}`];
  }

  getTransitions(appId: string, version: string): Transitions {
    return this.transitions[`${appId}/${version}`] as Transitions;
  }

  setTransitions(appId: string, version: string, transitions: Transitions): void {
    this.transitions[`${appId}/${version}`] = transitions;
  }

  getHookRules(appId: string): Record<string, HookRule[]> {
    return this.hookRules[`${appId}`];
  }

  setHookRules(appId: string, hookRules: Record<string, HookRule[]>): void {
    this.hookRules[`${appId}`] = hookRules;
  }

  getSignals(appId: string, version: string): Record<string, unknown> {
    throw new Error("SIGNAL (getHooks) is not supported");
  }

  setSignals(appId: string, version: string): Record<string, unknown> {
    throw new Error("SIGNAL (setHook) is not supported");
  }

  getActiveTaskQueue(appId: string): string {
    return this.workItems[appId];
  }

  setWorkItem(appId: string, workItem: string): void {
    this.workItems[appId] = workItem;
  }

  removeWorkItem(appId: string): void {
    delete this.workItems[appId];
  }
}

export { Cache };
