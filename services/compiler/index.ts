import $RefParser from '@apidevtools/json-schema-ref-parser';
import yaml from 'js-yaml';
import * as fs from 'fs/promises';
import * as path from 'path';

import { ILogger } from '../logger';
import { StoreService } from '../store';
import { Deployer } from './deployer';
import { Validator } from './validator';
import { HotMeshManifest } from '../../types/hotmesh';
import { RedisClient, RedisMulti } from '../../types/redis';

/**
 * The compiler service converts a graph into a executable program.
 */
class CompilerService {
  store: StoreService<RedisClient, RedisMulti> | null;
  logger: ILogger;

  constructor(store: StoreService<RedisClient, RedisMulti>, logger: ILogger) {
    this.store = store;
    this.logger = logger;
  }

  /**
   * verifies and plans the deployment of an app to Redis; the app is not deployed yet
   * @param path 
   */
  async plan(mySchemaOrPath: string): Promise<HotMeshManifest> {
    try {
      let schema: HotMeshManifest;
      if (this.isPath(mySchemaOrPath)) {
        schema = await $RefParser.dereference(mySchemaOrPath) as HotMeshManifest;
      } else {
        schema = yaml.load(mySchemaOrPath) as HotMeshManifest;
      }

      // 1) validate the manifest file
      const validator = new Validator(schema);
      validator.validate(this.store);

      // 2) todo: add a PlannerService module that will plan the deployment (what might break, drift, etc)
      return schema as HotMeshManifest
    } catch(err) {
      this.logger.error('compiler-plan-error', err);
    }
  }

  isPath(input: string): boolean {
    return !input.trim().startsWith('app:');
  }

  /**
   * deploys an app to Redis but does NOT activate it.
   */
  async deploy(mySchemaOrPath: string): Promise<HotMeshManifest> {
    try {
      let schema: HotMeshManifest;
      if (this.isPath(mySchemaOrPath)) {
        schema = await $RefParser.dereference(mySchemaOrPath) as HotMeshManifest;
        await this.saveAsJSON(mySchemaOrPath, schema);
      } else {
        schema = yaml.load(mySchemaOrPath) as HotMeshManifest;
      }

      // 2) validate the manifest file (synchronous operation...no callbacks)
      const validator = new Validator(schema);
      validator.validate(this.store);

      // 3) deploy the schema (segment, optimize, etc; save to Redis)
      const deployer = new Deployer(schema);
      await deployer.deploy(this.store);

      // 4) save the app version to Redis (so it can be activated later)
      await this.store.setApp(schema.app.id, schema.app.version);
      return schema;
    } catch(err) {
      this.logger.error('compiler-deploy-error', err);
    }
  }

  /**
   * activates a deployed version of an app;
   * @param appId 
   * @param appVersion 
   */
  async activate(appId: string, appVersion: string): Promise<boolean> {
    return await this.store.activateAppVersion(appId, appVersion);
  }

  async saveAsJSON(originalPath: string, schema: HotMeshManifest): Promise<void> {
    const json = JSON.stringify(schema, null, 2);
    const newPath = path.join( path.dirname(originalPath), `.hotmesh.${schema.app.id}.${schema.app.version}.json` );
    await fs.writeFile(newPath, json, 'utf8');
  }
}

export { CompilerService };
