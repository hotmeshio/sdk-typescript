import { nanoid } from 'nanoid';
import { PSNS } from '../../modules/key';
import { EngineService } from '../engine';
import { LoggerService, ILogger } from '../logger';
import { StreamSignaler } from '../signaler/stream';
import { QuorumService } from '../quorum';
import { WorkerService } from '../worker';
import {
  JobState,
  JobData,
  JobOutput, 
  JobStatus } from '../../types/job';
import {
  HotMeshConfig,
  HotMeshManifest } from '../../types/hotmesh';
import { JobMessageCallback } from '../../types/quorum';
import {
  JobStatsInput,
  GetStatsOptions,
  IdsResponse,
  StatsResponse } from '../../types/stats';
import { ConnectorService } from '../connector';
import { StreamData, StreamDataResponse } from '../../types/stream';

class HotMeshService {
  namespace: string;
  appId: string;
  guid: string;
  engine: EngineService | null = null;
  quorum: QuorumService | null = null;
  workers: WorkerService[] = [];
  logger: ILogger;

  verifyAndSetNamespace(namespace?: string) {
    if (!namespace) {
      this.namespace = PSNS;
    } else if (!namespace.match(/^[A-Za-z0-9-]+$/)) {
      throw new Error(`config.namespace [${namespace}] is invalid`);
    } else {
      this.namespace = namespace;
    }
  }

  verifyAndSetAppId(appId: string) {
    if (!appId?.match(/^[A-Za-z0-9-]+$/)) {
      throw new Error(`config.appId [${appId}] is invalid`);
    } else if (appId === 'a') {
      throw new Error(`config.appId [${appId}] is reserved`);
    } else {
      this.appId = appId;
    }
  }

  static async init(config: HotMeshConfig) {
    const instance = new HotMeshService();
    instance.guid = nanoid();
    instance.verifyAndSetNamespace(config.namespace);
    instance.verifyAndSetAppId(config.appId);
    instance.logger = new LoggerService(config.appId, instance.guid, config.name || '', config.logLevel);
    await instance.initEngine(config, instance.logger);
    await instance.initQuorum(config, instance.engine, instance.logger);
    await instance.initWorkers(config, instance.logger);
    return instance;
  }

  async initEngine(config: HotMeshConfig, logger: ILogger): Promise<void> {
    if (config.engine) {
      await ConnectorService.initRedisClients(
        config.engine.redis?.class,
        config.engine.redis?.options,
        config.engine,
      );
      this.engine = await EngineService.init(
        this.namespace,
        this.appId,
        this.guid,
        config,
        logger,
      );
    }
  }

  async initQuorum(config: HotMeshConfig, engine: EngineService, logger: ILogger): Promise<void> {
    if (engine) {
      this.quorum = await QuorumService.init(
        this.namespace,
        this.appId,
        this.guid,
        config,
        engine,
        logger
      );
    }
  }

  async initWorkers(config: HotMeshConfig, logger: ILogger) {
    this.workers = await WorkerService.init(
      this.namespace,
      this.appId,
      this.guid,
      config,
      logger
    );
  }

  // ************* PUB/SUB METHODS *************
  async pub(topic: string, data: JobData = {}, context?: JobState) {
    return await this.engine?.pub(topic, data, context);
  }
  async sub(topic: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.sub(topic, callback);
  }
  async unsub(topic: string): Promise<void> {
    return await this.engine?.unsub(topic);
  }
  async psub(wild: string, callback: JobMessageCallback): Promise<void> {
    return await this.engine?.psub(wild, callback);
  }
  async punsub(wild: string): Promise<void> {
    return await this.engine?.punsub(wild);
  }
  async pubsub(topic: string, data: JobData = {}, context?: JobState | null, timeout?: number): Promise<JobOutput> {
    return await this.engine?.pubsub(topic, data, context, timeout);
  }
  async add(streamData: StreamData|StreamDataResponse): Promise<string> {
    return await this.engine.add(streamData) as string;
  }

  // ************* COMPILER METHODS *************
  async plan(path: string): Promise<HotMeshManifest> {
    return await this.engine?.plan(path);
  }
  async deploy(pathOrYAML: string): Promise<HotMeshManifest> {
    return await this.engine?.deploy(pathOrYAML);
  }
  async activate(version: string, delay?: number): Promise<boolean> {
    //activation is a quorum operation
    return await this.quorum?.activate(version, delay);
  }

  // ************* REPORTER METHODS *************
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return await this.engine?.getStats(topic, query);
  }
  async getStatus(jobId: string): Promise<JobStatus> {
    return this.engine?.getStatus(jobId);
  }
  async getState(topic: string, jobId: string) {
    return this.engine?.getState(topic, jobId);
  }
  async getIds(topic: string, query: JobStatsInput, queryFacets = []): Promise<IdsResponse> {
    return await this.engine?.getIds(topic, query, queryFacets);
  }
  async resolveQuery(topic: string, query: JobStatsInput): Promise<GetStatsOptions> {
    return await this.engine?.resolveQuery(topic, query);
  }

  // ****************** `SCRUB` CLEAN COMPLETED JOBS *****************
  async scrub(jobId: string) {
    await this.engine?.scrub(jobId);
  }

  // ****** `HOOK` ACTIVITY RE-ENTRY POINT ******
  async hook(topic: string, data: JobData, dad?: string): Promise<JobStatus | void> {
    //return collation int
    return await this.engine?.hook(topic, data, dad);
  }
  async hookAll(hookTopic: string, data: JobData, query: JobStatsInput, queryFacets: string[] = []): Promise<string[]> {
    return await this.engine?.hookAll(hookTopic, data, query, queryFacets);
  }

  async stop() {
    await StreamSignaler.stopConsuming();
    this.engine?.task.cancelCleanup();
  }

  async compress(terms: string[]): Promise<boolean> {
    return await this.engine?.compress(terms);
  }
}

export { HotMeshService };
