import { HMNS } from '../../modules/key';
import { guid } from '../../modules/utils';
import { RedisConnection } from '../connector/clients/redis';
import { RedisConnection as IORedisConnection } from '../connector/clients/ioredis';
import { EngineService } from '../engine';
import { LoggerService, ILogger } from '../logger';
import { QuorumService } from '../quorum';
import { Router } from '../router';
import { WorkerService } from '../worker';
import {
  JobState,
  JobData,
  JobOutput, 
  JobStatus, 
  JobInterruptOptions} from '../../types/job';
import {
  HotMeshConfig,
  HotMeshManifest } from '../../types/hotmesh';
import { JobMessageCallback, QuorumProfile, ThrottleMessage, ThrottleOptions } from '../../types/quorum';
import {
  JobStatsInput,
  GetStatsOptions,
  IdsResponse,
  StatsResponse } from '../../types/stats';
import { ConnectorService } from '../connector';
import { StreamCode, StreamData, StreamDataResponse, StreamStatus } from '../../types/stream';
import { StringAnyType, StringStringType } from '../../types/serializer';
import { JobExport } from '../../types/exporter';

class HotMeshService {
  namespace: string;
  appId: string;
  guid: string;
  engine: EngineService | null = null;
  quorum: QuorumService | null = null;
  workers: WorkerService[] = [];
  logger: ILogger;

  static disconnecting = false;

  verifyAndSetNamespace(namespace?: string) {
    if (!namespace) {
      this.namespace = HMNS;
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
    instance.guid = guid();
    instance.verifyAndSetNamespace(config.namespace);
    instance.verifyAndSetAppId(config.appId);
    instance.logger = new LoggerService(config.appId, instance.guid, config.name || '', config.logLevel);
    await instance.initEngine(config, instance.logger);
    await instance.initQuorum(config, instance.engine, instance.logger);
    await instance.doWork(config, instance.logger);
    return instance;
  }

  static guid(): string {
    return guid();
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

  async doWork(config: HotMeshConfig, logger: ILogger) {
    this.workers = await WorkerService.init(
      this.namespace,
      this.appId,
      this.guid,
      config,
      logger
    );
  }

  // ************* PUB/SUB METHODS *************
  async pub(topic: string, data: JobData = {}, context?: JobState): Promise<string> {
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


  // ************* QUORUM METHODS *************
  async rollCall(delay?: number): Promise<QuorumProfile[]> {
    return await this.quorum?.rollCall(delay);
  }
  async throttle(options: ThrottleOptions): Promise<boolean> {
    const throttleMessage: ThrottleMessage = {
      type: 'throttle',
      throttle: options.throttle,
    };
    if (options.guid) {
      throttleMessage.guid = options.guid;
    } else if (options.topic) {
      throttleMessage.topic = options.topic;
    }
    return await this.quorum?.pub(throttleMessage);
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
  async export(jobId: string): Promise<JobExport> {
    return await this.engine?.export(jobId);
  }
  async getRaw(jobId: string): Promise<StringStringType> {
    return await this.engine?.getRaw(jobId);
  }
  async getStats(topic: string, query: JobStatsInput): Promise<StatsResponse> {
    return await this.engine?.getStats(topic, query);
  }
  async getStatus(jobId: string): Promise<JobStatus> {
    return this.engine?.getStatus(jobId);
  }
  async getState(topic: string, jobId: string): Promise<JobOutput> {
    return this.engine?.getState(topic, jobId);
  }
  async getQueryState(jobId: string, fields: string[]): Promise<StringAnyType> {
    return await this.engine?.getQueryState(jobId, fields);
  }
  async getIds(topic: string, query: JobStatsInput, queryFacets = []): Promise<IdsResponse> {
    return await this.engine?.getIds(topic, query, queryFacets);
  }
  async resolveQuery(topic: string, query: JobStatsInput): Promise<GetStatsOptions> {
    return await this.engine?.resolveQuery(topic, query);
  }

  // ****************** `INTERRUPT` ACTIVE JOBS *****************
  async interrupt(topic: string, jobId: string, options: JobInterruptOptions = {}): Promise<string> {
    return await this.engine?.interrupt(topic, jobId, options);
  }

  // ****************** `SCRUB` CLEAN COMPLETED JOBS *****************
  async scrub(jobId: string) {
    await this.engine?.scrub(jobId);
  }

  // ****** `HOOK` ACTIVITY RE-ENTRY POINT ******
  async hook(topic: string, data: JobData, status?: StreamStatus, code?: StreamCode): Promise<string> {
    return await this.engine?.hook(topic, data, status, code);
  }
  async hookAll(hookTopic: string, data: JobData, query: JobStatsInput, queryFacets: string[] = []): Promise<string[]> {
    return await this.engine?.hookAll(hookTopic, data, query, queryFacets);
  }

  static async stop() {
    if (!this.disconnecting) {
      this.disconnecting = true;
      await Router.stopConsuming();
      await RedisConnection.disconnectAll();
      await IORedisConnection.disconnectAll();
    }
  }

  stop() {
    this.engine?.taskService.cancelCleanup();
    this.quorum?.stop();
    this.workers?.forEach((worker: WorkerService) => {
      worker.stop();
    });
  }

  async compress(terms: string[]): Promise<boolean> {
    return await this.engine?.compress(terms);
  }
}

export { HotMeshService };
