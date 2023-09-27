import { GetStateError } from '../../modules/errors';
import { Activity } from './activity';
import { EngineService } from '../engine';
import {
  ActivityData,
  ActivityMetadata,
  AwaitActivity,
  ActivityType } from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus } from '../../types/stream';
import { TelemetryService } from '../telemetry';
import { CollatorService } from '../collator';

class Await extends Activity {
  config: AwaitActivity;

  constructor(
    config: ActivityType,
    data: ActivityData,
    metadata: ActivityMetadata,
    hook: ActivityData | null,
    engine: EngineService,
    context?: JobState) {
      super(config, data, metadata, hook, engine, context);
  }

  //********  INITIAL ENTRY POINT (A)  ********//
  async process(): Promise<string> {
    this.logger.debug('await-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    let telemetry: TelemetryService;
    try {
      this.setLeg(1);
      await CollatorService.notarizeEntry(this);

      await this.getState();
      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);
      this.mapInputData();

      const multi = this.store.getMulti();
      //await this.registerTimeout();
      await CollatorService.authorizeReentry(this, multi);
      await this.setState(multi);
      await this.setStatus(0, multi);
      const multiResponse = await multi.exec() as MultiResponseFlags;

      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);
      const messageId = await this.execActivity();
      telemetry.setActivityAttributes({
        'app.activity.mid': messageId,
        'app.job.jss': jobStatus
      });
      return this.context.metadata.aid;
    } catch (error) {
      telemetry.setActivityError(error.message);
      if (error instanceof GetStateError) {
        this.logger.error('await-get-state-error', error);
      } else {
        this.logger.error('await-process-error', error);
      }
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('await-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }


  async execActivity(): Promise<string> {
    const streamData: StreamData = {
      metadata: {
        jid: this.context.metadata.jid,
        dad: this.metadata.dad,
        aid: this.metadata.aid,
        topic: this.config.subtype,
        spn: this.context['$self'].output.metadata?.l1s,
        trc: this.context.metadata.trc,
      },
      type: StreamDataType.AWAIT,
      data: this.context.data
    };
    if (this.config.retry) {
      streamData.policies = {
        retry: this.config.retry
      };
    }
    return (await this.engine.streamSignaler?.publishMessage(null, streamData)) as string;
  }


  //********  `RESOLVE` ENTRY POINT (B)  ********//
  //this method is invoked when the job spawned by this job ends;
  //`this.data` is the job data produced by the spawned job
  async processEvent(status: StreamStatus = StreamStatus.SUCCESS, code: StreamCode = 200): Promise<void> {
    this.setLeg(2);
    const jid = this.context.metadata.jid;
    const aid = this.metadata.aid;
    if (!jid) {
      throw new Error('await-process-event-error');
    }
    this.logger.debug('await-resolve-await', { jid, aid, status, code });
    this.status = status;
    this.code = code;
    let telemetry: TelemetryService;
    try {
      await this.getState();
      const aState = await CollatorService.notarizeReentry(this);
      this.adjacentIndex = CollatorService.getDimensionalIndex(aState);

      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      telemetry.startActivitySpan(this.leg);

      let multiResponse: MultiResponseFlags = [];
      if (status === StreamStatus.SUCCESS) {
        this.bindActivityData('output');
        this.adjacencyList = await this.filterAdjacent();
        multiResponse = await this.processSuccess(this.adjacencyList);
      } else {
        this.bindActivityError(this.data);
        this.adjacencyList = await this.filterAdjacent();
        multiResponse = await this.processError(this.adjacencyList);
      }

      telemetry.mapActivityAttributes();
      const jobStatus = this.resolveStatus(multiResponse);
      const attrs: StringScalarType = { 'app.job.jss': jobStatus };
      const messageIds = await this.transition(this.adjacencyList, jobStatus);
      if (messageIds.length) {
        attrs['app.activity.mids'] = messageIds.join(',')
      }
      telemetry.setActivityAttributes(attrs);
    } catch (error) {
      this.logger.error('await-resolve-await-error', error);
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('await-resolve-await-end', { jid, aid, status, code });
    }
  }

  async processSuccess(adjacencyList: StreamData[]): Promise<MultiResponseFlags> {
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(adjacencyList.length - 1, multi);
    return await multi.exec() as MultiResponseFlags;
  }

  async processError(adjacencyList: StreamData[]): Promise<MultiResponseFlags> {
    //todo: if adjacencyList.length == 0, then map to the job output
    //      this method would be added to Base activity class 
    //this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(adjacencyList.length - 1, multi);
    return await multi.exec() as MultiResponseFlags;
  }
}

export { Await };
