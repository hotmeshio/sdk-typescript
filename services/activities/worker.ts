import { GetStateError } from '../../modules/errors';
import { Activity } from './activity';
import { CollatorService } from '../collator';
import { EngineService } from '../engine';
import {
  ActivityData,
  ActivityMetadata,
  ActivityType,
  WorkerActivity } from '../../types/activity';
import { JobState } from '../../types/job';
import { MultiResponseFlags } from '../../types/redis';
import { StringScalarType } from '../../types/serializer';
import {
  StreamCode,
  StreamData,
  StreamStatus } from '../../types/stream';
import { TelemetryService } from '../telemetry';

class Worker extends Activity {
  config: WorkerActivity;

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
    this.logger.debug('worker-process', { jid: this.context.metadata.jid, aid: this.metadata.aid });
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
      //TODO: UPDATE ACTIVITY STATE (LEG 1 EXIT)
      return this.context.metadata.aid;
    } catch (error) {
      if (error instanceof GetStateError) {
        this.logger.error('worker-get-state-error', error);
      } else {
        console.error(error);
        this.logger.error('worker-process-error', error);
      }
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('worker-process-end', { jid: this.context.metadata.jid, aid: this.metadata.aid });
    }
  }

  async execActivity(): Promise<string> {
    const streamData: StreamData = {
      metadata: {
        jid: this.context.metadata.jid,
        dad: this.metadata.dad,
        aid: this.metadata.aid,
        topic: this.config.subtype,
        spn: this.context['$self'].output.metadata.l1s,
        trc: this.context.metadata.trc,
      },
      data: this.context.data
    };
    if (this.config.retry) {
      streamData.policies = {
        retry: this.config.retry
      };
    }
    return (await this.engine.streamSignaler?.publishMessage(this.config.subtype, streamData)) as string;
  }


  //********  SIGNAL RE-ENTRY POINT (DUPLEX LEG 2 of 2)  ********//
  async processEvent(status: StreamStatus = StreamStatus.SUCCESS, code: StreamCode = 200): Promise<void> {
    this.setLeg(2);
    const jid = this.context.metadata.jid;
    const aid = this.metadata.aid;
    this.status = status;
    this.code = code;
    this.logger.debug('worker-process-event', { topic: this.config.subtype, jid, aid, status, code });
    let telemetry: TelemetryService;
    try {
      await this.getState();
      const aState = await CollatorService.notarizeReentry(this);
      this.adjacentIndex = CollatorService.getDimensionalIndex(aState);

      telemetry = new TelemetryService(this.engine.appId, this.config, this.metadata, this.context);
      let isComplete = CollatorService.isActivityComplete(this.context.metadata.js);
      if (isComplete) {
        this.logger.warn('worker-process-event-duplicate', { jid, aid });
        this.logger.debug('worker-process-event-duplicate-resolution', { resolution: 'Increase HotMesh config `reclaimDelay` timeout.' });
        return;
      }
      telemetry.startActivitySpan(this.leg);
      if (status === StreamStatus.PENDING) {
        await this.processPending(telemetry);
      } else if (status === StreamStatus.SUCCESS) {
        await this.processSuccess(telemetry);
      } else {
        await this.processError(telemetry);
      }
    } catch (error) {
      this.logger.error('worker-process-event-error', error);
      telemetry.setActivityError(error.message);
      throw error;
    } finally {
      telemetry.endActivitySpan();
      this.logger.debug('worker-process-event-end', { jid, aid });
    }
  }

  async processPending(telemetry: TelemetryService): Promise<void> {
    this.bindActivityData('output');
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeContinuation(this, multi);

    await this.setStatus(0, multi);
    const multiResponse = await multi.exec() as MultiResponseFlags;
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    telemetry.setActivityAttributes({ 'app.job.jss': jobStatus });
  }

  async processSuccess(telemetry: TelemetryService): Promise<void> {
    this.bindActivityData('output');
    this.adjacencyList = await this.filterAdjacent();
    this.mapJobData();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    const multiResponse = await multi.exec() as MultiResponseFlags;
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    const attrs: StringScalarType = { 'app.job.jss': jobStatus };
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds.length) {
      attrs['app.activity.mids'] = messageIds.join(',')
    }
    telemetry.setActivityAttributes(attrs);
  }

  async processError(telemetry: TelemetryService): Promise<void> {
    this.bindActivityError(this.data);
    this.adjacencyList = await this.filterAdjacent();
    const multi = this.store.getMulti();
    await this.setState(multi);
    await CollatorService.notarizeCompletion(this, multi);

    await this.setStatus(this.adjacencyList.length - 1, multi);
    const multiResponse = await multi.exec() as MultiResponseFlags;
    telemetry.mapActivityAttributes();
    const jobStatus = this.resolveStatus(multiResponse);
    const attrs: StringScalarType = { 'app.job.jss': jobStatus };
    const messageIds = await this.transition(this.adjacencyList, jobStatus);
    if (messageIds.length) {
      attrs['app.activity.mids'] = messageIds.join(',')
    }
    telemetry.setActivityAttributes(attrs);
  }
}

export { Worker };
