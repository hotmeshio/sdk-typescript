import packageJson from '../../package.json';
import { MapperService } from '../mapper';
import {
  ActivityMetadata,
  ActivityType,
  Consumes } from '../../types/activity';
import { JobState } from '../../types/job';
import {
  StringAnyType,
  StringScalarType,
  StringStringType } from '../../types/serializer';
import { StreamData, StreamDataType, StreamRole } from '../../types/stream';
import {
  Span,
  SpanContext,
  SpanKind,
  trace,
  Context,
  context, 
  SpanStatusCode } from '../../types/telemetry';

class TelemetryService {
  span: Span;
  jobSpan: Span;
  config: ActivityType;
  traceId: string | null;
  spanId: string | null;
  appId: string;
  metadata: ActivityMetadata;
  context: JobState;
  leg = 1;

  constructor(
    appId: string,
    config?: ActivityType,
    metadata?: ActivityMetadata,
    context?: JobState,
  ) {
    this.appId = appId;
    //these are REQUIRED for job and activity spans
    this.config = config;
    this.metadata = metadata;
    this.context = context;
  }

  getJobParentSpanId(): string | undefined {
    return this.context.metadata.spn;
  }

  getActivityParentSpanId(leg: number): string | undefined {
    if (leg === 1) {
      return this.context[this.config.parent].output?.metadata?.l2s;
    } else {
      return this.context['$self'].output?.metadata?.l1s;
    }
  }

  getTraceId(): string | undefined {
    return this.context.metadata.trc;
  }

  startJobSpan(): TelemetryService {
    const spanName = `JOB/${this.appId}/${this.config.subscribes}/1`;
    const traceId = this.getTraceId();
    const spanId = this.getJobParentSpanId();
    const attributes = this.getSpanAttrs(1);
    const span: Span = this.startSpan(traceId, spanId, spanName, attributes);
    this.jobSpan = span;
    this.setTelemetryContext(span, 1);
    return this;
  }

  startActivitySpan(leg = this.leg): TelemetryService {
    const spanName = `${this.config.type.toUpperCase()}/${this.appId}/${this.metadata.aid}/${leg}`;
    const traceId = this.getTraceId();
    const spanId = this.getActivityParentSpanId(leg);
    const attributes = this.getSpanAttrs(leg);
    const span: Span = this.startSpan(traceId, spanId, spanName, attributes);
    this.setTelemetryContext(span, leg);
    this.span = span;
    return this;
  }

  startStreamSpan(data: StreamData, role: StreamRole): TelemetryService {
    let type: string;
    if (role === StreamRole.SYSTEM) {
      type = 'SYSTEM';
    } else if (role === StreamRole.WORKER) {
      type = 'EXECUTE';
    } else if (data.type === StreamDataType.RESULT || data.type === StreamDataType.RESPONSE) {
      type = 'FANIN';
    } else {
      type = 'FANOUT';
    }
    const topic = data.metadata.topic ? `/${data.metadata.topic}` : '';
    const spanName = `${type}/${this.appId}/${data.metadata.aid}${topic}`;
    const attributes = this.getStreamSpanAttrs(data);
    const span: Span = this.startSpan(data.metadata.trc, data.metadata.spn, spanName, attributes);
    this.span = span;
    return this;
  }

  startSpan(traceId: string, spanId: string, spanName: string, attributes: StringScalarType): Span {
    this.traceId = traceId;
    this.spanId = spanId;
    const tracer = trace.getTracer(packageJson.name, packageJson.version);
    let parentContext = this.getParentSpanContext();
    const span = tracer.startSpan(
      spanName,
      { kind: SpanKind.CLIENT, attributes, root: !parentContext },
      parentContext
    );
    return span;
  }

  mapActivityAttributes(): void {
    //export user-defined span attributes (app.activity.data.*)
    if (this.config.telemetry) {
      const telemetryAtts = new MapperService(this.config.telemetry, this.context).mapRules();
      const namespacedAtts = {
        ...Object.keys(telemetryAtts).reduce((result, key) => {
          if (['string', 'boolean', 'number'].includes(typeof telemetryAtts[key])) {
            result[`app.activity.data.${key}`] = telemetryAtts[key];
          }
          return result;
        }, {})
      };
      this.span.setAttributes(namespacedAtts as StringScalarType);
    }
  }

  setActivityAttributes(attributes: StringScalarType): void {
    this.span.setAttributes(attributes);
  }

  setStreamAttributes(attributes: StringScalarType): void {
    this.span.setAttributes(attributes);
  }

  setJobAttributes(attributes: StringScalarType): void {
    this.jobSpan.setAttributes(attributes);
  }

  endJobSpan(): void {
    this.endSpan(this.jobSpan);
  }

  endActivitySpan(): void {
    this.endSpan(this.span);
  }

  endStreamSpan(): void {
    this.endSpan(this.span);
  }

  endSpan(span: Span): void {
    span && span.end();
  }

  getParentSpanContext(): undefined | Context {
    if (this.traceId && this.spanId) {
      const restoredSpanContext: SpanContext = {
        traceId: this.traceId,
        spanId: this.spanId,
        isRemote: true,
        traceFlags: 1, // (todo: revisit sampling strategy/config)
      };
      const parentContext = trace.setSpanContext(context.active(), restoredSpanContext);
      return parentContext;
    }
  }

  getSpanAttrs(leg: number): StringAnyType {
    return {
      ...Object.keys(this.context.metadata).reduce((result, key) => {
        if (key !== 'trc') {
          result[`app.job.${key}`] = this.context.metadata[key];
        }
        return result;
      }, {}),
      ...Object.keys(this.metadata).reduce((result, key) => {
        result[`app.activity.${key}`] = this.metadata[key];
        return result;
      }, {}),
      'app.activity.leg': leg,
    };
  };

  getStreamSpanAttrs(input: StreamData): StringAnyType {
    return {
      ...Object.keys(input.metadata).reduce((result, key) => {
        if (key !== 'trc' && key !== 'spn') {
          result[`app.stream.${key}`] = input.metadata[key];
        }
        return result;
      }, {})
    };
  };

  setTelemetryContext(span: Span, leg: number) {
    if (!this.context.metadata.trc) {
      this.context.metadata.trc = span.spanContext().traceId;
    }
    if (leg === 1) {
      if (!this.context['$self'].output.metadata) {
        this.context['$self'].output.metadata = {};
      }
      this.context['$self'].output.metadata.l1s = span.spanContext().spanId;
    } else {
      if (!this.context['$self'].output.metadata) {
        this.context['$self'].output.metadata = {};
      }
      this.context['$self'].output.metadata.l2s = span.spanContext().spanId;
    }
  }

  setActivityError(message: string) {
    this.span.setStatus({ code: SpanStatusCode.ERROR, message });
  }

  setStreamError(message: string) {
    this.span.setStatus({ code: SpanStatusCode.ERROR, message });
  }

  /**
   * Adds the paths (HGET) necessary to restore telemetry state for an activity
   * @param consumes 
   * @param config 
   * @param metadata 
   * @param leg 
   */
  static addTargetTelemetryPaths(consumes: Consumes, config: ActivityType, metadata: ActivityMetadata, leg: number): void {
    if (leg === 1) {
      if (!(config.parent in consumes)) {
        consumes[config.parent] = [];
      }
      consumes[config.parent].push(`${config.parent}/output/metadata/l2s`);
    } else {
      if (!(metadata.aid in consumes)) {
        consumes[metadata.aid] = [];
      }
      consumes[metadata.aid].push(`${metadata.aid}/output/metadata/l1s`);
    }
  }

  static bindJobTelemetryToState(state: StringStringType, config: ActivityType, context:JobState) {
    if (config.type === 'trigger') {
      state['metadata/trc'] = context.metadata.trc;
    }
  }

  static bindActivityTelemetryToState(state: StringAnyType, config: ActivityType, metadata: ActivityMetadata, context: JobState, leg: number): void {
    if (config.type === 'trigger') {
      state[`${metadata.aid}/output/metadata/l1s`] = context['$self'].output.metadata.l1s;
      state[`${metadata.aid}/output/metadata/l2s`] = context['$self'].output.metadata.l2s;
    } else if (config.type === 'activity' && leg === 1) {
      //activities run non-duplexed and only have a single leg
      state[`${metadata.aid}/output/metadata/l1s`] = context['$self'].output.metadata.l1s;
      state[`${metadata.aid}/output/metadata/l2s`] = context['$self'].output.metadata.l1s;
    } else {
      const target = `l${leg}s`;
      state[`${metadata.aid}/output/metadata/${target}`] = context['$self'].output.metadata[target];
    }
  }
}

export { TelemetryService };
