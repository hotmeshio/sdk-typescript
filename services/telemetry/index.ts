import packageJson from '../../package.json';
import { MapperService } from '../mapper';
import { ActivityMetadata, ActivityType, Consumes } from '../../types/activity';
import { JobState } from '../../types/job';
import {
  StringAnyType,
  StringScalarType,
  StringStringType,
} from '../../types/serializer';
import { StreamData, StreamDataType, StreamRole } from '../../types/stream';
import {
  Span,
  SpanContext,
  SpanKind,
  trace,
  Context,
  context,
  SpanStatusCode,
} from '../../types/telemetry';
import { HMSH_TELEMETRY } from '../../modules/enums';

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
    this.config = config;
    this.metadata = metadata;
    this.context = context;
  }

  /**
   * too chatty for production; only output traces, jobs, triggers and workers
   */
  private shouldCreateSpan(): boolean {
    return (
      HMSH_TELEMETRY === 'debug' ||
      this.config?.type === 'trigger' ||
      this.config?.type === 'worker'
    );
  }

  private static createNoopSpan(traceId: string, spanId: string): Span {
    // A no-op span that returns the given spanContext and ignores all operations.
    return {
      spanContext(): SpanContext {
        return {
          traceId,
          spanId,
          isRemote: true,
          traceFlags: 1,
        };
      },
      addEvent(
        _name: string,
        _attributesOrStartTime?: unknown,
        _startTime?: unknown,
      ): Span {
        return this;
      },
      setAttribute(_key: string, _value: unknown): Span {
        return this;
      },
      setAttributes(_attributes: { [p: string]: unknown }): Span {
        return this;
      },
      setStatus(_status: { code: SpanStatusCode; message?: string }): Span {
        return this;
      },
      updateName(_name: string): Span {
        return this;
      },
      end(_endTime?: number): void {
        // no-op
      },
      isRecording(): boolean {
        return false;
      },
      recordException(_exception: unknown, _time?: number): void {
        // no-op
      },
    };
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

  /**
   * Traces an activity.
   * @private
   */
  static async traceActivity(
    appId: string,
    attributes: StringScalarType,
    activityId: string,
    traceId: string,
    spanId: string,
    index = 0,
  ): Promise<boolean> {
    const spanName = `TRACE/${appId}/${activityId}/${index}`;
    const tracer = trace.getTracer(packageJson.name, packageJson.version);

    const restoredSpanContext: SpanContext = {
      traceId,
      spanId,
      isRemote: true,
      traceFlags: 1,
    };

    const parentContext = trace.setSpanContext(
      context.active(),
      restoredSpanContext,
    );

    return context.with(parentContext, () => {
      const span = tracer.startSpan(spanName, {
        kind: SpanKind.CLIENT,
        attributes,
      });

      span.setAttributes(attributes);
      span.end();
      return true;
    });
  }

  startActivitySpan(leg = this.leg): TelemetryService {
    const spanName = `${this.config.type.toUpperCase()}/${this.appId}/${this.metadata.aid}/${leg}`;
    const traceId = this.getTraceId();
    const spanId = this.getActivityParentSpanId(leg);
    const attributes = this.getSpanAttrs(leg);
    const span: Span = this.startSpan(traceId, spanId, spanName, attributes);

    this.span = span;
    this.setTelemetryContext(span, leg);
    return this;
  }

  startStreamSpan(data: StreamData, role: StreamRole): TelemetryService {
    let type: string;
    if (role === StreamRole.SYSTEM) {
      type = 'SYSTEM';
    } else if (role === StreamRole.WORKER) {
      type = 'EXECUTE';
    } else if (
      data.type === StreamDataType.RESULT ||
      data.type === StreamDataType.RESPONSE
    ) {
      type = 'FANIN'; //re-entering engine router (from worker router)
    } else {
      type = 'FANOUT'; //exiting engine router (to worker router)
    }

    // `EXECUTE` refers to the 'worker router' NOT the 'worker activity' run by the 'engine router'
    // (Regardless, it's worker-related, so it matters and will be traced)
    // `SYSTEM` refers to catastrophic errors, which are always traced
    if (this.shouldCreateSpan() || type === 'EXECUTE' || type === 'SYSTEM') {
      const topic = data.metadata.topic ? `/${data.metadata.topic}` : '';
      const spanName = `${type}/${this.appId}/${data.metadata.aid}${topic}`;
      const attributes = this.getStreamSpanAttrs(data);
      this.span = this.startSpan(
        data.metadata.trc,
        data.metadata.spn,
        spanName,
        attributes,
        true,
      );
    } else {
      this.traceId = data.metadata.trc;
      this.spanId = data.metadata.spn;
      this.span = TelemetryService.createNoopSpan(
        data.metadata.trc,
        data.metadata.spn,
      );
    }

    return this;
  }

  startSpan(
    traceId: string,
    spanId: string,
    spanName: string,
    attributes: StringScalarType,
    bCreate = false,
  ): Span {
    this.traceId = traceId;
    this.spanId = spanId;

    if (bCreate || this.shouldCreateSpan()) {
      const tracer = trace.getTracer(packageJson.name, packageJson.version);
      const parentContext = this.getParentSpanContext();
      const span = tracer.startSpan(
        spanName,
        { kind: SpanKind.CLIENT, attributes, root: !parentContext },
        parentContext,
      );
      return span;
    }

    return TelemetryService.createNoopSpan(traceId, spanId);
  }

  mapActivityAttributes(): void {
    if (this.config.telemetry && this.span) {
      const telemetryAtts = new MapperService(
        this.config.telemetry,
        this.context,
      ).mapRules();
      const namespacedAtts = {
        ...Object.keys(telemetryAtts).reduce((result, key) => {
          if (
            ['string', 'boolean', 'number'].includes(typeof telemetryAtts[key])
          ) {
            result[`app.activity.data.${key}`] = telemetryAtts[key];
          }
          return result;
        }, {}),
      };
      this.span.setAttributes(namespacedAtts as StringScalarType);
    }
  }

  setActivityAttributes(attributes: StringScalarType): void {
    this.span?.setAttributes(attributes);
  }

  setStreamAttributes(attributes: StringScalarType): void {
    this.span?.setAttributes(attributes);
  }

  setJobAttributes(attributes: StringScalarType): void {
    this.jobSpan?.setAttributes(attributes);
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
    // For a no-op span, end() does nothing anyway
    span && span.end();
  }

  getParentSpanContext(): undefined | Context {
    if (this.traceId && this.spanId) {
      const restoredSpanContext: SpanContext = {
        traceId: this.traceId,
        spanId: this.spanId,
        isRemote: true,
        traceFlags: 1,
      };
      const parentContext = trace.setSpanContext(
        context.active(),
        restoredSpanContext,
      );
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
  }

  getStreamSpanAttrs(input: StreamData): StringAnyType {
    return {
      ...Object.keys(input.metadata).reduce((result, key) => {
        if (key !== 'trc' && key !== 'spn') {
          result[`app.stream.${key}`] = input.metadata[key];
        }
        return result;
      }, {}),
    };
  }

  setTelemetryContext(span: Span, leg: number) {
    // Even if span is no-op, we still set context so that callers remain unaware
    if (!this.context.metadata.trc) {
      this.context.metadata.trc = span.spanContext().traceId;
    }
    if (!this.context['$self'].output.metadata) {
      this.context['$self'].output.metadata = {};
    }
    // Echo the parent's or the newly created span's spanId
    // This ensures the caller sees a consistent chain
    if (leg === 1) {
      this.context['$self'].output.metadata.l1s = span.spanContext().spanId;
    } else {
      this.context['$self'].output.metadata.l2s = span.spanContext().spanId;
    }
  }

  setActivityError(message: string) {
    this.span?.setStatus({ code: SpanStatusCode.ERROR, message });
  }

  setStreamError(message: string) {
    this.span?.setStatus({ code: SpanStatusCode.ERROR, message });
  }

  static addTargetTelemetryPaths(
    consumes: Consumes,
    config: ActivityType,
    metadata: ActivityMetadata,
    leg: number,
  ): void {
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

  static bindJobTelemetryToState(
    state: StringStringType,
    config: ActivityType,
    context: JobState,
  ) {
    if (config.type === 'trigger') {
      state['metadata/trc'] = context.metadata.trc;
    }
  }

  static bindActivityTelemetryToState(
    state: StringAnyType,
    config: ActivityType,
    metadata: ActivityMetadata,
    context: JobState,
    leg: number,
  ): void {
    if (config.type === 'trigger') {
      state[`${metadata.aid}/output/metadata/l1s`] =
        context['$self'].output.metadata.l1s;
      state[`${metadata.aid}/output/metadata/l2s`] =
        context['$self'].output.metadata.l2s;
    } else if (config.type === 'hook' && leg === 1) {
      state[`${metadata.aid}/output/metadata/l1s`] =
        context['$self'].output.metadata.l1s;
      state[`${metadata.aid}/output/metadata/l2s`] =
        context['$self'].output.metadata.l1s;
    } else if (config.type === 'signal' && leg === 1) {
      state[`${metadata.aid}/output/metadata/l1s`] =
        context['$self'].output.metadata.l1s;
      state[`${metadata.aid}/output/metadata/l2s`] =
        context['$self'].output.metadata.l1s;
    } else {
      const target = `l${leg}s`;
      state[`${metadata.aid}/output/metadata/${target}`] =
        context['$self'].output.metadata[target];
    }
  }
}

export { TelemetryService };
