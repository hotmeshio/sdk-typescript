import packageJson from '../../package.json';
import { HMSH_TELEMETRY } from '../../modules/enums';
import {
  Context,
  Span,
  SpanContext,
  SpanKind,
  SpanStatusCode,
  context,
  trace,
} from '../../types/telemetry';

/**
 * Emits OpenTelemetry spans for durable workflow execution. All methods
 * are no-ops when no OTel SDK is registered — `@opentelemetry/api`
 * returns a no-op tracer by default, so there is zero overhead without
 * a configured exporter.
 *
 * ## Span Categories
 *
 * | Span | When | Mode |
 * |------|------|------|
 * | `WORKFLOW/START/{name}` | First execution (not replay) | `info` |
 * | `WORKFLOW/COMPLETE/{name}` | Workflow returns successfully | `info` |
 * | `WORKFLOW/ERROR/{name}` | Workflow throws a fatal error | `info` |
 * | `ACTIVITY/{name}` | Real wall-clock activity execution on the worker | `info` |
 * | `DISPATCH/{type}/{name}/{idx}` | Operation dispatched (first execution only) | `debug` |
 * | `RETURN/{type}/{name}/{idx}` | Operation result returned (with ac/au duration) | `debug` |
 *
 * ## Gating
 *
 * - `isEnabled()` — true when `HMSH_TELEMETRY` is set (any value)
 * - `isVerbose()` — true only when `HMSH_TELEMETRY === 'debug'`
 *
 * In `info` mode, engine-layer spans (stream hops, DAG activity spans)
 * are suppressed for durable workflows — only the spans above are
 * emitted. This keeps dashboards clean and focused on the user's
 * workflow story. Set `HMSH_TELEMETRY=debug` to also see engine
 * internals and per-operation DISPATCH/RETURN spans.
 *
 * ## Setup
 *
 * Register an OTel SDK with a trace exporter before starting workers:
 *
 * ```typescript
 * import { NodeSDK } from '@opentelemetry/sdk-node';
 * import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
 * import { resourceFromAttributes } from '@opentelemetry/resources';
 * import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
 *
 * const sdk = new NodeSDK({
 *   resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: 'my-service' }),
 *   traceExporter: new OTLPTraceExporter({
 *     url: 'https://api.honeycomb.io/v1/traces',
 *     headers: { 'x-honeycomb-team': process.env.HONEYCOMB_API_KEY },
 *   }),
 * });
 * sdk.start();
 * ```
 *
 * ```bash
 * # Concise workflow telemetry (recommended for production)
 * HMSH_TELEMETRY=info node worker.js
 *
 * # Full operational detail (debugging)
 * HMSH_TELEMETRY=debug node worker.js
 * ```
 */
export class DurableTelemetryService {
  static isEnabled(): boolean {
    return !!HMSH_TELEMETRY;
  }

  static isVerbose(): boolean {
    return HMSH_TELEMETRY === 'debug';
  }

  static getParentContext(traceId: string, spanId: string): Context {
    const restoredSpanContext: SpanContext = {
      traceId,
      spanId,
      isRemote: true,
      traceFlags: 1,
    };
    return trace.setSpanContext(context.active(), restoredSpanContext);
  }

  /**
   * Emit a point-in-time span (starts and ends immediately).
   */
  static emitPointSpan(
    traceId: string,
    parentSpanId: string,
    spanName: string,
    attributes: Record<string, string | number | boolean>,
    statusCode?: SpanStatusCode,
    statusMessage?: string,
  ): void {
    const tracer = trace.getTracer(packageJson.name, packageJson.version);
    const parentContext = DurableTelemetryService.getParentContext(
      traceId,
      parentSpanId,
    );
    context.with(parentContext, () => {
      const span = tracer.startSpan(spanName, {
        kind: SpanKind.CLIENT,
        attributes,
      });
      if (statusCode !== undefined) {
        span.setStatus({ code: statusCode, message: statusMessage });
      }
      span.end();
    });
  }

  /**
   * Emit a duration span with explicit start/end times.
   * Used for reconstructing operation durations from stored timestamps.
   */
  static emitDurationSpan(
    traceId: string,
    parentSpanId: string,
    spanName: string,
    startTimeMs: number,
    endTimeMs: number,
    attributes: Record<string, string | number | boolean>,
  ): void {
    const tracer = trace.getTracer(packageJson.name, packageJson.version);
    const parentContext = DurableTelemetryService.getParentContext(
      traceId,
      parentSpanId,
    );
    context.with(parentContext, () => {
      const span = tracer.startSpan(
        spanName,
        {
          kind: SpanKind.CLIENT,
          attributes,
          startTime: startTimeMs,
        },
        parentContext,
      );
      span.end(endTimeMs);
    });
  }

  /**
   * Start a span and return it for manual end (e.g., wrapping activity execution).
   */
  static startSpan(
    traceId: string,
    parentSpanId: string,
    spanName: string,
    attributes: Record<string, string | number | boolean>,
  ): Span {
    const tracer = trace.getTracer(packageJson.name, packageJson.version);
    const parentContext = DurableTelemetryService.getParentContext(
      traceId,
      parentSpanId,
    );
    return tracer.startSpan(
      spanName,
      {
        kind: SpanKind.CLIENT,
        attributes,
      },
      parentContext,
    );
  }

  /**
   * Parse ac/au timestamps from jmark results to epoch ms.
   * Handles both ISO strings and numeric epoch values.
   */
  static parseTimestamp(ts: string | number): number {
    if (typeof ts === 'number') return ts;
    const parsed = Date.parse(ts);
    return isNaN(parsed) ? Date.now() : parsed;
  }
}
