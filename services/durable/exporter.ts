import { restoreHierarchy } from '../../modules/utils';
import { ILogger } from '../logger';
import { SerializerService } from '../serializer';
import { StoreService } from '../store';
import {
  ExportOptions,
  DurableJobExport,
  TimelineType,
  TransitionType,
  ExportFields,
  ExecutionExportOptions,
  WorkflowExecution,
  WorkflowExecutionEvent,
  WorkflowExecutionStatus,
  WorkflowExecutionSummary,
  WorkflowEventType,
  WorkflowEventCategory,
  WorkflowEventAttributes,
} from '../../types/exporter';
import { ProviderClient, ProviderTransaction } from '../../types/provider';
import {
  StringAnyType,
  StringStringType,
  Symbols,
} from '../../types/serializer';

// ── Timestamp helpers ────────────────────────────────────────────────────────

/**
 * Parse a HotMesh compact timestamp (YYYYMMDDHHmmss.mmm) into ISO 8601.
 * Also accepts ISO 8601 strings directly.
 */
function parseTimestamp(raw: string | undefined | null): string | null {
  if (!raw || typeof raw !== 'string') return null;
  const m = raw.match(
    /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\.?(\d+))?$/,
  );
  if (m) {
    const [, yr, mo, dy, hr, mi, sc, ms] = m;
    const frac = ms ? `.${ms.padEnd(3, '0').slice(0, 3)}` : '.000';
    return `${yr}-${mo}-${dy}T${hr}:${mi}:${sc}${frac}Z`;
  }
  // Try ISO 8601
  if (raw.includes('T') || raw.includes('-')) {
    const d = new Date(raw);
    if (!isNaN(d.getTime())) return d.toISOString();
  }
  return null;
}

/**
 * Compute duration in milliseconds between two HotMesh timestamps.
 */
function computeDuration(
  ac: string | undefined,
  au: string | undefined,
): number | null {
  const start = parseTimestamp(ac);
  const end = parseTimestamp(au);
  if (!start || !end) return null;
  return new Date(end).getTime() - new Date(start).getTime();
}

// ── Timeline key parsing ─────────────────────────────────────────────────────

/**
 * Extract the operation type (proxy, child, start, wait, sleep, hook)
 * from a timeline key like `-proxy,0,0-1-`.
 */
function extractOperation(key: string): string {
  const parts = key.split('-').filter(Boolean);
  return parts[0]?.split(',')[0] || 'unknown';
}

// ── Name extraction ──────────────────────────────────────────────────────────

/**
 * Extract the activity name from a timeline entry value's job_id.
 *
 * Job ID format: `-{workflowId}-$${activityName}{dimension}-{execIndex}`
 * Examples:
 *   `-wfId-$analyzeContent-5`  → `'analyzeContent'`
 *   `-wfId-$processOrder,0,0-3` → `'processOrder'`
 */
function extractActivityName(
  value: Record<string, any> | null,
): string {
  const jobId = value?.job_id;
  if (!jobId || typeof jobId !== 'string') return 'unknown';
  const dollarIdx = jobId.lastIndexOf('$');
  if (dollarIdx === -1) return jobId;
  const afterDollar = jobId.substring(dollarIdx + 1);
  const dashIdx = afterDollar.lastIndexOf('-');
  const nameWithDim = dashIdx > 0 ? afterDollar.substring(0, dashIdx) : afterDollar;
  // Strip dimension suffix (,N,N...)
  const commaIdx = nameWithDim.indexOf(',');
  return (commaIdx > 0 ? nameWithDim.substring(0, commaIdx) : nameWithDim) || 'unknown';
}

/**
 * Check if an activity name is a system (interceptor) operation.
 */
function isSystemActivity(name: string): boolean {
  return name.startsWith('lt');
}

/**
 * Extract a child workflow ID from a child/start timeline value.
 */
function extractChildWorkflowId(
  value: Record<string, any> | null,
): string {
  return value?.job_id || 'unknown';
}

// ── Status mapping ───────────────────────────────────────────────────────────

/**
 * Map HotMesh job state to a human-readable execution status.
 *
 * HotMesh semaphore: `0` = idle, `> 0` = pending activities,
 * `< 0` = failed / interrupted.
 *
 * A workflow can be "done" (`state.data.done === true`) while the
 * semaphore is still > 0 (cleanup activities pending). We check
 * both the `done` flag and the semaphore to determine status.
 */
function mapStatus(
  rawStatus: number | undefined,
  isDone?: boolean,
  hasError?: boolean,
): WorkflowExecutionStatus {
  if (hasError || (rawStatus !== undefined && !isNaN(rawStatus) && rawStatus < 0)) {
    return 'failed';
  }
  if (isDone || rawStatus === 0) return 'completed';
  if (rawStatus === undefined || isNaN(rawStatus)) return 'running';
  return 'running';
}

// ── Event construction ───────────────────────────────────────────────────────

function makeEvent(
  event_id: number,
  event_type: WorkflowEventType,
  category: WorkflowEventCategory,
  event_time: string,
  duration_ms: number | null,
  is_system: boolean,
  attributes: WorkflowEventAttributes,
): WorkflowExecutionEvent {
  return { event_id, event_type, category, event_time, duration_ms, is_system, attributes };
}

function computeSummary(events: WorkflowExecutionEvent[]): WorkflowExecutionSummary {
  const summary: WorkflowExecutionSummary = {
    total_events: events.length,
    activities: { total: 0, completed: 0, failed: 0, system: 0, user: 0 },
    child_workflows: { total: 0, completed: 0, failed: 0 },
    timers: 0,
    signals: 0,
  };

  for (const e of events) {
    switch (e.event_type) {
      case 'activity_task_scheduled':
        summary.activities.total++;
        if (e.is_system) summary.activities.system++;
        else summary.activities.user++;
        break;
      case 'activity_task_completed':
        summary.activities.completed++;
        break;
      case 'activity_task_failed':
        summary.activities.failed++;
        break;
      case 'child_workflow_execution_started':
        summary.child_workflows.total++;
        break;
      case 'child_workflow_execution_completed':
        summary.child_workflows.completed++;
        break;
      case 'child_workflow_execution_failed':
        summary.child_workflows.failed++;
        break;
      case 'timer_started':
        summary.timers++;
        break;
      case 'workflow_execution_signaled':
        summary.signals++;
        break;
    }
  }

  return summary;
}

// ── Exporter Service ─────────────────────────────────────────────────────────

class ExporterService {
  appId: string;
  logger: ILogger;
  store: StoreService<ProviderClient, ProviderTransaction>;
  symbols: Promise<Symbols> | Symbols;
  private static symbols: Map<string, Symbols> = new Map();

  constructor(
    appId: string,
    store: StoreService<ProviderClient, ProviderTransaction>,
    logger: ILogger,
  ) {
    this.appId = appId;
    this.logger = logger;
    this.store = store;
  }

  /**
   * Convert the job hash from its compiled format into a DurableJobExport object with
   * facets that describe the workflow in terms relevant to narrative storytelling.
   */
  async export(
    jobId: string,
    options: ExportOptions = {},
  ): Promise<DurableJobExport> {
    if (!ExporterService.symbols.has(this.appId)) {
      const symbols: Symbols | Promise<Symbols> = this.store.getAllSymbols();
      ExporterService.symbols.set(this.appId, await symbols);
    }
    const jobData = await this.store.getRaw(jobId);
    const jobExport = this.inflate(jobData, options);
    return jobExport;
  }

  /**
   * Export a workflow execution as a Temporal-compatible event history.
   *
   * **Sparse mode** (default): transforms the main workflow's timeline
   * into a flat event list. No additional I/O beyond the initial export.
   *
   * **Verbose mode**: recursively fetches child workflow jobs and attaches
   * their executions as nested `children`.
   */
  async exportExecution(
    jobId: string,
    workflowTopic: string,
    options: ExecutionExportOptions = {},
  ): Promise<WorkflowExecution> {
    const raw = await this.export(jobId);
    const execution = this.transformToExecution(raw, jobId, workflowTopic, options);

    if (options.mode === 'verbose') {
      const maxDepth = options.max_depth ?? 5;
      execution.children = await this.fetchChildren(raw, workflowTopic, options, 1, maxDepth);
    }

    return execution;
  }

  /**
   * Pure transformation: convert a raw DurableJobExport into a
   * Temporal-compatible WorkflowExecution event history.
   */
  transformToExecution(
    raw: DurableJobExport,
    workflowId: string,
    workflowTopic: string,
    options: ExecutionExportOptions,
  ): WorkflowExecution {
    const events: WorkflowExecutionEvent[] = [];
    let nextId = 1;

    // ── Extract timing from state metadata ─────────────────────────
    const state = raw.state as Record<string, any> | undefined;
    const metadata = state?.output?.metadata ?? state?.metadata;
    const stateData = state?.output?.data ?? state?.data;
    const jobCreated = metadata?.jc ?? stateData?.jc ?? metadata?.ac;
    const jobUpdated = metadata?.ju ?? stateData?.ju ?? metadata?.au;
    const startTime = parseTimestamp(jobCreated);
    const closeTime = parseTimestamp(jobUpdated);

    // ── Synthetic workflow_execution_started ─────────────────────────
    if (startTime) {
      events.push(makeEvent(nextId++, 'workflow_execution_started', 'workflow', startTime, null, false, {
        kind: 'workflow_execution_started',
        workflow_type: workflowTopic,
        task_queue: workflowTopic,
        input: options.omit_results ? undefined : raw.data,
      }));
    }

    // ── Transform timeline entries ───────────────────────────────
    const timeline = raw.timeline || [];
    for (const entry of timeline) {
      const operation = extractOperation(entry.key);
      const val = (typeof entry.value === 'object' && entry.value !== null)
        ? entry.value as Record<string, any>
        : null;

      const ac = val?.ac as string | undefined;
      const au = val?.au as string | undefined;
      const acIso = parseTimestamp(ac);
      const auIso = parseTimestamp(au);
      const dur = computeDuration(ac, au);
      const hasError = val != null && '$error' in val;

      switch (operation) {
        case 'proxy': {
          const name = extractActivityName(val);
          const isSys = isSystemActivity(name);
          if (options.exclude_system && isSys) break;

          if (acIso) {
            events.push(makeEvent(nextId++, 'activity_task_scheduled', 'activity', acIso, null, isSys, {
              kind: 'activity_task_scheduled',
              activity_type: name,
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }

          if (auIso) {
            if (hasError) {
              events.push(makeEvent(nextId++, 'activity_task_failed', 'activity', auIso, dur, isSys, {
                kind: 'activity_task_failed',
                activity_type: name,
                failure: val?.$error,
                timeline_key: entry.key,
                execution_index: entry.index,
              }));
            } else {
              events.push(makeEvent(nextId++, 'activity_task_completed', 'activity', auIso, dur, isSys, {
                kind: 'activity_task_completed',
                activity_type: name,
                result: options.omit_results ? undefined : val?.data,
                timeline_key: entry.key,
                execution_index: entry.index,
              }));
            }
          }
          break;
        }

        case 'child': {
          const childId = extractChildWorkflowId(val);
          if (acIso) {
            events.push(makeEvent(nextId++, 'child_workflow_execution_started', 'child_workflow', acIso, null, false, {
              kind: 'child_workflow_execution_started',
              child_workflow_id: childId,
              awaited: true,
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }
          if (auIso) {
            if (hasError) {
              events.push(makeEvent(nextId++, 'child_workflow_execution_failed', 'child_workflow', auIso, dur, false, {
                kind: 'child_workflow_execution_failed',
                child_workflow_id: childId,
                failure: val?.$error,
                timeline_key: entry.key,
                execution_index: entry.index,
              }));
            } else {
              events.push(makeEvent(nextId++, 'child_workflow_execution_completed', 'child_workflow', auIso, dur, false, {
                kind: 'child_workflow_execution_completed',
                child_workflow_id: childId,
                result: options.omit_results ? undefined : val?.data,
                timeline_key: entry.key,
                execution_index: entry.index,
              }));
            }
          }
          break;
        }

        case 'start': {
          const childId = extractChildWorkflowId(val);
          const ts = acIso || auIso;
          if (ts) {
            events.push(makeEvent(nextId++, 'child_workflow_execution_started', 'child_workflow', ts, null, false, {
              kind: 'child_workflow_execution_started',
              child_workflow_id: childId,
              awaited: false,
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }
          break;
        }

        case 'wait': {
          const signalName = val?.id
            || val?.data?.id
            || val?.data?.data?.id
            || `signal-${entry.index}`;
          const ts = auIso || acIso;
          if (ts) {
            events.push(makeEvent(nextId++, 'workflow_execution_signaled', 'signal', ts, dur, false, {
              kind: 'workflow_execution_signaled',
              signal_name: signalName,
              input: options.omit_results ? undefined : val?.data?.data,
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }
          break;
        }

        case 'sleep': {
          if (acIso) {
            events.push(makeEvent(nextId++, 'timer_started', 'timer', acIso, null, false, {
              kind: 'timer_started',
              duration_ms: dur ?? undefined,
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }
          if (auIso) {
            events.push(makeEvent(nextId++, 'timer_fired', 'timer', auIso, dur, false, {
              kind: 'timer_fired',
              timeline_key: entry.key,
              execution_index: entry.index,
            }));
          }
          break;
        }
        // Unknown operation types are silently skipped (forward-compatible)
      }
    }

    // ── Determine status ─────────────────────────────────────────
    const isDone = stateData?.done === true;
    const hasError = !!stateData?.$error;
    const status = mapStatus(raw.status, isDone, hasError);

    // ── Extract workflow result ──────────────────────────────────
    const result = stateData?.response ?? (
      raw.data && Object.keys(raw.data).length > 0 ? raw.data : null
    );

    // ── Synthetic workflow_execution_completed / failed ───────────
    if (status === 'completed' && closeTime) {
      const totalDur = startTime
        ? new Date(closeTime).getTime() - new Date(startTime).getTime()
        : null;
      events.push(makeEvent(nextId++, 'workflow_execution_completed', 'workflow', closeTime, totalDur, false, {
        kind: 'workflow_execution_completed',
        result: options.omit_results ? undefined : result,
      }));
    } else if (status === 'failed' && closeTime) {
      const totalDur = startTime
        ? new Date(closeTime).getTime() - new Date(startTime).getTime()
        : null;
      events.push(makeEvent(nextId++, 'workflow_execution_failed', 'workflow', closeTime, totalDur, false, {
        kind: 'workflow_execution_failed',
        failure: stateData?.err as string | undefined,
      }));
    }

    // ── Sort chronologically ─────────────────────────────────────
    events.sort((a, b) => {
      const cmp = a.event_time.localeCompare(b.event_time);
      return cmp !== 0 ? cmp : a.event_id - b.event_id;
    });

    // ── Re-number event IDs after sort ───────────────────────────
    for (let i = 0; i < events.length; i++) {
      events[i].event_id = i + 1;
    }

    // ── Back-references (Temporal-compatible) ────────────────────
    const scheduledMap = new Map<string, number>();
    const initiatedMap = new Map<string, number>();
    for (const e of events) {
      const attrs = e.attributes as any;
      if (e.event_type === 'activity_task_scheduled' && attrs.timeline_key) {
        scheduledMap.set(attrs.timeline_key, e.event_id);
      }
      if (e.event_type === 'child_workflow_execution_started' && attrs.timeline_key) {
        initiatedMap.set(attrs.timeline_key, e.event_id);
      }
      if ((e.event_type === 'activity_task_completed' || e.event_type === 'activity_task_failed') && attrs.timeline_key) {
        attrs.scheduled_event_id = scheduledMap.get(attrs.timeline_key) ?? null;
      }
      if ((e.event_type === 'child_workflow_execution_completed' || e.event_type === 'child_workflow_execution_failed') && attrs.timeline_key) {
        attrs.initiated_event_id = initiatedMap.get(attrs.timeline_key) ?? null;
      }
    }

    // ── Compute total duration ───────────────────────────────────
    const totalDuration = (startTime && closeTime)
      ? new Date(closeTime).getTime() - new Date(startTime).getTime()
      : null;

    return {
      workflow_id: workflowId,
      workflow_type: workflowTopic,
      task_queue: workflowTopic,
      status,
      start_time: startTime,
      close_time: (status !== 'running') ? closeTime : null,
      duration_ms: totalDuration,
      result,
      events,
      summary: computeSummary(events),
    };
  }

  /**
   * Recursively fetch child workflow executions for verbose mode.
   */
  private async fetchChildren(
    raw: DurableJobExport,
    workflowTopic: string,
    options: ExecutionExportOptions,
    depth: number,
    maxDepth: number,
  ): Promise<WorkflowExecution[]> {
    if (depth >= maxDepth) return [];

    const children: WorkflowExecution[] = [];
    const timeline = raw.timeline || [];

    for (const entry of timeline) {
      const operation = extractOperation(entry.key);
      if (operation !== 'child' && operation !== 'start') continue;

      const val = (typeof entry.value === 'object' && entry.value !== null)
        ? entry.value as Record<string, any>
        : null;
      const childJobId = val?.job_id;
      if (!childJobId || typeof childJobId !== 'string') continue;

      try {
        const childRaw = await this.export(childJobId);
        const childTopic = childRaw.data?.workflowTopic ?? workflowTopic;
        const childExecution = this.transformToExecution(
          childRaw, childJobId, childTopic, options,
        );

        if (options.mode === 'verbose') {
          childExecution.children = await this.fetchChildren(
            childRaw, childTopic, options, depth + 1, maxDepth,
          );
        }

        children.push(childExecution);
      } catch {
        // Child job may have expired or been cleaned up
      }
    }

    return children;
  }

  /**
   * Inflates the job data into a DurableJobExport object
   * @param jobHash - the job data
   * @param dependencyList - the list of dependencies for the job
   * @returns - the inflated job data
   */
  inflate(jobHash: StringStringType, options: ExportOptions): DurableJobExport {
    const timeline: TimelineType[] = [];
    const state: StringAnyType = {};
    const data: StringStringType = {};
    const transitionsObject: Record<string, TransitionType> = {};
    const regex = /^([a-zA-Z]{3}),(\d+(?:,\d+)*)/;

    Object.entries(jobHash).forEach(([key, value]) => {
      const match = key.match(regex);

      if (match) {
        //transitions
        this.inflateTransition(match, value, transitionsObject);
      } else if (key.startsWith('_')) {
        //data
        data[key.substring(1)] = value;
      } else if (key.startsWith('-')) {
        //timeline
        const keyParts = this.keyToObject(key);
        timeline.push({
          ...keyParts,
          key,
          value: this.resolveValue(value, options.values),
        });
      } else if (key.length === 3) {
        //state
        state[this.inflateKey(key)] = SerializerService.fromString(value);
      }
    });

    return this.filterFields(
      {
        data: restoreHierarchy(data),
        state: Object.entries(restoreHierarchy(state))[0][1],
        status: parseInt(jobHash[':'], 10),
        timeline: this.sortParts(timeline),
        transitions: this.sortEntriesByCreated(transitionsObject),
      },
      options.block,
      options.allow,
    );
  }

  resolveValue(
    raw: string,
    withValues: boolean,
  ): Record<string, any> | string | number | null {
    const resolved = SerializerService.fromString(raw);
    if (withValues !== false) {
      return resolved;
    }
    if (resolved && typeof resolved === 'object') {
      if ('data' in resolved) {
        resolved.data = {};
      }
      if ('$error' in resolved) {
        resolved.$error = {};
      }
    }
    return resolved;
  }

  /**
   * Inflates the key
   * into a human-readable JSON path, reflecting the
   * tree-like structure of the unidimensional Hash
   * @private
   */
  inflateKey(key: string): string {
    const symbols = ExporterService.symbols.get(this.appId);
    if (key in symbols) {
      const path = symbols[key];
      const parts = path.split('/');
      return parts.join('/');
    }
    return key;
  }

  filterFields(
    fullObject: DurableJobExport,
    block: ExportFields[] = [],
    allow: ExportFields[] = [],
  ): Partial<DurableJobExport> {
    let result: Partial<DurableJobExport> = {};
    if (allow && allow.length > 0) {
      allow.forEach((field) => {
        if (field in fullObject) {
          result[field] = fullObject[field] as StringAnyType &
            number &
            TimelineType[] &
            TransitionType[];
        }
      });
    } else {
      result = { ...fullObject };
    }
    if (block && block.length > 0) {
      block.forEach((field) => {
        if (field in result) {
          delete result[field];
        }
      });
    }
    return result as DurableJobExport;
  }

  inflateTransition(
    match: RegExpMatchArray,
    value: string,
    transitionsObject: Record<string, TransitionType>,
  ) {
    const [_, letters, dimensions] = match;
    const path = this.inflateKey(letters);
    const parts = path.split('/');
    const activity = parts[0];
    const isCreate = path.endsWith('/output/metadata/ac');
    const isUpdate = path.endsWith('/output/metadata/au');
    //for now only export activity start/stop; activity data would also be interesting
    if (isCreate || isUpdate) {
      const targetName = `${activity},${dimensions}`;
      const target = transitionsObject[targetName];
      if (!target) {
        transitionsObject[targetName] = {
          activity,
          dimensions,
          created: isCreate ? value : null,
          updated: isUpdate ? value : null,
        };
      } else {
        target[isCreate ? 'created' : 'updated'] = value;
      }
    }
  }

  sortEntriesByCreated(obj: {
    [key: string]: TransitionType;
  }): TransitionType[] {
    const entriesArray: TransitionType[] = Object.values(obj);
    entriesArray.sort((a, b) => {
      return (a.created || a.updated).localeCompare(b.created || b.updated);
    });
    return entriesArray;
  }

  /**
   * marker names are overloaded with details like sequence, type, etc
   */
  keyToObject(key: string): {
    index: number;
    dimension?: string;
    secondary?: number;
  } {
    function extractDimension(label: string): string {
      const parts = label.split(',');
      if (parts.length > 1) {
        parts.shift();
        return parts.join(',');
      }
    }
    const parts = key.split('-');
    if (parts.length === 4) {
      //-proxy-5-     -search-1-1-
      return {
        index: parseInt(parts[2], 10),
        dimension: extractDimension(parts[1]),
      };
    } else {
      //-search,0,0-1-1-    -proxy,0,0-1-
      return {
        index: parseInt(parts[2], 10),
        secondary: parseInt(parts[3], 10),
        dimension: extractDimension(parts[1]),
      };
    }
  }

  /**
   * idem list has a complicated sort order based on indexes and dimensions
   */
  sortParts(parts: TimelineType[]): TimelineType[] {
    return parts.sort((a, b) => {
      const { dimension: aDim, index: aIdx, secondary: aSec } = a;
      const { dimension: bDim, index: bIdx, secondary: bSec } = b;

      if (aDim === undefined && bDim !== undefined) return -1;
      if (aDim !== undefined && bDim === undefined) return 1;
      if (aDim !== undefined && bDim !== undefined) {
        if (aDim < bDim) return -1;
        if (aDim > bDim) return 1;
      }

      if (aIdx < bIdx) return -1;
      if (aIdx > bIdx) return 1;

      if (aSec === undefined && bSec !== undefined) return -1;
      if (aSec !== undefined && bSec === undefined) return 1;
      if (aSec !== undefined && bSec !== undefined) {
        if (aSec < bSec) return -1;
        if (aSec > bSec) return 1;
      }

      return 0;
    });
  }
}

export {
  ExporterService,
  parseTimestamp,
  computeDuration,
  extractOperation,
  extractActivityName,
  isSystemActivity,
  mapStatus,
};
