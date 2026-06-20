import { CollatorService } from '../collator';
import { Pipe } from '../pipe';
import { TaskService } from '../task';
import { TelemetryService } from '../telemetry';
import { HookActivity } from '../../types/activity';
import { HookRule } from '../../types/hook';
import { JobStatus } from '../../types/job';
import { ProviderTransaction } from '../../types/provider';
import {
  StreamCode,
  StreamData,
  StreamDataType,
  StreamStatus,
} from '../../types/stream';
import { CollationError } from '../../modules/errors';
import { CollationFaultType } from '../../types/collator';
import { guid, sleepFor } from '../../modules/utils';

import { Activity } from './activity';

/**
 * The most flexible activity type in the HotMesh YAML DAG. Depending on
 * its configuration it operates as one of four distinct flavors:
 *
 * | Flavor | Key field | Behavior |
 * |---|---|---|
 * | **Time** | `sleep` | Pauses for a duration, then resumes |
 * | **Signal** | `hook.topic` | Pauses until an external signal arrives |
 * | **Cycle** | `cycle: true` | Passthrough that also accepts re-entry from a `cycle` activity |
 * | **Passthrough** | _(none)_ | Maps data and transitions immediately |
 *
 * ---
 *
 * ## Flavor 1 — Time Hook (sleep)
 *
 * Pauses the flow for `sleep` seconds. The value can be a literal number
 * or a `@pipe` expression (e.g., for exponential backoff).
 *
 * ```yaml
 * activities:
 *   t1:
 *     type: trigger
 *
 *   wait_30s:
 *     type: hook
 *     sleep: 30                            # pause for 30 seconds
 *     job:
 *       maps:
 *         paused_at: '{$self.output.metadata.ac}'
 *
 *   next_step:
 *     type: hook
 *
 * transitions:
 *   t1:
 *     - to: wait_30s
 *   wait_30s:
 *     - to: next_step
 * ```
 *
 * Dynamic delay with `@pipe` (exponential backoff on retry):
 *
 * ```yaml
 * wait_retry:
 *   type: hook
 *   sleep:
 *     '@pipe':
 *       - ['{$self.output.data.attempt}', 2]
 *       - ['{@math.pow}']           # 1, 2, 4, 8, 16 …
 * ```
 *
 * ---
 *
 * ## Flavor 2 — Signal Hook (webhook)
 *
 * Registers a listener on a named topic. The flow pauses until an
 * external caller delivers a signal. Signal data is available as
 * `$self.hook.data`. The graph-level `hooks` section routes incoming
 * signals to the waiting activity via a `conditions.match` rule.
 *
 * **Send the signal** from any process:
 *
 * ```typescript
 * await hotMesh.signal('order.approval', { id: jobId, approved: true });
 * ```
 *
 * **Claim and delete** a pending signal via the collator key:
 *
 * ```typescript
 * // Ack/delete — deliver a signal and clear the hook registration
 * await hotMesh.signal('order.approval', { id: jobId, approved: false });
 * ```
 *
 * **YAML configuration:**
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: order.placed
 *       expire: 3600
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         wait_for_approval:
 *           type: hook
 *           hook:
 *             type: object
 *             properties:
 *               approved: { type: boolean }
 *           job:
 *             maps:
 *               approved: '{$self.hook.data.approved}'
 *
 *         done:
 *           type: hook
 *
 *       transitions:
 *         t1:
 *           - to: wait_for_approval
 *         wait_for_approval:
 *           - to: done
 *
 *       hooks:
 *         order.approval:                  # topic delivering the signal
 *           - to: wait_for_approval
 *             conditions:
 *               match:
 *                 - expected: '{t1.output.data.id}'   # job ID
 *                   actual: '{$self.hook.data.id}'    # signal payload ID
 * ```
 *
 * ### Signal Hook with Escalation
 *
 * Adding an `escalation:` block causes the hook activity to write one row
 * to `public.hmsh_escalations` atomically inside its Leg 1 transaction —
 * the same database commit that checkpoints job state. The row is
 * immediately queryable and claimable by any external system.
 *
 * All field values support `@pipe` expressions so they can reference job
 * data computed by earlier activities (e.g., `'{t1.output.data.region}'`).
 *
 * ```yaml
 * wait_for_approval:
 *   type: hook
 *   hook:
 *     type: object
 *     properties:
 *       approved: { type: boolean }
 *   escalation:
 *     role: manager                            # RBAC role that should act
 *     type: order-approval
 *     subtype: regional
 *     priority: 2                              # lower = higher priority
 *     description: Approve or reject the order
 *     entity: '{t1.output.data.entityType}'
 *     metadata:
 *       orderId: '{t1.output.data.orderId}'
 *       region: '{t1.output.data.region}'
 *     envelope:
 *       instructions: Review the attached order and approve or reject
 *     expiresAt: '{t1.output.data.dueDate}'
 *   job:
 *     maps:
 *       approved: '{$self.hook.data.approved}'
 * ```
 *
 * **Claim and resolve the escalation** (resumes the waiting workflow):
 *
 * ```typescript
 * // Find pending approvals for the manager role
 * const [item] = await client.escalations.list({ role: 'manager', status: 'pending' });
 *
 * // Claim it (sets assigned_to + claim_expires_at)
 * const claim = await client.escalations.claim({
 *   id: item.id,
 *   assignee: 'alice@company.com',
 *   durationMinutes: 30,
 * });
 *
 * // Resolve atomically delivers the signal and resumes the workflow
 * await client.escalations.resolve({
 *   id: item.id,
 *   resolverPayload: { approved: true },
 * });
 * ```
 *
 * ---
 *
 * ## Flavor 3 — Cycle Pivot
 *
 * A passthrough hook with `cycle: true` acts as the named re-entry point
 * for a `cycle` activity. On first entry it behaves identically to a
 * passthrough (maps data, transitions forward). When a `cycle` activity
 * downstream names it as its `ancestor`, the engine routes execution back
 * to it, allowing a controlled loop without spawning a new job.
 *
 * ```yaml
 * app:
 *   id: myapp
 *   version: '1'
 *   graphs:
 *     - subscribes: job.start
 *       expire: 120
 *
 *       activities:
 *         t1:
 *           type: trigger
 *
 *         pivot:
 *           type: hook
 *           cycle: true                    # marks this as a loop re-entry point
 *
 *         do_work:
 *           type: worker
 *           topic: work.process
 *           output:
 *             schema:
 *               type: object
 *               properties:
 *                 counter: { type: number }
 *           job:
 *             maps:
 *               counter: '{$self.output.data.counter}'
 *
 *         loop_back:
 *           type: cycle
 *           ancestor: pivot               # jumps back to `pivot` when condition holds
 *
 *       transitions:
 *         t1:
 *           - to: pivot
 *         pivot:
 *           - to: do_work
 *         do_work:
 *           - to: loop_back
 *             conditions:
 *               match:
 *                 - expected: true
 *                   actual:
 *                     '@pipe':
 *                       - ['{do_work.output.data.counter}', 5]
 *                       - ['{@conditional.less_than}']
 * ```
 *
 * ---
 *
 * ## Flavor 4 — Passthrough
 *
 * When none of `sleep`, `hook`, or `cycle` is set, the hook activity
 * immediately maps data and transitions to its children. Useful as a
 * data transformation node or fan-in convergence point.
 *
 * ```yaml
 * merge:
 *   type: hook
 *   output:
 *     maps:
 *       total: '{a1.output.data.subtotal}'   # copy field into activity output
 *   job:
 *     maps:
 *       total: '{$self.output.data.total}'   # promote to job-level data
 * ```
 *
 * ---
 *
 * ## Execution Model
 *
 * - **Time and Signal flavors** — Category A (duplex). Leg 1 registers the
 *   hook (timer or webhook), saves state, and commits. Leg 2 fires when the
 *   timer fires or the external signal arrives.
 * - **Cycle and Passthrough flavors** — Category B. Uses the crash-safe
 *   `executeLeg1StepProtocol` (GUID ledger backed) to map data and
 *   immediately transition to adjacent activities.
 *
 * @see {@link HookActivity} for the TypeScript interface
 */
class Hook extends Activity {
  config: HookActivity;

  async process(): Promise<string> {
    this.logger.debug('hook-process', {
      jid: this.context.metadata.jid,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    let telemetry: TelemetryService;

    try {
      //Phase 1: Load state and verify entry (all paths use verifyLeg1Entry
      //for GUID-ledger-backed crash recovery)
      const isResume = await this.verifyLeg1Entry();

      telemetry = new TelemetryService(
        this.engine.appId,
        this.config,
        this.metadata,
        this.context,
      );
      telemetry.startActivitySpan(this.leg);

      //Phase 2: Route based on RUNTIME evaluation (not static config)
      if (this.isConfiguredAsHook() && this.doesHook()) {
        //Category A: duplexed hook registration (Leg2 handles completion)
        if (!isResume) {
          await this.doHook(telemetry);
        } else if (this.config.hook?.topic) {
          //DUPLICATE: Leg1 completed previously but hook registration
          //may not have happened (crash between transaction.exec and
          //registerWebHookSignal). Attempt registration — setHookSignal
          //is idempotent (returns success:false if hook already exists).
          const hookResult = await this.registerWebHookSignal();
          const pending = hookResult && hookResult.pending;
          if (pending) {
            await this.redeliverPendingSignal(pending);
          }
        }
      } else {
        //Category B: passthrough with crash-safe step protocol + GUID ledger
        await this.doPassThrough(telemetry);
      }

      return this.context.metadata.aid;
    } catch (error) {
      this.handleProcessError(error, telemetry, 'hook');
      return;
    } finally {
      telemetry?.endActivitySpan();
      this.logger.debug('hook-process-end', {
        jid: this.context.metadata.jid,
        gid: this.context.metadata.gid,
        aid: this.metadata.aid,
      });
    }
  }

  isConfiguredAsHook(): boolean {
    return !!this.config.sleep || !!this.config.hook?.topic;
  }

  doesHook(): boolean {
    if (this.config.hook?.topic) {
      return true;
    }
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      return !isNaN(duration) && Number(duration) > 0;
    }
    return false;
  }

  async doHook(telemetry: TelemetryService): Promise<void> {
    const transaction = this.store.transact();

    //register time hooks (sleep) inside the transaction — these
    //don't participate in the signal race
    await this.registerTimeHook(transaction);

    this.mapOutputData();
    this.mapJobData();
    await this.setState(transaction);
    await CollatorService.notarizeLeg1Completion(this, transaction);
    await this.setStatus(0, transaction);

    //enqueue escalation INSERT inside the Leg1 transaction so it is
    //written atomically with the job state checkpoint — one committed
    //unit, crash-safe, no separate recovery path needed.
    const escalationSignalKey = await this.addEscalationToTransaction(transaction);

    // exec() returns one result per queued command; the escalation INSERT
    // (with RETURNING *) is the last command — its row is results[last].
    const execResults = await transaction.exec();
    telemetry.mapActivityAttributes();

    //register the web hook signal AFTER the transaction commits.
    //this eliminates the FORBIDDEN window: the hook signal is never
    //visible before Leg1 completion. If Leg2 arrives before this
    //point, getHookSignal finds no hook and stores $pending, which
    //setHookSignal will detect and return for redelivery.
    const hookResult = await this.registerWebHookSignal();
    const pending = hookResult && hookResult.pending;

    if (pending) {
      await this.redeliverPendingSignal(pending);
    }

    // Post-commit: fire the escalation.created event using the row returned
    // directly from the RETURNING * clause — no extra SELECT.
    if (escalationSignalKey) {
      const store = this.store as any;
      if (store.eventsPublish) {
        const row = execResults[execResults.length - 1];
        if (row?.id) {
          const ts = new Date().toISOString();
          const updatedAt = row.updated_at ? new Date(row.updated_at).toISOString() : ts;
          void Promise.resolve(store.eventsPublish({
            event_id: `${row.id}:created:${updatedAt}`,
            type: `system.escalation.${row.id}.created`,
            ts,
            namespace: row.namespace ?? (this.engine as any).namespace ?? this.engine.appId,
            app_id: row.app_id ?? this.engine.appId,
            workflow_id: row.workflow_id ?? undefined,
            topic: row.topic ?? undefined,
            origin_id: row.origin_id ?? undefined,
            parent_id: row.parent_id ?? undefined,
            trace_id: row.trace_id ?? undefined,
            span_id: row.span_id ?? undefined,
            data: row,
          })).catch(() => { /* best-effort */ });
        }
      }
    }
  }

  private async addEscalationToTransaction(
    transaction: import('../../types/provider').ProviderTransaction,
  ): Promise<string | null> {
    if (!this.config.escalation || !this.config.hook?.topic) return null;
    const store = this.store as any;
    if (typeof store.addEscalationToTransaction !== 'function') return null;

    const escalationConfig = this.config.escalation as Record<string, unknown>;
    const jid = this.context.metadata.jid;
    const appId = this.engine.appId;
    const namespace = (this.engine as any).namespace ?? appId;
    const resolveField = (v: unknown) =>
      typeof v === 'string' ? Pipe.resolve(v, this.context) : v;

    // Resolve metadata/envelope: a string value is a pipe expression that resolves
    // to an object (factory path); an object value has per-key pipe expressions
    // (YAML DAG path).
    const resolveObj = (v: unknown): unknown => {
      if (typeof v === 'string') return Pipe.resolve(v, this.context);
      if (!v || typeof v !== 'object' || Array.isArray(v)) return v;
      const out: Record<string, unknown> = {};
      for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
        out[k] = typeof val === 'string' ? Pipe.resolve(val, this.context) : val;
      }
      return out;
    };

    const params = {
      type: resolveField(escalationConfig.type),
      subtype: resolveField(escalationConfig.subtype),
      entity: resolveField(escalationConfig.entity),
      description: resolveField(escalationConfig.description),
      role: resolveField(escalationConfig.role),
      priority: resolveField(escalationConfig.priority),
      originId: resolveField(escalationConfig.originId),
      parentId: resolveField(escalationConfig.parentId),
      initiatedBy: resolveField(escalationConfig.initiatedBy),
      traceId: resolveField(escalationConfig.traceId),
      spanId: resolveField(escalationConfig.spanId),
      metadata: resolveObj(escalationConfig.metadata),
      envelope: resolveObj(escalationConfig.envelope),
      expiresAt: resolveField(escalationConfig.expiresAt),
      taskQueue: resolveField(escalationConfig.taskQueue),
      workflowType: resolveField(escalationConfig.workflowType),
    };

    // Skip the INSERT when no escalation fields resolved — this happens when
    // the factory waiter runs for a condition() call that had no queueConfig.
    if (
      params.role == null && params.type == null &&
      params.priority == null && params.metadata == null
    ) return null;

    // Derive signal_key from the hook rule's expected condition — the same
    // value registerWebHook stores as the signal lookup key.
    const hookRule = await this.getHookRule(this.config.hook.topic);
    const signalKey = hookRule?.conditions?.match?.[0]?.expected
      ? Pipe.resolve(hookRule.conditions.match[0].expected as string, this.context)
      : jid;

    store.addEscalationToTransaction({
      namespace,
      appId,
      signalKey,
      topic: this.config.hook.topic,
      workflowId: jid,
      ...params,
    }, transaction);

    return signalKey as string;
  }

  /**
   * Re-publishes a pending signal as a WEBHOOK stream message so the
   * normal leg2 dispatch path processes it. Called when leg1's
   * setHookSignal atomically detected and consumed a pending signal.
   */
  private async redeliverPendingSignal(pendingJson: string): Promise<void> {
    const data = JSON.parse(pendingJson);
    const hookRule = await this.getHookRule(this.config.hook.topic);
    this.logger.warn('hook-pending-signal-redelivery', {
      topic: this.config.hook.topic,
      aid: hookRule?.to || this.metadata.aid,
      jid: this.context.metadata.jid,
    });
    const streamData: StreamData = {
      type: StreamDataType.WEBHOOK,
      status: StreamStatus.SUCCESS,
      code: 200,
      metadata: {
        guid: guid(),
        aid: hookRule?.to || this.metadata.aid,
        topic: this.config.hook.topic,
      },
      data,
    };
    await this.engine.router?.publishMessage(null, streamData);
  }

  async doPassThrough(telemetry: TelemetryService): Promise<void> {
    this.adjacencyList = await this.filterAdjacent();
    this.mapOutputData();
    this.mapJobData();
    await this.executeLeg1StepProtocol(this.adjacencyList.length - 1);
    telemetry.mapActivityAttributes();
    telemetry.setActivityAttributes({});
  }

  async getHookRule(topic: string): Promise<HookRule | undefined> {
    const rules = await this.store.getHookRules();
    return rules?.[topic]?.[0] as HookRule;
  }

  /**
   * Register the time hook (sleep) inside the Leg1 transaction.
   * Time hooks don't participate in the signal race — they're
   * purely internal timeout registrations.
   */
  async registerTimeHook(
    transaction: ProviderTransaction,
  ): Promise<void> {
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      if (!isNaN(duration) && Number(duration) > 0) {
        await this.engine.taskService.registerTimeHook(
          this.context.metadata.jid,
          this.context.metadata.gid,
          `${this.metadata.aid}${this.metadata.dad || ''}`,
          'sleep',
          duration,
          this.metadata.dad || '',
          transaction,
        );
      }
    }
  }

  /**
   * Register the web hook signal AFTER the Leg1 transaction commits.
   * This ensures the hook signal is never visible before Leg1
   * completion, eliminating the FORBIDDEN window where Leg2 could
   * find the hook but fail on the collation check.
   *
   * If a pending signal was stored by an early-arriving Leg2,
   * setHookSignal atomically detects and returns it.
   */
  async registerWebHookSignal(): Promise<{ pending?: string } | void> {
    if (this.config.hook?.topic) {
      const hookResult = await this.engine.taskService.registerWebHook(
        this.config.hook.topic,
        this.context,
        this.resolveDad(),
        this.context.metadata.expire,
      );
      if (hookResult.pending) {
        return { pending: hookResult.pending };
      }
    }
  }

  /**
   * @deprecated Use registerTimeHook + registerWebHookSignal instead.
   * Kept for backward compatibility with tests that monkey-patch this method.
   */
  async registerHook(
    transaction?: ProviderTransaction,
  ): Promise<{ jobId?: string; pending?: string } | void> {
    let jobId: string | undefined;
    let pending: string | undefined;
    if (this.config.hook?.topic) {
      const hookResult = await this.engine.taskService.registerWebHook(
        this.config.hook.topic,
        this.context,
        this.resolveDad(),
        this.context.metadata.expire,
      );
      jobId = hookResult.jobId;
      pending = hookResult.pending;
    }
    if (this.config.sleep) {
      const duration = Pipe.resolve(this.config.sleep, this.context);
      if (!isNaN(duration) && Number(duration) > 0) {
        await this.engine.taskService.registerTimeHook(
          this.context.metadata.jid,
          this.context.metadata.gid,
          `${this.metadata.aid}${this.metadata.dad || ''}`,
          'sleep',
          duration,
          this.metadata.dad || '',
          transaction,
        );
        if (!jobId) jobId = this.context.metadata.jid;
      }
    }
    if (jobId) return { jobId, pending };
  }

  async processWebHookEvent(
    status: StreamStatus = StreamStatus.SUCCESS,
    code: StreamCode = 200,
  ): Promise<JobStatus | void> {
    this.logger.debug('hook-process-web-hook-event', {
      topic: this.config.hook.topic,
      aid: this.metadata.aid,
      status,
      code,
    });
    const taskService = new TaskService(this.store, this.logger);
    const data = { ...this.data };
    const signal = await taskService.processWebHookSignal(
      this.config.hook.topic,
      data,
    );
    if (signal) {
      const [jobId, _aid, dad, gId] = signal;
      this.context.metadata.jid = jobId;
      this.context.metadata.gid = gId;
      this.context.metadata.dad = dad;

      // Inline retry for FORBIDDEN: Leg2 arrived in the window between
      // setHookSignal (standalone) and Leg1 transaction.exec(). The 100B
      // ledger digit is not yet visible. Leg1 needs only milliseconds to
      // commit — retry here, inside the message processing loop, before
      // consumeOne's finally block acks the message. Stream-level retry
      // won't help: ENGINE consumers have no retry policy, so shouldRetry
      // returns [false, 0] and the message is ack'd with no retry.
      const MAX_FORBIDDEN_RETRIES = 5;
      const FORBIDDEN_RETRY_DELAY_MS = 50;
      for (let attempt = 0; attempt <= MAX_FORBIDDEN_RETRIES; attempt++) {
        try {
          await this.processEvent(status, code, 'hook');
          if (code === 200) {
            await taskService.deleteWebHookSignal(this.config.hook.topic, data);
            //clean up orphan pending on the sibling signal topic
            //  wfs.wait delivered → remove wfs.signal pending
            //  wfs.signal delivered → remove wfs.wait pending
            const topic = this.config.hook.topic;
            const siblingTopic = topic.includes('.wfs.wait')
              ? topic.replace('.wfs.wait', '.wfs.signal')
              : topic.includes('.wfs.signal')
                ? topic.replace('.wfs.signal', '.wfs.wait')
                : null;
            if (siblingTopic) {
              try {
                await taskService.deleteWebHookSignal(siblingTopic, data);
              } catch {
                //sibling entry may not exist — ignore
              }
            }
          }
          return;
        } catch (error) {
          if (error instanceof CollationError &&
              error.fault === CollationFaultType.FORBIDDEN &&
              attempt < MAX_FORBIDDEN_RETRIES) {
            this.logger.warn('hook-webhook-forbidden-inline-retry', {
              attempt: attempt + 1,
              maxAttempts: MAX_FORBIDDEN_RETRIES,
              jid: this.context.metadata.jid,
              aid: this.metadata.aid,
            });
            await sleepFor(FORBIDDEN_RETRY_DELAY_MS * (attempt + 1));
            continue;
          }
          throw error;
        }
      }
    }
  }

  async processTimeHookEvent(jobId: string): Promise<JobStatus | void> {
    this.logger.debug('hook-process-time-hook-event', {
      jid: jobId,
      gid: this.context.metadata.gid,
      aid: this.metadata.aid,
    });
    //if combined hook+sleep, clean up the webhook signal (timeout won the race)
    if (this.config.hook?.topic) {
      try {
        const taskService = new TaskService(this.store, this.logger);
        await taskService.deleteWebHookSignal(
          this.config.hook.topic,
          this.data,
        );
      } catch (e) {
        this.logger.debug('hook-timeout-signal-cleanup', {
          topic: this.config.hook.topic,
          error: e.message,
        });
      }
    }
    await this.processEvent(StreamStatus.SUCCESS, 200, 'hook');
  }
}

export { Hook };
