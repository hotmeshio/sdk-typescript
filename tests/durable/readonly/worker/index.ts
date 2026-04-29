/**
 * Standalone non-readonly worker process.
 *
 * Runs in its own container (hotmesh-readonly-worker). This is the
 * ONLY process that hosts a worker, registers activities, registers
 * interceptors, and consumes stream messages.
 *
 * The readonly test container (hotmesh-readonly) connects with
 * readonly=true and can only enqueue jobs — it depends on THIS
 * process to actually execute them.
 */
import { Client as Postgres } from 'pg';

import { Durable } from '../../../../services/durable';
import {
  WorkflowInboundCallsInterceptor,
  WorkflowOutboundCallsInterceptor,
} from '../../../../types/durable';
import { guid } from '../../../../modules/utils';
import { PostgresConnection } from '../../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../../$setup/postgres';

import * as workflows from '../src/workflows';
import * as activities from '../src/activities';

const { Worker } = Durable;

const TASK_QUEUE = 'readonly-test';

async function main() {
  const connection = { class: Postgres, options: postgres_options };

  // ── Drop stale data from prior runs ──────────────────────────
  console.log('[readonly-worker] Dropping tables from prior runs...');
  const pgClient = (
    await PostgresConnection.connect(guid(), Postgres, postgres_options)
  ).getClient();
  await dropTables(pgClient);

  // ── Register interceptors ────────────────────────────────────
  console.log('[readonly-worker] Registering interceptors...');

  const loggingInterceptor: WorkflowInboundCallsInterceptor = {
    async execute(_ctx, next) {
      try {
        return await next();
      } catch (err) {
        if (Durable.didInterrupt(err)) throw err;
        throw err;
      }
    },
  };
  Durable.registerInboundInterceptor(loggingInterceptor);

  // Outbound interceptor that calls a proxy activity before each
  // activity — would stall if the readonly engine tried to route it.
  const auditInterceptor: WorkflowOutboundCallsInterceptor = {
    async execute(activityCtx, workflowCtx, next) {
      try {
        if (activityCtx.activityName !== 'auditLog') {
          const { auditLog } = Durable.workflow.proxyActivities<
            typeof activities
          >({
            activities,
            taskQueue: 'readonly-shared-activities',
            retry: { maximumAttempts: 3, throwOnError: true },
          });
          await auditLog(
            workflowCtx.get('workflowId'),
            `before:${activityCtx.activityName}`,
          );
        }
        return await next();
      } catch (err) {
        if (Durable.didInterrupt(err)) throw err;
        throw err;
      }
    },
  };
  Durable.registerOutboundInterceptor(auditInterceptor);

  // ── Register shared activity worker for interceptor calls ────
  await Durable.registerActivityWorker(
    { connection, taskQueue: 'readonly-shared-activities' },
    activities,
    'readonly-shared-activities',
  );

  // ── Start workers ────────────────────────────────────────────
  console.log('[readonly-worker] Starting workers...');

  for (const wf of [
    workflows.readonlyExample,
    workflows.robustWorkflow,
    workflows.metadataWorkflow,
  ]) {
    const worker = await Worker.create({
      connection,
      taskQueue: TASK_QUEUE,
      workflow: wf,
    });
    await worker.run();
  }

  // Signal readiness — the healthcheck file lets docker-compose
  // know this container is ready before starting the test runner.
  const fs = await import('fs');
  fs.writeFileSync('/tmp/worker-ready', 'ok');
  console.log('[readonly-worker] READY — all workers running.');

  const shutdown = async () => {
    console.log('[readonly-worker] Shutting down...');
    await Durable.shutdown();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  console.error('[readonly-worker] Fatal error:', err);
  process.exit(1);
});
