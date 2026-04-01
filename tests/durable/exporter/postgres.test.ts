import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { guid, sleepFor } from '../../../modules/utils';
import { Durable } from '../../../services/durable';
import { WorkflowHandleService } from '../../../services/durable/handle';
import { ProviderNativeClient } from '../../../types';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';
import { WorkflowExecution } from '../../../types/exporter';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | exporter | enrichment | Postgres', () => {
  let handle: WorkflowHandleService;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);
  });

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  describe('Worker & Client Setup', () => {
    it('should create and run a worker', async () => {
      const worker = await Worker.create({
        connection: {
          class: Postgres,
          options: postgres_options,
        },
        taskQueue: 'exporter-enrichment-test',
        workflow: workflows.testWorkflow,
      });
      await worker.run();
      expect(worker).toBeDefined();
    }, 10_000);

    it('should start a workflow execution', async () => {
      const client = new Client({
        connection: {
          class: Postgres,
          options: postgres_options,
        },
      });

      handle = await client.workflow.start({
        args: ['HotMesh', 42],
        taskQueue: 'exporter-enrichment-test',
        workflowName: 'testWorkflow',
        workflowId: 'exporter-test-' + guid(),
        expire: 180,
        signalIn: false,
      });
      expect(handle.workflowId).toBeDefined();
    }, 10_000);

    it('should complete the workflow', async () => {
      const result = await handle.result() as { greeting: string; doubled: number };
      expect(result).toBeDefined();
      expect(result.greeting).toBe('Hello, HotMesh!');
      expect(result.doubled).toBe(84);
    }, 10_000);
  });

  describe('Workflow input payload', () => {
    it('should not include workflow input args in job hash (raw export data field)', async () => {
      const raw = await handle.export();
      // The trigger job.maps only writes `done: false`.
      // Workflow arguments are transient — passed to the worker via
      // worker_streams but not persisted as _-prefixed keys in the job hash.
      expect(raw.data).toEqual({});
    }, 10_000);

    it('should enrich workflow_execution_started with input args from worker_streams', async () => {
      const execution: WorkflowExecution = await handle.exportExecution({
        enrich_inputs: true,
      });

      const startedEvent = execution.events.find(
        (e) => e.event_type === 'workflow_execution_started',
      );
      expect(startedEvent).toBeDefined();

      // With enrich_inputs, the exporter recovers the workflow's input
      // arguments from the first worker invocation in worker_streams.
      const input = (startedEvent?.attributes as any).input;
      expect(input).toBeDefined();
      expect(Array.isArray(input)).toBe(true);
      expect(input).toEqual(['HotMesh', 42]);
    }, 10_000);

    it('should NOT include workflow input args without enrich_inputs', async () => {
      const execution: WorkflowExecution = await handle.exportExecution({
        enrich_inputs: false,
      });

      const startedEvent = execution.events.find(
        (e) => e.event_type === 'workflow_execution_started',
      );
      expect(startedEvent).toBeDefined();

      // Without enrichment, input comes from raw.data which is empty
      const input = (startedEvent?.attributes as any).input;
      expect(input).toEqual({});
    }, 10_000);
  });

  describe('Late-binding Export (sparse, no input enrichment)', () => {
    it('should export without enriching inputs', async () => {
      const execution: WorkflowExecution = await handle.exportExecution({
        enrich_inputs: false,
      });

      expect(execution).toBeDefined();
      expect(execution.workflow_id).toBe(handle.workflowId);
      expect(execution.events.length).toBeGreaterThan(0);

      // Activity events should exist but without inputs
      const activityScheduled = execution.events.find(
        (e) => e.event_type === 'activity_task_scheduled',
      );
      expect(activityScheduled).toBeDefined();
      expect(activityScheduled?.attributes).toHaveProperty('activity_type');
      expect(activityScheduled?.attributes).toHaveProperty('timeline_key');
      expect((activityScheduled?.attributes as any).input).toBeUndefined();

      const activityCompleted = execution.events.find(
        (e) => e.event_type === 'activity_task_completed',
      );
      expect(activityCompleted).toBeDefined();
      expect((activityCompleted?.attributes as any).input).toBeUndefined();
    }, 10_000);
  });

  describe('Early-binding Export (enriched with inputs)', () => {
    it('should export with enriched activity inputs', async () => {
      const execution: WorkflowExecution = await handle.exportExecution({
        enrich_inputs: true,
      });

      expect(execution).toBeDefined();
      expect(execution.workflow_id).toBe(handle.workflowId);
      expect(execution.events.length).toBeGreaterThan(0);

      // Activity events should now have inputs enriched
      const activityScheduled = execution.events.find(
        (e) => e.event_type === 'activity_task_scheduled',
      );
      expect(activityScheduled).toBeDefined();
      const scheduledAttrs = activityScheduled?.attributes as any;
      expect(scheduledAttrs.input).toBeDefined();
      expect(Array.isArray(scheduledAttrs.input)).toBe(true);

      const activityCompleted = execution.events.find(
        (e) => e.event_type === 'activity_task_completed',
      );
      expect(activityCompleted).toBeDefined();
      const completedAttrs = activityCompleted?.attributes as any;
      expect(completedAttrs.input).toBeDefined();
      expect(completedAttrs.result).toBeDefined();

      // Verify the input matches what was passed to the activity
      if (completedAttrs.activity_type === 'greet') {
        expect(completedAttrs.input).toContain('HotMesh');
      } else if (completedAttrs.activity_type === 'doubleValue') {
        expect(completedAttrs.input).toContain(42);
      }
    }, 10_000);

    it('should work with both late and early binding in sequence', async () => {
      // First call without enrichment
      const sparse = await handle.exportExecution({ enrich_inputs: false });
      const sparseActivity = sparse.events.find(e => e.event_type === 'activity_task_completed');
      expect((sparseActivity?.attributes as any).input).toBeUndefined();

      // Second call with enrichment
      const enriched = await handle.exportExecution({ enrich_inputs: true });
      const enrichedActivity = enriched.events.find(e => e.event_type === 'activity_task_completed');
      expect((enrichedActivity?.attributes as any).input).toBeDefined();

      // Verify same number of events
      expect(sparse.events.length).toBe(enriched.events.length);
    }, 10_000);
  });

  describe('Summary and Metadata', () => {
    it('should include execution summary regardless of enrichment mode', async () => {
      const execution = await handle.exportExecution({ enrich_inputs: true });

      expect(execution.summary).toBeDefined();
      expect(execution.summary.total_events).toBeGreaterThan(0);
      expect(execution.summary.activities).toBeDefined();
      expect(execution.summary.activities.total).toBeGreaterThan(0);

      expect(execution.start_time).toBeDefined();
      expect(execution.close_time).toBeDefined();
      expect(execution.duration_ms).toBeGreaterThanOrEqual(0);
    }, 10_000);
  });
});
