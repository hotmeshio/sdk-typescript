import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { dropTables, postgres_options } from '../../$setup/postgres';
import { deterministicRandom, guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { Durable } from '../../../services/durable';
import { ClientService } from '../../../services/durable/client';
import { ProviderNativeClient } from '../../../types/provider';
import {
  WorkflowExecution,
  WorkflowExecutionEvent,
} from '../../../types/exporter';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = Durable;

describe('DURABLE | baseline | Postgres', () => {
  let client: ClientService;
  let postgresClient: ProviderNativeClient;
  const connection = { class: Postgres, options: postgres_options };
  const taskQueue = 'basic-world';

  beforeAll(async () => {
    if (process.env.POSTGRES_IS_REMOTE === 'true') return;

    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    // Setup: connection + client
    await Connection.connect({ class: Postgres, options: postgres_options });
    client = new Client({ connection });

    // Register a worker for each workflow function under test
    const workflowFunctions = [
      workflows.example,
      workflows.childExample,
      workflows.testRandom,
      workflows.testProxyActivity,
      workflows.testSleep,
      workflows.testExecChild,
      workflows.testStartChild,
      workflows.testWaitFor,
      workflows.testParallelActivities,
      workflows.testActivityThenSignal,
      workflows.testSignalThenActivity,
      workflows.testSignalThenParallel,
      workflows.testSignalThenExecChild,
      workflows.testSignalThenStartChild,
      workflows.testParallelChildren,
      workflows.testSignalThenParallelChildren,
    ];
    for (const wf of workflowFunctions) {
      const worker = await Worker.create({
        connection,
        taskQueue,
        workflow: wf,
      });
      await worker.run();
    }
  }, 60_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  // ──── Feature isolation tests ────

  describe('Feature: random', () => {
    it('should return a deterministic random value', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testRandom',
        workflowId: 'test-random-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect(typeof result).toBe('number');
    }, 15_000);
  });

  describe('Feature: proxyActivity', () => {
    it('should call a proxy activity and return its result', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testProxyActivity',
        workflowId: 'test-proxy-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBeDefined();
    }, 15_000);
  });

  describe('Feature: sleepFor', () => {
    it('should sleep and return', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testSleep',
        workflowId: 'test-sleep-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBe('slept');
    }, 15_000);
  });

  describe('Feature: execChild', () => {
    it('should execute a child workflow and wait for result', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testExecChild',
        workflowId: 'test-execchild-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBe('child-done');
    }, 15_000);
  });

  describe('Feature: startChild', () => {
    it('should start a child workflow without waiting', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testStartChild',
        workflowId: 'test-startchild-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBeDefined();
    }, 15_000);
  });

  describe('Feature: waitFor (signal)', () => {
    it('should pause and resume when signaled', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testWaitFor',
        workflowId: 'test-waitfor-' + guid(),
        expire: 120,
      });
      // Wait for workflow to pause
      await sleepFor(3_000);
      // Send signal
      const signalPayload = { id: 'test-signal', data: { hello: 'world' } };
      await handle.signal('test-signal', signalPayload);
      const result = await handle.result();
      expect(result).toBeDefined();
    }, 25_000);
  });

  describe('Feature: Promise.all with activities', () => {
    it('should run parallel proxy activities', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testParallelActivities',
        workflowId: 'test-parallel-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBeDefined();
    }, 15_000);
  });

  describe('Feature: activity THEN waitFor', () => {
    it('should run an activity then pause for signal', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testActivityThenSignal',
        workflowId: 'test-act-then-signal-' + guid(),
        expire: 120,
      });
      // Wait for workflow to reach the waitFor
      await sleepFor(5_000);
      // Send signal
      const signalPayload = { id: 'test-signal', data: { hello: 'world' } };
      await handle.signal('test-signal', signalPayload);
      const result = await handle.result();
      expect(result).toBeDefined();
    }, 25_000);
  });

  // ──── Post-signal continuation tests ────

  describe('Feature: waitFor THEN activity', () => {
    it('should resume from signal and run an activity', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenActivity',
        workflowId: 'test-signal-then-act-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).greeting).toBeDefined();
    }, 25_000);
  });

  describe('Feature: waitFor THEN parallel activities', () => {
    it('should resume from signal and run parallel activities', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenParallel',
        workflowId: 'test-signal-then-par-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).results).toHaveLength(2);
    }, 25_000);
  });

  describe('Feature: waitFor THEN execChild', () => {
    it('should resume from signal and execute a child', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenExecChild',
        workflowId: 'test-signal-then-exec-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).childResult).toBe('child-done');
    }, 25_000);
  });

  describe('Feature: waitFor THEN startChild', () => {
    it('should resume from signal and start a child', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenStartChild',
        workflowId: 'test-signal-then-start-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).jobId).toBeDefined();
    }, 25_000);
  });

  describe('Feature: Promise.all([startChild, execChild]) no signal', () => {
    it('should run parallel children without signal', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testParallelChildren',
        workflowId: 'test-par-children-' + guid(),
        expire: 120,
      });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).jobId).toBeDefined();
    }, 25_000);
  });

  describe('Feature: waitFor THEN Promise.all([startChild, execChild])', () => {
    it('should resume from signal and run parallel children', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenParallelChildren',
        workflowId: 'test-signal-then-parchild-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      const result = await handle.result();
      expect(result).toBeDefined();
      expect((result as any).jobId).toBeDefined();
    }, 25_000);
  });

  describe('Full workflow (original)', () => {
    it('should return the full workflow execution result', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'example',
        workflowId: 'workflow-' + guid(),
        expire: 600,
      });
      const signalId = 'abcdefg';
      await sleepFor(10_000);
      const signalPayload = {
        id: signalId,
        data: { hello: 'world', id: signalId },
      };
      await handle.signal(signalId, signalPayload);
      const result = await handle.result();
      const r1 = deterministicRandom(1);
      const r2 = deterministicRandom(4);
      expect(result).toEqual({
        jobId: 'MyWorkflowId123',
        payload: { data: { hello: 'world', id: 'abcdefg' }, id: 'abcdefg' },
        proxyGreeting: { complex: 'Basic, HotMesh!' },
        proxyGreeting3: { complex: 'Basic, HotMesh3!' },
        proxyGreeting4: { complex: 'Basic, HotMesh4!' },
        random1: r1,
        random2: r2,
      });
    }, 45_000);
  });

  // ──── exportExecution: Temporal-like event history ────

  const EXPORT_DEBUG = !!process.env.EXPORT_DEBUG;

  /**
   * When EXPORT_DEBUG is set, dump the full execution JSON to stdout.
   * Usage: EXPORT_DEBUG=1 npm run test:durable:basic
   */
  function dumpExecution(label: string, exec: WorkflowExecution) {
    if (!EXPORT_DEBUG) return;
    console.log(`\n╔══ EXPORT_DEBUG: ${label} ══`);
    console.log(JSON.stringify(exec, null, 2));
    console.log(`╚══ /${label} ══\n`);
  }

  /**
   * Helper to validate the common shape of every WorkflowExecution,
   * regardless of which primitives the workflow used.
   */
  function assertExecutionShape(exec: WorkflowExecution) {
    expect(exec.workflow_id).toBeDefined();
    expect(exec.workflow_type).toBeDefined();
    expect(exec.task_queue).toBeDefined();
    expect(['running', 'completed', 'failed']).toContain(exec.status);
    expect(exec.events).toBeInstanceOf(Array);
    expect(exec.summary).toBeDefined();
    expect(exec.summary.total_events).toBe(exec.events.length);

    // Events must be chronologically sorted with sequential IDs
    for (let i = 0; i < exec.events.length; i++) {
      expect(exec.events[i].event_id).toBe(i + 1);
      if (i > 0) {
        expect(exec.events[i].event_time >= exec.events[i - 1].event_time).toBe(true);
      }
    }
  }

  function findEvents(
    events: WorkflowExecutionEvent[],
    type: string,
  ): WorkflowExecutionEvent[] {
    return events.filter((e) => e.event_type === type);
  }

  describe('exportExecution: proxyActivity', () => {
    it('should produce activity_task_scheduled and activity_task_completed events', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testProxyActivity',
        workflowId: 'test-export-proxy-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution();
      dumpExecution('proxyActivity', exec);

      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');
      expect(exec.start_time).toBeDefined();
      expect(exec.close_time).toBeDefined();
      expect(exec.duration_ms).toBeGreaterThanOrEqual(0);

      // Must have workflow_execution_started + activity pair + workflow_execution_completed
      expect(findEvents(exec.events, 'workflow_execution_started')).toHaveLength(1);
      expect(findEvents(exec.events, 'workflow_execution_completed')).toHaveLength(1);

      const scheduled = findEvents(exec.events, 'activity_task_scheduled');
      const completed = findEvents(exec.events, 'activity_task_completed');
      expect(scheduled.length).toBeGreaterThanOrEqual(1);
      expect(completed.length).toBeGreaterThanOrEqual(1);

      // Scheduled must precede completed
      expect(scheduled[0].event_id).toBeLessThan(completed[0].event_id);

      // Activity name must be extracted
      const attrs = scheduled[0].attributes;
      expect(attrs.kind).toBe('activity_task_scheduled');
      if (attrs.kind === 'activity_task_scheduled') {
        expect(attrs.activity_type).toBeDefined();
        expect(attrs.activity_type).not.toBe('unknown');
      }

      // Summary counters
      expect(exec.summary.activities.total).toBeGreaterThanOrEqual(1);
      expect(exec.summary.activities.completed).toBeGreaterThanOrEqual(1);
    }, 15_000);
  });

  describe('exportExecution: sleepFor', () => {
    it('should produce timer_started and timer_fired events', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testSleep',
        workflowId: 'test-export-sleep-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution();
      dumpExecution('sleepFor', exec);

      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');

      const timerStarted = findEvents(exec.events, 'timer_started');
      const timerFired = findEvents(exec.events, 'timer_fired');
      expect(timerStarted.length).toBeGreaterThanOrEqual(1);
      expect(timerFired.length).toBeGreaterThanOrEqual(1);
      expect(exec.summary.timers).toBeGreaterThanOrEqual(1);
    }, 15_000);
  });

  describe('exportExecution: execChild', () => {
    it('should produce child_workflow_execution_started and child_workflow_execution_completed events', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testExecChild',
        workflowId: 'test-export-execchild-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution();
      dumpExecution('execChild', exec);

      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');

      const started = findEvents(exec.events, 'child_workflow_execution_started');
      const completed = findEvents(exec.events, 'child_workflow_execution_completed');
      expect(started.length).toBeGreaterThanOrEqual(1);
      expect(completed.length).toBeGreaterThanOrEqual(1);

      // Child started with awaited=true
      const startAttrs = started[0].attributes;
      if (startAttrs.kind === 'child_workflow_execution_started') {
        expect(startAttrs.awaited).toBe(true);
        expect(startAttrs.child_workflow_id).toBeDefined();
      }

      expect(exec.summary.child_workflows.total).toBeGreaterThanOrEqual(1);
      expect(exec.summary.child_workflows.completed).toBeGreaterThanOrEqual(1);
    }, 15_000);
  });

  describe('exportExecution: startChild', () => {
    it('should produce child_workflow_execution_started with awaited=false', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testStartChild',
        workflowId: 'test-export-startchild-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution();
      dumpExecution('startChild', exec);

      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');

      const started = findEvents(exec.events, 'child_workflow_execution_started');
      expect(started.length).toBeGreaterThanOrEqual(1);

      // Fire-and-forget: awaited=false
      const startAttrs = started[0].attributes;
      if (startAttrs.kind === 'child_workflow_execution_started') {
        expect(startAttrs.awaited).toBe(false);
      }
    }, 15_000);
  });

  describe('exportExecution: waitFor (signal)', () => {
    it('should produce workflow_execution_signaled event after signaling', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testWaitFor',
        workflowId: 'test-export-waitfor-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      await handle.result();
      const exec = await handle.exportExecution();
      dumpExecution('waitFor', exec);

      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');

      const signals = findEvents(exec.events, 'workflow_execution_signaled');
      expect(signals.length).toBeGreaterThanOrEqual(1);
      expect(exec.summary.signals).toBeGreaterThanOrEqual(1);
    }, 25_000);
  });

  describe('exportExecution: sparse vs verbose mode', () => {
    it('sparse mode should not include children array', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testExecChild',
        workflowId: 'test-export-sparse-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution({ mode: 'sparse' });
      dumpExecution('sparse', exec);

      assertExecutionShape(exec);
      expect(exec.children).toBeUndefined();
    }, 15_000);

    it('verbose mode should include children with their own events', async () => {
      const handle = await client.workflow.start({
        args: ['test'],
        taskQueue,
        workflowName: 'testExecChild',
        workflowId: 'test-export-verbose-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution({ mode: 'verbose' });
      dumpExecution('verbose (execChild)', exec);

      assertExecutionShape(exec);
      expect(exec.children).toBeInstanceOf(Array);
      expect(exec.children?.length).toBeGreaterThanOrEqual(1);

      // Each child should have a valid execution shape
      for (const child of exec.children ?? []) {
        assertExecutionShape(child);
        expect(child.workflow_id).toBeDefined();
      }
    }, 15_000);
  });

  describe('exportExecution: omit_results', () => {
    it('should strip result and payload data when omit_results is true', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testProxyActivity',
        workflowId: 'test-export-omit-' + guid(),
        expire: 120,
      });
      await handle.result();
      const exec = await handle.exportExecution({ omit_results: true });
      dumpExecution('omit_results', exec);

      assertExecutionShape(exec);

      // workflow_execution_started input should be stripped
      const started = findEvents(exec.events, 'workflow_execution_started')[0];
      if (started?.attributes.kind === 'workflow_execution_started') {
        expect(started.attributes.input).toBeUndefined();
      }

      // activity_task_completed result should be stripped
      const completed = findEvents(exec.events, 'activity_task_completed');
      for (const evt of completed) {
        if (evt.attributes.kind === 'activity_task_completed') {
          expect(evt.attributes.result).toBeUndefined();
        }
      }

      // workflow_execution_completed result should be stripped
      const wfCompleted = findEvents(exec.events, 'workflow_execution_completed')[0];
      if (wfCompleted?.attributes.kind === 'workflow_execution_completed') {
        expect(wfCompleted.attributes.result).toBeUndefined();
      }
    }, 15_000);
  });

  describe('exportExecution: exclude_system', () => {
    it('should filter system activities when exclude_system is true', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testProxyActivity',
        workflowId: 'test-export-sysfilter-' + guid(),
        expire: 120,
      });
      await handle.result();

      const execAll = await handle.exportExecution({ exclude_system: false });
      const execFiltered = await handle.exportExecution({ exclude_system: true });
      dumpExecution('exclude_system=false', execAll);
      dumpExecution('exclude_system=true', execFiltered);

      assertExecutionShape(execAll);
      assertExecutionShape(execFiltered);

      // Filtered should have <= events (system activities removed)
      expect(execFiltered.events.length).toBeLessThanOrEqual(execAll.events.length);

      // No system events in filtered output
      for (const evt of execFiltered.events) {
        if (evt.event_type === 'activity_task_scheduled') {
          expect(evt.is_system).toBe(false);
        }
      }
    }, 15_000);
  });

  describe('exportExecution: combined (signal + execChild)', () => {
    it('should export an execution with signals and child workflows', async () => {
      const handle = await client.workflow.start({
        args: ['HotMesh'],
        taskQueue,
        workflowName: 'testSignalThenExecChild',
        workflowId: 'test-export-combined-' + guid(),
        expire: 120,
      });
      await sleepFor(3_000);
      await handle.signal('test-signal', { id: 'test-signal', data: { hello: 'world' } });
      await handle.result();

      const exec = await handle.exportExecution();
      dumpExecution('combined (sparse)', exec);
      assertExecutionShape(exec);
      expect(exec.status).toBe('completed');

      // Should have signal + child workflow events
      expect(exec.summary.signals).toBeGreaterThanOrEqual(1);
      expect(exec.summary.child_workflows.total).toBeGreaterThanOrEqual(1);

      // First event should be workflow_execution_started, last should be workflow_execution_completed
      expect(exec.events[0].event_type).toBe('workflow_execution_started');
      expect(exec.events[exec.events.length - 1].event_type).toBe('workflow_execution_completed');

      // Kitchen sink: verbose mode with full results and all activities
      const verbose = await handle.exportExecution({ mode: 'verbose' });
      dumpExecution('KITCHEN SINK (verbose + signal + child)', verbose);
      assertExecutionShape(verbose);
      expect(verbose.children).toBeInstanceOf(Array);
      expect(verbose.children?.length).toBeGreaterThanOrEqual(1);
    }, 25_000);
  });
});
