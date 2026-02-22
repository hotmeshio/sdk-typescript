import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { dropTables, postgres_options } from '../../$setup/postgres';
import { deterministicRandom, guid, sleepFor } from '../../../modules/utils';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { MemFlow } from '../../../services/memflow';
import { ClientService } from '../../../services/memflow/client';
import { ProviderNativeClient } from '../../../types/provider';

import * as workflows from './src/workflows';

const { Connection, Client, Worker } = MemFlow;

describe('MEMFLOW | baseline | Postgres', () => {
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
    await MemFlow.shutdown();
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
});
