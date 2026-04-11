import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { guid, sleepFor } from '../../../modules/utils';
import { JobOutput } from '../../../types/job';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';

describe('FUNCTIONAL | Hook Timeout | Postgres', () => {
  const appConfig = { id: 'hook-timeout' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  beforeAll(async () => {
    postgresClient = (
      await PostgresConnection.connect(guid(), Postgres, postgres_options)
    ).getClient();

    await dropTables(postgresClient);

    const hmshConfig: HotMeshConfig = {
      appId: appConfig.id,
      logLevel: HMSH_LOGLEVEL,

      engine: {
        connection: { class: Postgres, options: postgres_options },
      },
    };

    hotMesh = await HotMesh.init(hmshConfig);
    await hotMesh.deploy(
      '/app/tests/$setup/apps/hook-timeout/v1/hotmesh.yaml',
    );
    await hotMesh.activate('1');
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Signal wins the race', () => {
    it('completes with signal data when signal arrives before timeout', async () => {
      let isDone = false;
      let resultData: Record<string, any> = {};
      const job_id = `sig-wins-${guid()}`;

      // Subscribe to the output
      await hotMesh.sub(
        `hook.timeout.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          resultData = message.data;
          isDone = true;
        },
      );

      // Start the workflow
      await hotMesh.pub('hook.timeout.test', { job_id });

      // Wait for the hook to register, then send signal before timeout
      await sleepFor(1000);
      await hotMesh.signal('hook.timeout.resume', {
        id: job_id,
        done: true,
        payload: 'signal-arrived',
      });

      // Wait for completion
      while (!isDone) {
        await sleepFor(100);
      }

      expect(resultData.signaled).toBe(true);
      expect(resultData.signal_data).toBe('signal-arrived');

      await hotMesh.unsub(`hook.timeout.tested.${job_id}`);
    }, 30_000);
  });

  describe('Timeout wins the race', () => {
    it('completes with timeout indicator when no signal arrives', async () => {
      let isDone = false;
      let resultData: Record<string, any> = {};
      const job_id = `timeout-wins-${guid()}`;

      // Subscribe to the output
      await hotMesh.sub(
        `hook.expire.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          resultData = message.data;
          isDone = true;
        },
      );

      // Start the workflow — no signal will be sent
      // The 5s timeout will fire (with ~3s fidelity bucketing)
      await hotMesh.pub('hook.expire.test', { job_id });

      // Wait for timeout to fire (5s + fidelity buffer)
      let elapsed = 0;
      while (!isDone && elapsed < 20_000) {
        await sleepFor(500);
        elapsed += 500;
      }

      expect(isDone).toBe(true);
      expect(resultData.signaled).toBe(false);

      await hotMesh.unsub(`hook.expire.tested.${job_id}`);
    }, 30_000);
  });

  describe('Race condition safety', () => {
    it('exactly one path completes when signal and timeout are close', async () => {
      let completionCount = 0;
      let resultData: Record<string, any> = {};
      const job_id = `race-${guid()}`;

      // Subscribe to the output — should fire exactly once
      await hotMesh.sub(
        `hook.timeout.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          completionCount++;
          resultData = message.data;
        },
      );

      // Start the workflow (10s timeout)
      await hotMesh.pub('hook.timeout.test', { job_id });

      // Send signal quickly
      await sleepFor(500);
      await hotMesh.signal('hook.timeout.resume', {
        id: job_id,
        done: true,
        payload: 'race-signal',
      });

      // Wait for completion and potential duplicate processing
      await sleepFor(15_000);

      // Should have completed exactly once
      expect(completionCount).toBe(1);
      expect(resultData.signaled).toBe(true);
      expect(resultData.signal_data).toBe('race-signal');

      await hotMesh.unsub(`hook.timeout.tested.${job_id}`);
    }, 30_000);
  });

  describe('Dynamic sleep (null = no timeout, webhook only)', () => {
    it('completes via signal when sleep resolves to null', async () => {
      let isDone = false;
      let resultData: Record<string, any> = {};
      const job_id = `dynamic-null-${guid()}`;

      await hotMesh.sub(
        `hook.dynamic.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          resultData = message.data;
          isDone = true;
        },
      );

      // Start workflow with no timeout (timeout field omitted → pipe resolves to null)
      await hotMesh.pub('hook.dynamic.test', { job_id });

      await sleepFor(1000);
      await hotMesh.signal('hook.dynamic.resume', {
        id: job_id,
        done: true,
        payload: 'no-timeout-signal',
      });

      while (!isDone) {
        await sleepFor(100);
      }

      expect(resultData.signaled).toBe(true);
      expect(resultData.signal_data).toBe('no-timeout-signal');

      await hotMesh.unsub(`hook.dynamic.tested.${job_id}`);
    }, 15_000);

    it('completes via timeout when sleep resolves to a number', async () => {
      let isDone = false;
      let resultData: Record<string, any> = {};
      const job_id = `dynamic-5s-${guid()}`;

      await hotMesh.sub(
        `hook.dynamic.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          resultData = message.data;
          isDone = true;
        },
      );

      // Start workflow WITH a timeout (5s)
      await hotMesh.pub('hook.dynamic.test', { job_id, timeout: 5 });

      let elapsed = 0;
      while (!isDone && elapsed < 20_000) {
        await sleepFor(500);
        elapsed += 500;
      }

      expect(isDone).toBe(true);
      // Timeout won — no signal data, so defaults apply
      expect(resultData.signaled).toBe(false);

      await hotMesh.unsub(`hook.dynamic.tested.${job_id}`);
    }, 30_000);

    it('signal wins when sleep resolves to a number and signal arrives first', async () => {
      let isDone = false;
      let resultData: Record<string, any> = {};
      const job_id = `dynamic-sig-${guid()}`;

      await hotMesh.sub(
        `hook.dynamic.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          resultData = message.data;
          isDone = true;
        },
      );

      // Start workflow with 10s timeout
      await hotMesh.pub('hook.dynamic.test', { job_id, timeout: 10 });

      // Signal arrives before timeout
      await sleepFor(1000);
      await hotMesh.signal('hook.dynamic.resume', {
        id: job_id,
        done: true,
        payload: 'beat-the-clock',
      });

      while (!isDone) {
        await sleepFor(100);
      }

      expect(resultData.signaled).toBe(true);
      expect(resultData.signal_data).toBe('beat-the-clock');

      await hotMesh.unsub(`hook.dynamic.tested.${job_id}`);
    }, 15_000);
  });
});
