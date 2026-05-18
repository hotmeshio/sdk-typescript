/**
 * Deterministic signal race condition tests.
 *
 * These tests inject deliberate delays into the Hook activity's
 * registration path to force the two race directions that occur
 * naturally on AWS (where network latency widens the gap).
 *
 * Direction A — Leg2 (signal) arrives before Leg1 registers the hook.
 *   Leg2 stores $pending; Leg1 finds it and redelivers.
 *
 * Direction B — Signal arrives after Leg1 commits but before the hook
 *   signal is registered. With the fix (hook registration moved to
 *   post-commit), this converges to Direction A: Leg2 stores $pending,
 *   Leg1's registerWebHookSignal finds it and redelivers.
 *
 *   Before the fix, hook registration happened BEFORE commit, creating
 *   a FORBIDDEN window where Leg2 found the hook but Leg1 wasn't
 *   committed. Inline retry exhausted → signal permanently lost.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import { Client as Postgres } from 'pg';

import { HotMesh, HotMeshConfig } from '../../../index';
import { HMSH_LOGLEVEL } from '../../../modules/enums';
import { guid, sleepFor } from '../../../modules/utils';
import { JobOutput } from '../../../types/job';
import { ProviderNativeClient } from '../../../types/provider';
import { PostgresConnection } from '../../../services/connector/providers/postgres';
import { dropTables, postgres_options } from '../../$setup/postgres';

// Internal import — needed to monkey-patch the race window
import { Hook } from '../../../services/activities/hook';

describe('FUNCTIONAL | Signal Race | Postgres', () => {
  const appConfig = { id: 'hook' };
  let hotMesh: HotMesh;
  let postgresClient: ProviderNativeClient;

  // Store originals for restoration
  const originalRegisterWebHookSignal = Hook.prototype.registerWebHookSignal;

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
    await hotMesh.deploy('/app/tests/$setup/apps/hook/v1/hotmesh.yaml');
    await hotMesh.activate('1');
  }, 15_000);

  afterEach(() => {
    Hook.prototype.registerWebHookSignal = originalRegisterWebHookSignal;
  });

  afterAll(async () => {
    hotMesh.stop();
    await HotMesh.stop();
  });

  describe('Direction A — Leg2 arrives before hook registration ($pending path)', () => {
    it('recovers via $pending when signal arrives before Leg1 registers', async () => {
      // Delay BEFORE registerWebHookSignal — widens the window
      // where no hook exists, forcing Leg2 to store $pending.
      // This delay runs AFTER Leg1's transaction has committed
      // (state + collation ledger visible), but BEFORE the hook
      // signal is written.
      const DELAY_MS = 2000;
      Hook.prototype.registerWebHookSignal = async function () {
        await sleepFor(DELAY_MS);
        return originalRegisterWebHookSignal.call(this);
      };

      let isDone = false;
      let resultData: Record<string, any> = {};
      const parent_job_id = guid();
      const job_id = guid();

      await hotMesh.sub(
        `hook.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          if (message.data.done) {
            resultData = message.data;
            isDone = true;
          }
        },
      );

      // Start workflow — Leg1 commits, then pauses before hook registration
      await hotMesh.pub('hook.test', { parent_job_id, job_id });

      // Send signal during the delay — Leg2 stores $pending
      await sleepFor(200);
      await hotMesh.signal('hook.resume', { id: job_id, done: true });

      // Wait for completion — $pending mechanism should recover
      let elapsed = 0;
      while (!isDone && elapsed < 15_000) {
        await sleepFor(200);
        elapsed += 200;
      }

      expect(isDone).toBe(true);
      expect(resultData.done).toBe(true);

      await hotMesh.unsub(`hook.tested.${job_id}`);
    }, 30_000);
  });

  describe('Direction B — signal during post-commit hook registration window', () => {
    it('recovers via $pending when signal arrives after commit but before hook registration', async () => {
      // Same delay as Direction A — the fix collapses Direction B
      // into the $pending recovery path. Before the fix, this window
      // produced FORBIDDEN errors because hook registration happened
      // BEFORE commit. Now, Leg1 commits first → Leg2 finds no hook
      // → stores $pending → registerWebHookSignal finds $pending →
      // redelivers → Leg2 succeeds.
      const DELAY_MS = 3000;
      Hook.prototype.registerWebHookSignal = async function () {
        await sleepFor(DELAY_MS);
        return originalRegisterWebHookSignal.call(this);
      };

      let isDone = false;
      let resultData: Record<string, any> = {};
      const parent_job_id = guid();
      const job_id = guid();

      await hotMesh.sub(
        `hook.tested.${job_id}`,
        (_topic: string, message: JobOutput) => {
          if (message.data.done) {
            resultData = message.data;
            isDone = true;
          }
        },
      );

      // Start workflow — Leg1 commits, then pauses before hook registration
      await hotMesh.pub('hook.test', { parent_job_id, job_id });

      // Send signal after Leg1 commits but before hook is registered.
      // Before the fix: Leg2 found hook signal (set pre-commit) →
      //   FORBIDDEN → inline retry exhausted → signal lost.
      // After the fix: Leg2 finds no hook → stores $pending →
      //   registerWebHookSignal finds $pending → redelivers → success.
      await sleepFor(1000);
      await hotMesh.signal('hook.resume', { id: job_id, done: true });

      let elapsed = 0;
      while (!isDone && elapsed < 15_000) {
        await sleepFor(200);
        elapsed += 200;
      }

      expect(isDone).toBe(true);
      expect(resultData.done).toBe(true);

      await hotMesh.unsub(`hook.tested.${job_id}`);
    }, 30_000);
  });
});
