import { describe, it, expect } from 'vitest';

import {
  getWorkflowYAML,
  APP_VERSION,
  APP_ID,
} from '../../../../services/durable/schemas/factory';

/**
 * Guards the durable app schema version contract.
 *
 * The deployed schema only upgrades when APP_VERSION increases (see
 * WorkerService.activateWorkflow / ClientService.deployAndActivate, which
 * compare `Number(deployedVersion) < Number(APP_VERSION)`). Two invariants must
 * hold or upgrades break for every already-deployed database:
 *
 *  1. APP_VERSION is a numeric string — the deploy/activate logic orders
 *     versions with `Number()`. A non-numeric version makes drift detection
 *     fall back to "redeploy always" at best, or mis-order at worst.
 *  2. The generated YAML carries the escalation hook wired to condition()'s
 *     queueConfig. If this regresses, condition(signalId, config) silently stops
 *     writing hmsh_escalations rows — exactly the failure that motivated v16.
 */
describe('UNIT | durable schema factory | version + escalation hook', () => {
  it('APP_VERSION is a numeric string (ordering invariant for drift detection)', () => {
    expect(APP_VERSION).toMatch(/^\d+$/);
    expect(Number.isNaN(Number(APP_VERSION))).toBe(false);
    expect(Number(APP_VERSION)).toBeGreaterThanOrEqual(16);
  });

  it('embeds the requested version in the generated YAML', () => {
    const yaml = getWorkflowYAML(APP_ID, APP_VERSION);
    expect(yaml).toContain(`version: '${APP_VERSION}'`);
    expect(yaml).toContain(`id: ${APP_ID}`);
  });

  it('wires condition() queueConfig into a wait-hook escalation block', () => {
    const yaml = getWorkflowYAML(APP_ID, APP_VERSION);
    // The wait hook maps the worker-emitted queueConfig onto an escalation:
    // block so the row is written in the hook's Leg1 transaction.
    expect(yaml).toContain('escalation:');
    expect(yaml).toContain('queueConfig.role');
    expect(yaml).toContain('queueConfig.type');
    expect(yaml).toContain('queueConfig.metadata');
    expect((yaml.match(/queueConfig/g) || []).length).toBeGreaterThan(1);
  });
});
