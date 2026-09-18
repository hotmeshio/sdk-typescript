/**
 * Manual paths on an accumulator row: resolve() and resolveByMetadata()
 * deliver the collection merged with the resolver payload under
 * `$trigger: 'resolve'`; cancel() keeps its null contract with the items
 * preserved for audit.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { Durable } from '../../../services/durable';
import { bootHarness, findPending, guid, sleepFor } from './src/harness';
import type { BinResult } from './src/workflows';

describe('DURABLE | escalations-accumulate | resolve + cancel | Postgres', () => {
  let client: Awaited<ReturnType<typeof bootHarness>>['client'];
  const taskQueue = 'escalation-accumulate-resolve-test';

  const startBin = (binKey: string, options: Record<string, unknown> = {}) =>
    client.workflow.start({ args: [binKey, options], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180 });

  beforeAll(async () => {
    ({ client } = await bootHarness(taskQueue, ['binWorkflow']));
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('resolve() merges the payload with the collection and stores the same', async () => {
    const binKey = guid();
    const handle = await startBin(binKey);
    const row = await findPending(client, 'bin', { binKey });
    await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1', payload: { weight: 1 } });
    await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-2', payload: { weight: 2 } });

    const resolved = await client.escalations.resolve({
      id: row.id,
      resolverPayload: { shippedBy: 'carrier-x' },
      metadata: { shipped: true },
      resolvedBy: { id: 'lead-1' },
    });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok) return;
    expect((resolved.entry.resolver_payload as any).$trigger).toBe('resolve');
    expect((resolved.entry.resolver_payload as any).shippedBy).toBe('carrier-x');
    expect((resolved.entry.resolver_payload as any).$accumulated).toHaveLength(2);
    expect((resolved.entry.metadata as any).shipped).toBe(true);

    const output = await handle.result<{ outcome: string; payload: BinResult }>();
    expect(output.outcome).toBe('delivered');
    expect(output.payload.$trigger).toBe('resolve');
    expect(output.payload.shippedBy).toBe('carrier-x');
    expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['bag-1', 'bag-2']);
    expect(output.payload.$resolution).toEqual({ escalationId: row.id, resolvedBy: 'lead-1' });
  }, 30_000);

  it('resolveByMetadata() delivers the collection with an empty payload too', async () => {
    const binKey = guid();
    const handle = await startBin(binKey);
    const row = await findPending(client, 'bin', { binKey });
    await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1' });
    const resolved = await client.escalations.resolveByMetadata({ key: 'binKey', value: binKey, roles: ['bin'] });
    expect(resolved.ok).toBe(true);
    const output = await handle.result<{ outcome: string; payload: BinResult }>();
    expect(output.payload.$trigger).toBe('resolve');
    expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['bag-1']);
  }, 30_000);

  it('cancel() resumes with null and preserves the held items', async () => {
    const binKey = guid();
    const handle = await startBin(binKey, { max: 4 });
    const row = await findPending(client, 'bin', { binKey });
    await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-1', payload: { weight: 1 } });
    const cancelled = await client.escalations.cancel(row.id);
    expect(cancelled.ok).toBe(true);
    const output = await handle.result<{ outcome: string; payload: null }>();
    expect(output.outcome).toBe('cancelled');
    const terminal = await client.escalations.get(row.id);
    expect(terminal!.status).toBe('cancelled');
    expect((terminal!.envelope as any).accumulate_items['bag-1'].payload).toEqual({ weight: 1 });
    const late = await client.escalations.accumulateItem({ id: row.id, itemKey: 'bag-2' });
    expect(late.ok).toBe(false);
    if (!late.ok) expect(late.outcome).toBe('already-cancelled');
  }, 30_000);

  it('a plain row still resolves with the bare payload', async () => {
    const plain = await client.escalations.create({ role: 'plain', metadata: { k: guid() } });
    const resolved = await client.escalations.resolve({ id: plain.id, resolverPayload: { approved: true } });
    expect(resolved.ok).toBe(true);
    if (resolved.ok) expect(resolved.entry.resolver_payload).toEqual({ approved: true });
  });
});
