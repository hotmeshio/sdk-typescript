/**
 * Facet-selected forms: accumulateItemByMetadata() and
 * removeAccumulatedItemByMetadata() address the highest priority pending
 * row whose metadata contains the key/value, mirroring resolveByMetadata().
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { Durable } from '../../../services/durable';
import { bootHarness, findPending, guid, sleepFor } from './src/harness';
import type { BinResult } from './src/workflows';

describe('DURABLE | escalations-accumulate | by-metadata | Postgres', () => {
  let client: Awaited<ReturnType<typeof bootHarness>>['client'];
  const taskQueue = 'escalation-accumulate-by-metadata-test';

  beforeAll(async () => {
    ({ client } = await bootHarness(taskQueue, ['binWorkflow', 'memberWorkflow']));
  }, 30_000);

  afterAll(async () => {
    await sleepFor(1500);
    await Durable.shutdown();
  }, 10_000);

  it('adds, removes, and completes through the facet selector with a facet-selected reciprocal', async () => {
    const binKey = guid();
    const handle = await client.workflow.start({
      args: [binKey, { max: 2 }], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const bin = await findPending(client, 'bin', { binKey });
    const orderId = guid();
    await client.workflow.start({ args: [orderId], taskQueue, workflowName: 'memberWorkflow', workflowId: guid(), expire: 180 });
    const member = await findPending(client, 'member', { orderId });

    const first = await client.escalations.accumulateItemByMetadata({
      key: 'binKey', value: binKey, roles: ['bin'], itemKey: 'bag-1', payload: { weight: 1 },
    });
    expect(first.ok && first.outcome === 'accepted' && first.entry.id === bin.id).toBe(true);

    const removed = await client.escalations.removeAccumulatedItemByMetadata({
      key: 'binKey', value: binKey, itemKey: 'bag-1',
    });
    expect(removed.ok && removed.count === 0).toBe(true);

    const wrongRole = await client.escalations.accumulateItemByMetadata({
      key: 'binKey', value: binKey, roles: ['other'], itemKey: 'bag-1',
    });
    expect(wrongRole.ok).toBe(false);
    if (!wrongRole.ok) expect(wrongRole.outcome).toBe('not-found');

    await client.escalations.accumulateItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    const last = await client.escalations.accumulateItemByMetadata({
      key: 'binKey', value: binKey, itemKey: orderId,
      reciprocal: { key: 'orderId', value: orderId, roles: ['member'] },
    });
    expect(last.ok && last.outcome === 'completed' && last.reciprocal?.outcome === 'completed').toBe(true);
    if (last.ok) expect(last.reciprocal?.entry.id).toBe(member.id);

    const output = await handle.result<{ payload: BinResult }>();
    expect(output.payload.$trigger).toBe('count');
    expect(output.payload.$accumulated.map((i) => i.itemKey)).toEqual(['bag-1', orderId]);
  }, 40_000);
});
