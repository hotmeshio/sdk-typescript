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

  it('a facet picks the pending accumulator, never a higher-priority non-accumulator sharing it', async () => {
    const binKey = guid();
    await client.workflow.start({
      args: [binKey, { max: 3 }], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const bin = await findPending(client, 'bin', { binKey });
    // the release row of the previous generation: same facet, same role, higher priority
    const release = await client.escalations.create({ role: 'bin', type: 'release', priority: 1, metadata: { binKey } });
    // a closed older generation that was cancelled: an accumulator, but not pending
    await client.workflow.start({
      args: [`${binKey}-old`, { max: 3 }], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const old = await findPending(client, 'bin', { binKey: `${binKey}-old` });
    await client.escalations.cancel(old.id);

    const added = await client.escalations.accumulateItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    expect(added.ok && added.outcome === 'accepted' && added.entry.id === bin.id).toBe(true);
    const untouched = await client.escalations.get(release.id);
    expect(untouched!.status).toBe('pending');
    expect((untouched!.envelope as any)?.accumulate_items).toBeUndefined();

    const removed = await client.escalations.removeAccumulatedItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    expect(removed.ok && removed.entry.id === bin.id).toBe(true);
    await client.escalations.cancel(bin.id);
    await client.escalations.cancel(release.id);
  }, 40_000);

  it('only non-accumulator rows sharing the facet answer not-found, and the by-id form still names not-accumulator', async () => {
    const binKey = guid();
    const release = await client.escalations.create({ role: 'bin', type: 'release', priority: 1, metadata: { binKey } });
    const added = await client.escalations.accumulateItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    expect(added.ok).toBe(false);
    if (!added.ok) expect(added.outcome).toBe('not-found');
    const removed = await client.escalations.removeAccumulatedItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    expect(removed.ok).toBe(false);
    if (!removed.ok) expect(removed.outcome).toBe('not-found');

    const byId = await client.escalations.accumulateItem({ id: release.id, itemKey: 'bag-1' });
    expect(byId.ok).toBe(false);
    if (!byId.ok) expect(byId.outcome).toBe('not-accumulator');
    await client.escalations.cancel(release.id);
  }, 30_000);

  it('a cancelled accumulator generation beside the open one is never the pick', async () => {
    const binKey = guid();
    // older generation, opened first so it sorts first on created_at, then cancelled
    await client.workflow.start({
      args: [binKey, { max: 3 }], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const older = await findPending(client, 'bin', { binKey });
    await client.escalations.cancel(older.id);
    await client.workflow.start({
      args: [binKey, { max: 3 }], taskQueue, workflowName: 'binWorkflow', workflowId: guid(), expire: 180,
    });
    const open = await findPending(client, 'bin', { binKey });
    expect(open.id).not.toBe(older.id);
    const added = await client.escalations.accumulateItemByMetadata({ key: 'binKey', value: binKey, itemKey: 'bag-1' });
    expect(added.ok && added.entry.id === open.id).toBe(true);
    await client.escalations.cancel(open.id);
  }, 40_000);
});

