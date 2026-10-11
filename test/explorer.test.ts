import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { MessageContext } from '../src/core';
import { SnugContext, SnugEvent, SnugHeader, SnugPayload, SnugSubscribe } from '../src/nest/decorators';
import { collectSubscriptions } from '../src/nest/explorer';

class Base {
  seen: unknown[] = [];
  @SnugSubscribe('base.topic')
  onBase(payload: unknown) {
    this.seen.push(payload);
  }
}

class Orders extends Base {
  @SnugSubscribe('orders.created', { concurrency: 3 })
  @SnugSubscribe('orders.updated')
  onOrder(payload: { id: number }, ctx: MessageContext) {
    return [payload.id, ctx.event];
  }

  @SnugSubscribe('orders.cancelled')
  decorated(@SnugContext() ctx: MessageContext, @SnugHeader('tenant') tenant: string, @SnugPayload() p: { id: number }, @SnugEvent() ev: string) {
    return [ctx.id, tenant, p.id, ev];
  }

  notHandler() {
    return 1;
  }
}

const ctx = (over: Partial<MessageContext> = {}): MessageContext => ({
  id: '1-0', topic: 't', group: 'g', consumer: 'c', event: 'ev', messageId: 'm', producer: 'p', publishedAt: 1,
  headers: { tenant: 'acme' }, attempt: 1, fields: {}, ack: async () => undefined, ...over,
});

test('finds decorated methods, including inherited and repeated ones', () => {
  const found = collectSubscriptions([new Orders(), {}, new Base()]);
  const topics = found.map((f) => f.meta.topic).sort();
  assert.deepEqual(topics, ['base.topic', 'base.topic', 'orders.cancelled', 'orders.created', 'orders.updated']);
  assert.equal(found.find((f) => f.meta.topic === 'orders.created')!.meta.concurrency, 3);
});

test('calls the method on its instance with (payload, ctx) by default', async () => {
  const o = new Orders();
  const f = collectSubscriptions([o]).find((x) => x.meta.topic === 'orders.created')!;
  assert.deepEqual(await f.handler({ id: 9 }, ctx()), [9, 'ev']);
  const b = collectSubscriptions([o]).find((x) => x.meta.topic === 'base.topic')!;
  await b.handler('hello', ctx());
  assert.deepEqual(o.seen, ['hello']);
});

test('parameter decorators pick the arguments', async () => {
  const f = collectSubscriptions([new Orders()]).find((x) => x.meta.topic === 'orders.cancelled')!;
  assert.deepEqual(await f.handler({ id: 4 }, ctx()), ['1-0', 'acme', 4, 'ev']);
});

test('each instance of the same class is found once', () => {
  const o = new Orders();
  assert.equal(collectSubscriptions([o, o]).length, 4);
});
