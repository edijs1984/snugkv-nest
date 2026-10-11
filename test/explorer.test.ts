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

import { OnSnugWorkerEvent, SnugProcess, SnugProcessor, WorkerHost } from '../src/nest/queue-decorators';
import { collectProcessors } from '../src/nest/explorer';
import { UnrecoverableError } from '../src/core/queue/types';

const fakeJob = (name: string) => ({ name, id: '1' }) as never;

@SnugProcessor('emails', { concurrency: 4 })
class EmailProcessor extends WorkerHost {
  events: unknown[] = [];
  process(job: { name: string }) {
    return `whole:${job.name}`;
  }
  @OnSnugWorkerEvent('completed')
  done(job: unknown, result: unknown) {
    this.events.push([job, result]);
  }
}

@SnugProcessor('reports')
class ReportProcessor {
  @SnugProcess('daily')
  daily() {
    return 'daily';
  }
  @SnugProcess()
  rest(job: { name: string }) {
    return `rest:${job.name}`;
  }
}

@SnugProcessor('strict')
class StrictProcessor {
  @SnugProcess('only')
  only() {
    return 1;
  }
}

class NotAProcessor {}

@SnugProcessor('broken')
class Broken {}

test('finds processors, routes jobs by name and wires worker events', async () => {
  const email = new EmailProcessor();
  const found = collectProcessors([email, new ReportProcessor(), new StrictProcessor(), new NotAProcessor(), email]);
  assert.deepEqual(found.map((f) => f.meta.queue), ['emails', 'reports', 'strict']);
  assert.equal(found[0]!.meta.options.concurrency, 4);
  assert.equal(await found[0]!.processor(fakeJob('x'), 't'), 'whole:x');
  assert.equal(await found[1]!.processor(fakeJob('daily'), 't'), 'daily');
  assert.equal(await found[1]!.processor(fakeJob('other'), 't'), 'rest:other');
  assert.equal(await found[2]!.processor(fakeJob('only'), 't'), 1);
  assert.throws(() => found[2]!.processor(fakeJob('nope'), 't'), UnrecoverableError);
  found[0]!.events[0]!.handler('job', 'result');
  assert.deepEqual(email.events, [['job', 'result']]);
  assert.equal(found[0]!.events[0]!.event, 'completed');
  assert.throws(() => collectProcessors([new Broken()]), /no process\(\) method/);
});
