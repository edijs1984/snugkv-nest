import assert from 'node:assert/strict';
import { afterEach, describe, test } from 'node:test';
import { PRESENCE_PREFIX, PresenceRecord, SnugKvBus, SnugKvClient, MessageContext } from '../src/core';
import { haveServer, quiet, sleep, startServer, TestServer, until } from './helpers';

const opts = { skip: !haveServer && 'set SNUGKV_BIN to a snugkv binary' };

describe('bus against a real server', opts, () => {
  let server: TestServer;
  const buses: SnugKvBus[] = [];

  const mkBus = (service: string, extra: Partial<ConstructorParameters<typeof SnugKvBus>[0]> = {}) => {
    const b = new SnugKvBus({ service, port: server.port, logger: quiet, presence: { ttlSeconds: 3, intervalMs: 200 }, ...extra });
    buses.push(b);
    return b;
  };

  const start = async () => {
    server = await startServer();
  };

  afterEach(async () => {
    await Promise.allSettled(buses.splice(0).map((b) => b.close()));
    await server?.stop();
  });

  test('delivers a published message with its envelope', async () => {
    await start();
    const bus = mkBus('billing');
    const got: Array<{ payload: unknown; ctx: MessageContext }> = [];
    await bus.subscribe({ topic: 'orders.created' }, (payload, ctx) => void got.push({ payload, ctx }));
    const id = await bus.publish('orders.created', { orderId: 7, items: ['a', 'ü'] }, { event: 'order.created', headers: { trace: 't1' } });
    await until(() => got.length === 1, 5000, 'delivery');
    assert.deepEqual(got[0]!.payload, { orderId: 7, items: ['a', 'ü'] });
    const ctx = got[0]!.ctx;
    assert.equal(ctx.id, id);
    assert.equal(ctx.event, 'order.created');
    assert.equal(ctx.headers.trace, 't1');
    assert.equal(ctx.group, 'billing');
    assert.equal(ctx.attempt, 1);
    assert.ok(ctx.messageId.length > 10);
    assert.ok(ctx.publishedAt && Math.abs(Date.now() - ctx.publishedAt) < 5000);
    assert.ok(ctx.producer.startsWith('billing/'));
  });

  test('instances of one service share messages; different services each get all', async () => {
    await start();
    const a1 = mkBus('billing', { instance: 'a1' });
    const a2 = mkBus('billing', { instance: 'a2' });
    const m1 = mkBus('mailer', { instance: 'm1' });
    const seen = { a1: [] as number[], a2: [] as number[], m1: [] as number[] };
    await a1.subscribe({ topic: 't' }, (p: number) => void seen.a1.push(p));
    await a2.subscribe({ topic: 't' }, (p: number) => void seen.a2.push(p));
    await m1.subscribe({ topic: 't' }, (p: number) => void seen.m1.push(p));
    const pub = mkBus('web');
    await pub.publishBatch('t', Array.from({ length: 40 }, (_, i) => ({ payload: i })));
    await until(() => seen.m1.length === 40 && seen.a1.length + seen.a2.length === 40, 8000, 'all delivered');
    assert.deepEqual([...seen.m1].sort((x, y) => x - y), Array.from({ length: 40 }, (_, i) => i));
    assert.deepEqual([...seen.a1, ...seen.a2].sort((x, y) => x - y), Array.from({ length: 40 }, (_, i) => i));
  });

  test('keeps order with concurrency 1', async () => {
    await start();
    const bus = mkBus('s');
    const got: number[] = [];
    await bus.subscribe({ topic: 'o' }, async (p: number) => {
      await sleep(p % 3);
      got.push(p);
    });
    await bus.publishBatch('o', Array.from({ length: 60 }, (_, i) => ({ payload: i })));
    await until(() => got.length === 60, 8000, 'all');
    assert.deepEqual(got, Array.from({ length: 60 }, (_, i) => i));
  });

  test('runs handlers in parallel up to the concurrency limit', async () => {
    await start();
    const bus = mkBus('s');
    let running = 0;
    let peak = 0;
    let done = 0;
    await bus.subscribe({ topic: 'p', concurrency: 5 }, async () => {
      running++;
      peak = Math.max(peak, running);
      await sleep(80);
      running--;
      done++;
    });
    await bus.publishBatch('p', Array.from({ length: 20 }, (_, i) => ({ payload: i })));
    await until(() => done === 20, 8000, 'all');
    assert.equal(peak, 5);
  });

  test('retries a failing handler, then succeeds', async () => {
    await start();
    const bus = mkBus('s');
    const attempts: number[] = [];
    await bus.subscribe({ topic: 'r', retryDelayMs: 10, maxAttempts: 4 }, (_p, ctx) => {
      attempts.push(ctx.attempt);
      if (ctx.attempt < 3) throw new Error('flaky');
    });
    await bus.publish('r', 1);
    await until(() => attempts.length === 3, 5000, 'retries');
    await sleep(100);
    assert.deepEqual(attempts, [1, 2, 3]);
    const pending = await bus.client.call('XPENDING', 'r', 's');
    assert.equal((pending as unknown[])[0], 0);
  });

  test('dead-letters a message that keeps failing', async () => {
    await start();
    const bus = mkBus('s');
    let calls = 0;
    await bus.subscribe({ topic: 'd', retryDelayMs: 5, maxAttempts: 2 }, () => {
      calls++;
      throw new Error('always bad');
    });
    await bus.publish('d', { x: 1 }, { event: 'e' });
    await until(async () => (await bus.client.xlen('d.dlq')) === 1, 5000, 'dlq entry');
    assert.equal(calls, 2);
    const [dead] = await bus.client.xrange('d.dlq');
    assert.equal(dead!.fields.dlq_error, 'always bad');
    assert.equal(dead!.fields.dlq_source, 'd');
    assert.equal(dead!.fields.dlq_group, 's');
    assert.equal(dead!.fields.dlq_attempts, '2');
    assert.equal(dead!.fields.event, 'e');
    assert.equal(dead!.fields.payload, '{"x":1}');
    const pending = (await bus.client.call('XPENDING', 'd', 's')) as unknown[];
    assert.equal(pending[0], 0);
  });

  test('dead-letters undecodable payloads without retrying', async () => {
    await start();
    const bus = mkBus('s');
    let calls = 0;
    await bus.subscribe({ topic: 'bad' }, () => void calls++);
    await bus.client.xadd('bad', { payload: '{not json' });
    await until(async () => (await bus.client.xlen('bad.dlq')) === 1, 5000, 'dlq');
    assert.equal(calls, 0);
  });

  test('foreign entries without a payload field arrive as an object of fields', async () => {
    await start();
    const bus = mkBus('s');
    const got: unknown[] = [];
    await bus.subscribe({ topic: 'f' }, (p) => void got.push(p));
    await bus.client.xadd('f', { user: 'amy', n: '3' });
    await until(() => got.length === 1, 5000, 'delivery');
    assert.deepEqual(got[0], { user: 'amy', n: '3' });
  });

  test('manual ack leaves the message pending until ack() is called', async () => {
    await start();
    const bus = mkBus('s');
    const ctxs: MessageContext[] = [];
    await bus.subscribe({ topic: 'm', autoAck: false }, (_p, ctx) => void ctxs.push(ctx));
    await bus.publish('m', 1);
    await until(() => ctxs.length === 1, 5000, 'delivery');
    const pend = async () => ((await bus.client.call('XPENDING', 'm', 's')) as unknown[])[0];
    assert.equal(await pend(), 1);
    await ctxs[0]!.ack();
    await ctxs[0]!.ack();
    assert.equal(await pend(), 0);
  });

  test('a crashed consumer\'s messages are taken over by another instance', async () => {
    await start();
    const crashing = mkBus('s', { instance: 'dead' });
    const started: string[] = [];
    await crashing.subscribe({ topic: 'c', shutdownTimeoutMs: 50 }, async (p: string) => {
      started.push(p);
      await new Promise(() => undefined); // never finishes
    });
    await crashing.publish('c', 'poison');
    await until(() => started.length === 1, 5000, 'first delivery');
    // Simulate a crash: drop connections without a clean stop.
    crashing.client.close();

    const rescuer = mkBus('s', { instance: 'alive' });
    const got: Array<{ p: string; attempt: number }> = [];
    await rescuer.subscribe({ topic: 'c', claimMinIdleMs: 200, claimIntervalMs: 100 }, (p: string, ctx) => void got.push({ p, attempt: ctx.attempt }));
    await until(() => got.length === 1, 8000, 'takeover');
    assert.equal(got[0]!.p, 'poison');
    assert.equal(got[0]!.attempt, 2);
  });

  test('a message that keeps crashing consumers ends in the dead-letter topic', async () => {
    await start();
    const setup = mkBus('s');
    await setup.client.xgroupCreate('k', 's', '0');
    await setup.publish('k', 'p');
    // Pretend three earlier consumers died holding it.
    for (const c of ['x1', 'x2', 'x3']) {
      await setup.client.xreadgroup({ group: 's', consumer: c, streams: [{ key: 'k', id: '>' }], count: 1 }).catch(() => undefined);
      await setup.client.call('XCLAIM', 'k', 's', c === 'x1' ? 'x2' : 'x3', 0, (await setup.client.xrange('k'))[0]!.id);
    }
    let calls = 0;
    const b = mkBus('s');
    await b.subscribe({ topic: 'k', maxAttempts: 3, claimMinIdleMs: 50, claimIntervalMs: 50 }, () => void calls++);
    await until(async () => (await b.client.xlen('k.dlq')) === 1, 8000, 'dlq');
    assert.equal(calls, 0);
  });

  test('presence records appear, count, and vanish on close', async () => {
    await start();
    const bus = mkBus('billing', { instance: 'i1' });
    await bus.subscribe({ topic: 'pr' }, () => undefined);
    await bus.publish('pr', 1);
    await bus.publish('pr', 2);
    const read = async () => {
      const keys = ((await bus.client.call('KEYS', `${PRESENCE_PREFIX}*`)) as string[]).sort();
      const vals = await Promise.all(keys.map((k) => bus.client.get(k)));
      return vals.filter((v): v is string => v !== null).map((v) => JSON.parse(v) as PresenceRecord);
    };
    await until(async () => {
      const r = await read();
      return r.some((x) => x.role === 'publisher' && x.published === 2) && r.some((x) => x.role === 'consumer' && x.processed === 2);
    }, 6000, 'presence counters');
    const recs = await read();
    const consumer = recs.find((r) => r.role === 'consumer')!;
    assert.equal(consumer.group, 'billing');
    assert.equal(consumer.topic, 'pr');
    assert.equal(consumer.consumer, 'billing-i1');
    const observer = new SnugKvClient({ port: server.port });
    await bus.close();
    assert.deepEqual(await observer.call('KEYS', `${PRESENCE_PREFIX}*`), []);
    observer.close();
  });

  test('stop() lets a running handler finish and removes the idle consumer', async () => {
    await start();
    const bus = mkBus('s', { instance: 'one' });
    let finished = false;
    let started = false;
    const sub = await bus.subscribe({ topic: 'g' }, async () => {
      started = true;
      await sleep(300);
      finished = true;
    });
    await bus.publish('g', 1);
    await until(() => started, 5000, 'start');
    await sub.stop();
    assert.equal(finished, true);
    const consumers = (await bus.client.call('XINFO', 'CONSUMERS', 'g', 's')) as unknown[];
    assert.equal(consumers.length, 0);
    const pending = (await bus.client.call('XPENDING', 'g', 's')) as unknown[];
    assert.equal(pending[0], 0);
  });

  test('a new group can start from the beginning of the stream', async () => {
    await start();
    const bus = mkBus('s');
    await bus.publishBatch('h', [{ payload: 1 }, { payload: 2 }]);
    const got: number[] = [];
    await bus.subscribe({ topic: 'h', startFrom: '0' }, (p: number) => void got.push(p));
    await until(() => got.length === 2, 5000, 'history');
    assert.deepEqual(got, [1, 2]);
  });

  test('keyPrefix is applied to streams and dead letters', async () => {
    await start();
    const bus = mkBus('s', { keyPrefix: 'app:' });
    await bus.subscribe({ topic: 'x', maxAttempts: 1 }, () => {
      throw new Error('no');
    });
    await bus.publish('x', 1);
    await until(async () => (await bus.client.xlen('app:x.dlq')) === 1, 5000, 'dlq');
    assert.equal(await bus.client.xlen('app:x'), 1);
  });

  test('maxLen trims the stream', async () => {
    await start();
    const bus = mkBus('s', { maxLen: 10 });
    await bus.publishBatch('t', Array.from({ length: 200 }, (_, i) => ({ payload: i })));
    const len = await bus.client.xlen('t');
    assert.ok(len >= 10 && len < 200, `len ${len}`);
  });

  test('keeps working across a server restart', async () => {
    server = await startServer();
    const port = server.port;
    const dir = server.dir;
    const bus = mkBus('s', { instance: 'i' });
    const got: number[] = [];
    await bus.subscribe({ topic: 'z', blockMs: 500 }, (p: number) => void got.push(p));
    await bus.publish('z', 1);
    await until(() => got.length === 1, 5000, 'first');
    await server.kill();
    server = await startServer({ dir, port });
    // The publish waits for the reconnect.
    await bus.publish('z', 2);
    await until(() => got.length === 2, 15000, 'after restart');
    assert.deepEqual(got, [1, 2]);
  });

  test('connect() fails when nothing listens', async () => {
    const bus = new SnugKvBus({ service: 's', port: 1, connectTimeoutMs: 300, logger: quiet });
    await assert.rejects(bus.connect(), /could not connect/);
    await bus.close();
  });
});
