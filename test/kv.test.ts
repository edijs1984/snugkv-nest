import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, test } from 'node:test';
import { SnugKvCache, SnugKvClient, SnugKvLocks, SnugKvRateLimiter } from '../src/core';
import { haveServer, sleep, startServer, TestServer, until } from './helpers';

const opts = { skip: !haveServer && 'set SNUGKV_BIN to a snugkv binary' };

describe('general client against a real server', opts, () => {
  let server: TestServer;
  let c: SnugKvClient;

  beforeEach(async () => {
    server = await startServer();
    c = new SnugKvClient({ port: server.port });
    await c.connect(5000);
  });

  afterEach(async () => {
    c.close();
    await server.stop();
  });

  test('strings, counters, json and conditional set', async () => {
    assert.equal(await c.setOpts('k', 'v', { nx: true }), true);
    assert.equal(await c.setOpts('k', 'w', { nx: true }), false);
    assert.equal(await c.get('k'), 'v');
    await c.mset({ a: 1, b: 'two' });
    assert.deepEqual(await c.mget('a', 'b', 'nope'), ['1', 'two', null]);
    assert.equal(await c.incr('n'), 1);
    assert.equal(await c.incr('n', 9), 10);
    assert.equal(await c.decr('n', 3), 7);
    assert.equal(await c.incrByFloat('f', 1.5), 1.5);
    assert.equal(await c.append('s', 'ab'), 2);
    await c.setJson('j', { x: [1, 2], y: 'ü' });
    assert.deepEqual(await c.getJson('j'), { x: [1, 2], y: 'ü' });
    assert.equal(await c.getJson('missing'), null);
    assert.equal(await c.getDel('k'), 'v');
    assert.equal(await c.get('k'), null);
  });

  test('expiry and keys', async () => {
    await c.set('e', '1');
    assert.equal(await c.ttl('e'), -1);
    assert.equal(await c.expire('e', 100), true);
    const ttl = await c.ttl('e');
    assert.ok(ttl > 90 && ttl <= 100);
    assert.equal(await c.persist('e'), true);
    assert.equal(await c.ttl('e'), -1);
    assert.equal(await c.ttl('gone'), -2);
    await c.setOpts('short', 'x', { pxMs: 150 });
    await until(async () => (await c.exists('short')) === 0, 3000, 'expiry').catch(async () => {
      await sleep(300);
      assert.equal(await c.exists('short'), 0);
    });
    assert.equal(await c.type('e'), 'string');
    await c.rename('e', 'e2');
    assert.equal(await c.exists('e', 'e2'), 1);
  });

  test('scan iterates all matching keys', async () => {
    for (let i = 0; i < 250; i++) await c.set(`u:${i}`, '1');
    await c.set('other', '1');
    const seen = new Set<string>();
    for await (const k of c.scanIterator({ match: 'u:*', count: 50 })) seen.add(k);
    assert.equal(seen.size, 250);
  });

  test('hashes', async () => {
    assert.equal(await c.hset('h', { name: 'a', n: 1 }), 2);
    assert.equal(await c.hget('h', 'name'), 'a');
    assert.deepEqual(await c.hgetall('h'), { name: 'a', n: '1' });
    assert.deepEqual(await c.hmget('h', 'n', 'zz'), ['1', null]);
    assert.equal(await c.hincrBy('h', 'n', 5), 6);
    assert.equal(await c.hincrByFloat('h', 'f', 0.5), 0.5);
    assert.equal(await c.hexists('h', 'name'), true);
    assert.equal(await c.hlen('h'), 3);
    assert.deepEqual((await c.hkeys('h')).sort(), ['f', 'n', 'name']);
    assert.equal(await c.hdel('h', 'name', 'f'), 2);
    assert.deepEqual(await c.hgetall('h'), { n: '6' });
    assert.deepEqual(await c.hgetall('none'), {});
  });

  test('lists', async () => {
    assert.equal(await c.rpush('l', 'a', 'b', 'c'), 3);
    assert.equal(await c.lpush('l', 'z'), 4);
    assert.deepEqual(await c.lrange('l'), ['z', 'a', 'b', 'c']);
    assert.equal(await c.lpop('l'), 'z');
    assert.equal(await c.rpop('l'), 'c');
    assert.equal(await c.llen('l'), 2);
    await c.ltrim('l', 0, 0);
    assert.deepEqual(await c.lrange('l'), ['a']);
    assert.equal(await c.lpop('empty'), null);
  });

  test('sets', async () => {
    assert.equal(await c.sadd('s', 'a', 'b', 'c'), 3);
    assert.equal(await c.sadd('s', 'a'), 0);
    assert.equal(await c.sismember('s', 'b'), true);
    assert.equal(await c.scard('s'), 3);
    assert.equal(await c.srem('s', 'a'), 1);
    assert.deepEqual((await c.smembers('s')).sort(), ['b', 'c']);
  });

  test('sorted sets', async () => {
    assert.equal(await c.zadd('z', [{ member: 'a', score: 3 }, { member: 'b', score: 1 }, { member: 'c', score: 2 }]), 3);
    assert.deepEqual(await c.zrange('z'), [
      { member: 'b', score: 1 },
      { member: 'c', score: 2 },
      { member: 'a', score: 3 },
    ]);
    assert.deepEqual((await c.zrange('z', 0, 0, { reverse: true }))[0], { member: 'a', score: 3 });
    assert.equal(await c.zscore('z', 'c'), 2);
    assert.equal(await c.zscore('z', 'nope'), null);
    assert.equal(await c.zrank('z', 'a'), 2);
    assert.equal(await c.zincrBy('z', 'b', 10), 11);
    assert.deepEqual(await c.zrangeByScore('z', 2, 3), ['c', 'a']);
    assert.deepEqual(await c.zrangeByScore('z', '-inf', '+inf', { count: 1 }), ['c']);
    assert.deepEqual(await c.zpopMin('z'), { member: 'c', score: 2 });
    assert.equal(await c.zrem('z', 'a'), 1);
    assert.equal(await c.zcard('z'), 1);
  });

  test('transactions run together and return every reply', async () => {
    const res = await c.tx().set('t', '1').incr('n').incr('n', 4).hset('h', { a: 1 }).exec();
    assert.deepEqual(res, ['OK', 1, 5, 1]);
    assert.equal(await c.get('t'), '1');
    assert.deepEqual(await c.tx().exec(), []);
  });

  test('lua scripts', async () => {
    const r = await c.eval("redis.call('set', KEYS[1], ARGV[1]); return redis.call('get', KEYS[1])", ['k'], ['hello']);
    assert.equal(r, 'hello');
  });

  test('pub/sub delivers to subscribers and patterns, and unsubscribes', async () => {
    const sub = c.subscriber();
    const got: string[] = [];
    const pat: string[] = [];
    const off = await sub.subscribe('news', (m, ch) => void got.push(`${ch}:${m}`));
    await sub.psubscribe('ev.*', (m, ch, p) => void pat.push(`${p}|${ch}|${m}`));
    assert.equal(await c.publish('news', 'one'), 1);
    assert.equal(await c.publish('ev.x', 'two'), 1);
    await until(() => got.length === 1 && pat.length === 1, 3000, 'messages');
    assert.deepEqual(got, ['news:one']);
    assert.deepEqual(pat, ['ev.*|ev.x|two']);
    await off();
    assert.equal(await c.publish('news', 'three'), 0);
    sub.close();
  });

  test('pub/sub resumes after the server connection drops', async () => {
    const sub = c.subscriber({ retryMinMs: 50, retryMaxMs: 100 });
    sub.on('error', () => undefined);
    const got: string[] = [];
    await sub.subscribe('r', (m) => void got.push(m));
    // Kill only the subscriber connection.
    const list = String(await c.call('CLIENT', 'LIST'));
    const id = list.split('\n').find((l) => l.includes('cmd=subscribe') || l.includes('sub=1'))?.match(/id=(\d+)/)?.[1];
    assert.ok(id, 'subscriber connection found');
    await c.call('CLIENT', 'KILL', 'ID', id);
    await until(async () => (await c.publish('r', 'again')) === 1, 5000, 'resubscribe');
    await until(() => got.includes('again'), 3000, 'message after resubscribe');
    sub.close();
  });

  test('cache wraps loaders once and expires', async () => {
    const cache = new SnugKvCache(c, { prefix: 'c:', ttlSeconds: 60 });
    let calls = 0;
    const load = async () => {
      calls++;
      await sleep(30);
      return { v: calls };
    };
    const [a, b] = await Promise.all([cache.wrap('k', load), cache.wrap('k', load)]);
    assert.deepEqual(a, { v: 1 });
    assert.deepEqual(b, { v: 1 });
    assert.deepEqual(await cache.wrap('k', load), { v: 1 });
    assert.equal(calls, 1);
    assert.ok((await c.ttl('c:k')) > 0);
    await cache.del('k');
    assert.equal(await cache.get('k'), undefined);
    await cache.set('x', 1);
    await cache.set('y', 2);
    assert.equal(await cache.clear(), 2);
  });

  test('locks are exclusive, expire and release only by the holder', async () => {
    const locks = new SnugKvLocks(c);
    const a = await locks.acquire('job', { ttlMs: 5000 });
    assert.ok(a);
    assert.equal(await locks.acquire('job'), null);
    assert.equal(await a.extend(5000), true);
    assert.equal(await a.release(), true);
    assert.equal(await a.release(), false);
    const b = await locks.acquire('job', { ttlMs: 100 });
    assert.ok(b);
    const waited = await locks.acquire('job', { waitMs: 2000, retryMs: 20 });
    assert.ok(waited, 'acquired after expiry');
    assert.equal(await b.release(), false, 'old holder cannot release');
    assert.equal(await waited.release(), true);
    const r = await locks.run('x', () => 42);
    assert.deepEqual(r, { acquired: true, value: 42 });
    assert.equal(await c.exists('lock:x'), 0);
  });

  test('rate limiter counts per window', async () => {
    const rl = new SnugKvRateLimiter(c);
    const out = [];
    for (let i = 0; i < 4; i++) out.push(await rl.hit('u1', 3, 400));
    assert.deepEqual(out.map((r) => r.allowed), [true, true, true, false]);
    assert.equal(out[3]!.remaining, 0);
    assert.ok(out[0]!.resetMs > 0 && out[0]!.resetMs <= 400);
    await sleep(500);
    assert.equal((await rl.hit('u1', 3, 400)).allowed, true);
    assert.equal((await rl.hit('u2', 3, 400)).count, 1);
  });
});
