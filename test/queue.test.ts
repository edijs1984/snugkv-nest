import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, test } from 'node:test';
import { SnugKvClient } from '../src/core';
import { DelayedError, FlowProducer, Job, Queue, QueueEvents, RateLimitError, UnrecoverableError, Worker, nextCronTime } from '../src/core/queue';
import { haveServer, sleep, startServer, TestServer, until } from './helpers';

const opts = { skip: !haveServer && 'set SNUGKV_BIN to a snugkv binary' };

describe('cron', () => {
  test('next times in UTC and in a zone', () => {
    const from = Date.UTC(2026, 0, 1, 10, 0, 30);
    assert.equal(nextCronTime('*/15 * * * *', from), Date.UTC(2026, 0, 1, 10, 15));
    assert.equal(nextCronTime('0 9 * * *', from), Date.UTC(2026, 0, 2, 9));
    assert.equal(nextCronTime('30 8 1 * *', from), Date.UTC(2026, 1, 1, 8, 30));
    assert.equal(nextCronTime('0 0 * * mon', from), Date.UTC(2026, 0, 5, 0)); // 2026-01-05 is a Monday
    assert.equal(nextCronTime('*/10 * * * * *', from), Date.UTC(2026, 0, 1, 10, 0, 40));
    assert.equal(nextCronTime('0 0 29 2 *', from), Date.UTC(2028, 1, 29));
    // Riga is UTC+2 in January.
    assert.equal(nextCronTime('0 9 * * *', from, 'Europe/Riga'), Date.UTC(2026, 0, 2, 7));
    // ...and UTC+3 in July.
    assert.equal(nextCronTime('0 9 * * *', Date.UTC(2026, 6, 1, 12), 'Europe/Riga'), Date.UTC(2026, 6, 2, 6));
    assert.throws(() => nextCronTime('61 * * * *', from));
    assert.throws(() => nextCronTime('* * *', from));
  });
});

describe('queues against a real server', opts, () => {
  let server: TestServer;
  let client: SnugKvClient;
  let n = 0;
  const closers: Array<() => Promise<unknown> | unknown> = [];
  const name = () => `q${++n}`;

  beforeEach(async () => {
    server = await startServer();
    client = new SnugKvClient({ port: server.port });
    await client.connect(5000);
  });

  afterEach(async () => {
    for (const c of closers.splice(0).reverse()) await Promise.resolve(c()).catch(() => undefined);
    client.close();
    await server.stop();
  });

  const queue = (qn: string) => {
    const q = new Queue(qn, { client });
    closers.push(() => q.close());
    return q;
  };
  const worker = <D, R>(qn: string, fn: (job: Job<D, R>, token: string) => Promise<R> | R, o: Record<string, unknown> = {}) => {
    const w = new Worker<D, R>(qn, fn, { client, drainDelay: 300, ...o });
    w.on('error', () => undefined);
    closers.push(() => w.close(true));
    return w;
  };

  test('adds jobs and processes them in order, with priority and lifo', async () => {
    const qn = name();
    const q = queue(qn);
    const a = await q.add('t', { n: 'a' });
    await q.add('t', { n: 'b' });
    await q.add('t', { n: 'c' }, { lifo: true });
    await q.add('t', { n: 'd' }, { priority: 2 });
    await q.add('t', { n: 'e' }, { priority: 1 });
    assert.equal(a.id, '1');
    assert.deepEqual(await q.getJobCounts('wait', 'prioritized'), { wait: 3, prioritized: 2 });
    const seen: string[] = [];
    const w = worker<{ n: string }, string>(qn, async (job) => {
      seen.push(job.data.n);
      return job.data.n.toUpperCase();
    });
    await until(() => seen.length === 5);
    assert.deepEqual(seen, ['c', 'a', 'b', 'e', 'd']);
    await until(async () => (await q.getJobCounts('completed')).completed === 5);
    const done = await q.getJob(a.id);
    assert.equal(done?.returnvalue, 'A');
    assert.equal(await done?.getState(), 'completed');
    assert.equal(done?.attemptsMade, 1);
    await w.close();
  });

  test('retries with backoff, then fails for good, then retry() runs it again', async () => {
    const qn = name();
    const q = queue(qn);
    const calls: number[] = [];
    const failed: string[] = [];
    const w = worker(qn, async (job) => {
      calls.push(Date.now());
      throw new Error(`boom ${job.attemptsMade}`);
    });
    w.on('failed', (job, err) => failed.push(err.message));
    const job = await q.add('t', {}, { attempts: 3, backoff: { type: 'fixed', delay: 150 } });
    await until(async () => (await q.getJobCounts('failed')).failed === 1, 8000);
    assert.equal(calls.length, 3);
    assert.ok(calls[1]! - calls[0]! >= 140, 'backoff delay');
    assert.deepEqual(failed, ['boom 1', 'boom 2', 'boom 3']);
    const j = (await q.getJob(job.id))!;
    assert.equal(j.failedReason, 'boom 3');
    assert.equal(j.stacktrace.length, 3);
    await j.retry('failed', { resetAttemptsMade: true });
    await until(() => calls.length === 6, 8000);
  });

  test('custom backoff, unrecoverable errors', async () => {
    const qn = name();
    const q = queue(qn);
    const delays: number[] = [];
    const w = worker(
      qn,
      async (job) => {
        if (job.name === 'fatal') throw new UnrecoverableError('no retry');
        throw new Error('x');
      },
      { settings: { backoffStrategy: (made: number) => (delays.push(made), 50 * made) } },
    );
    void w;
    await q.add('fatal', {}, { attempts: 5 });
    const c = await q.add('custom', {}, { attempts: 3, backoff: { type: 'custom' } });
    await until(async () => (await q.getJobCounts('failed')).failed === 2, 8000);
    assert.deepEqual(delays, [1, 2]);
    assert.equal((await q.getJob(c.id))?.attemptsMade, 3);
    const fatal = (await q.getFailed())[1]!;
    assert.equal(fatal.attemptsMade, 1);
  });

  test('delayed jobs, promote and change delay', async () => {
    const qn = name();
    const q = queue(qn);
    const ran: string[] = [];
    worker(qn, async (job) => void ran.push(job.name));
    const t0 = Date.now();
    await q.add('slow', {}, { delay: 100000 });
    const soon = await q.add('soon', {}, { delay: 250 });
    const other = await q.add('later', {}, { delay: 100000 });
    assert.equal(await soon.getState(), 'delayed');
    await (await q.getJob(other.id))!.changeDelay(200);
    await until(() => ran.length === 2);
    assert.ok(Date.now() - t0 >= 190);
    const slow = (await q.getDelayed())[0]!;
    await slow.promote();
    await until(() => ran.length === 3);
    assert.deepEqual(ran.sort(), ['later', 'slow', 'soon']);
  });

  test('QueueEvents and waitUntilFinished', async () => {
    const qn = name();
    const q = queue(qn);
    const events = new QueueEvents(qn, { client });
    closers.push(() => events.close());
    await events.waitUntilReady();
    const seen: string[] = [];
    for (const e of ['added', 'waiting', 'active', 'progress', 'completed', 'failed']) events.on(e, (a) => seen.push(`${e}:${a.jobId}`));
    worker(qn, async (job) => {
      await job.updateProgress({ pct: 50 });
      if (job.name === 'bad') throw new Error('nope');
      return { sum: 3 };
    });
    const ok = await q.add('good', {});
    assert.deepEqual(await ok.waitUntilFinished(events, 5000), { sum: 3 });
    const bad = await q.add('bad', {});
    await assert.rejects(bad.waitUntilFinished(events, 5000), /nope/);
    // A job that finished before we asked.
    assert.deepEqual(await ok.waitUntilFinished(events, 1000), { sum: 3 });
    assert.ok(seen.includes(`completed:${ok.id}`) && seen.includes(`failed:${bad.id}`) && seen.includes(`progress:${ok.id}`), seen.join(','));
  });

  test('concurrency, pause and resume, drain', async () => {
    const qn = name();
    const q = queue(qn);
    let running = 0;
    let peak = 0;
    let done = 0;
    await q.pause();
    for (let i = 0; i < 6; i++) await q.add('t', { i });
    assert.equal(await q.isPaused(), true);
    worker(
      qn,
      async () => {
        peak = Math.max(peak, ++running);
        await sleep(100);
        running--;
        done++;
      },
      { concurrency: 3 },
    );
    await sleep(400);
    assert.equal(done, 0);
    await q.resume();
    await until(() => done === 6);
    assert.equal(peak, 3);
    await q.add('t', {});
    await q.pause();
    await q.add('t', {});
    assert.ok((await q.drain()) >= 1);
    assert.equal(await q.count(), 0);
  });

  test('global concurrency and rate limiting', async () => {
    const qn = name();
    const q = queue(qn);
    await q.setGlobalConcurrency(1);
    let running = 0;
    let peak = 0;
    let done = 0;
    const fn = async () => {
      peak = Math.max(peak, ++running);
      await sleep(60);
      running--;
      done++;
    };
    worker(qn, fn, { concurrency: 3 });
    worker(qn, fn, { concurrency: 3 });
    for (let i = 0; i < 4; i++) await q.add('t', {});
    await until(() => done === 4, 10000);
    assert.equal(peak, 1);
    await q.removeGlobalConcurrency();

    const times: number[] = [];
    const qn2 = name();
    const q2 = queue(qn2);
    await q2.setRateLimit({ max: 2, duration: 400 });
    worker(qn2, () => void times.push(Date.now()), { concurrency: 4 });
    for (let i = 0; i < 4; i++) await q2.add('t', {});
    await until(() => times.length === 4, 10000);
    assert.ok(times[2]! - times[0]! >= 350, `rate limit window: ${times[2]! - times[0]!}`);
  });

  test('manual rate limit from a processor does not use an attempt', async () => {
    const qn = name();
    const q = queue(qn);
    let calls = 0;
    const w = worker(qn, async (job) => {
      calls++;
      if (calls === 1) {
        await w.rateLimit(300);
        throw new RateLimitError();
      }
      return job.attemptsMade;
    });
    const t0 = Date.now();
    const job = await q.add('t', {});
    await until(async () => (await q.getJobCounts('completed')).completed === 1, 8000);
    assert.ok(Date.now() - t0 >= 280);
    assert.equal((await q.getJob(job.id))?.returnvalue, 1);
  });

  test('moveToDelayed from a processor', async () => {
    const qn = name();
    const q = queue(qn);
    let calls = 0;
    worker(qn, async (job, token) => {
      if (++calls === 1) {
        await job.moveToDelayed(Date.now() + 150, token);
        throw new DelayedError();
      }
    });
    await q.add('t', {});
    await until(async () => (await q.getJobCounts('completed')).completed === 1, 8000);
    assert.equal(calls, 2);
  });

  test('stalled jobs are recovered when a worker dies', async () => {
    const qn = name();
    const q = queue(qn);
    let started = 0;
    const w1 = worker(qn, () => new Promise(() => void started++), { lockDuration: 300, lockRenewTime: 100_000, stalledInterval: 200, skipStalledCheck: true });
    await q.add('t', {});
    await until(() => started === 1);
    await w1.close(true);
    const stalled: string[] = [];
    const w2 = worker(qn, async () => 'recovered', { lockDuration: 300, stalledInterval: 200 });
    w2.on('stalled', (id) => stalled.push(id));
    await until(async () => (await q.getJobCounts('completed')).completed === 1, 8000);
    assert.equal(stalled.length, 1);
  });

  test('deduplication and custom ids', async () => {
    const qn = name();
    const q = queue(qn);
    const a = await q.add('t', 1, { jobId: 'same' });
    const b = await q.add('t', 2, { jobId: 'same' });
    assert.equal(a.id, b.id);
    assert.equal(b.data, 1);
    await assert.rejects(q.add('t', 1, { jobId: '42' }), /integers/);
    const d1 = await q.add('t', 1, { deduplication: { id: 'k' } });
    const d2 = await q.add('t', 2, { deduplication: { id: 'k' } });
    assert.equal(d1.id, d2.id);
    assert.equal(await q.count(), 2);
  });

  test('removeOnComplete and getJobs', async () => {
    const qn = name();
    const q = queue(qn);
    worker(qn, () => 'x');
    for (let i = 0; i < 5; i++) await q.add('t', { i }, { removeOnComplete: 2 });
    await until(async () => (await q.getJobCounts('completed')).completed === 2 && (await q.getJobCounts('wait')).wait === 0);
    const done = await q.getCompleted();
    assert.deepEqual(done.map((j) => j.data), [{ i: 4 }, { i: 3 }]);
    assert.deepEqual((await q.getMetrics('completed', 5)).count, 5);
    assert.equal((await q.clean(0, 0, 'completed')).length, 2);
    assert.deepEqual(await Queue.list(client), [qn]);
    await q.obliterate();
    assert.deepEqual(await Queue.list(client), []);
  });

  test('flows: the parent runs after its children and sees their results', async () => {
    const qn = name();
    const flow = new FlowProducer({ client });
    const order: string[] = [];
    const childQ = `${qn}-child`;
    worker<number, number>(childQ, async (job) => {
      order.push(job.name);
      return job.data * 10;
    });
    worker(qn, async (job) => {
      order.push(job.name);
      return Object.values(await job.getChildrenValues()).reduce((a: number, b) => a + (b as number), 0);
    });
    const tree = await flow.add({
      name: 'parent',
      queueName: qn,
      children: [
        { name: 'c1', queueName: childQ, data: 1 },
        { name: 'c2', queueName: childQ, data: 2 },
      ],
    });
    const q = queue(qn);
    assert.equal(await tree.job.getState(), 'waiting-children');
    await until(async () => (await q.getJobCounts('completed')).completed === 1, 8000);
    assert.equal(order[2], 'parent');
    assert.equal((await q.getJob(tree.job.id))?.returnvalue, 30);
  });

  test('flows: failParentOnFailure and ignoreDependencyOnFailure', async () => {
    const qn = name();
    const flow = new FlowProducer({ client });
    worker(`${qn}-c`, async (job) => {
      if (job.name === 'bad') throw new Error('child failed');
      return 'ok';
    });
    worker(qn, async (job) => Object.keys(await job.getFailedChildrenValues()).length);
    const q = queue(qn);
    const failing = await flow.add({
      name: 'p1',
      queueName: qn,
      children: [{ name: 'bad', queueName: `${qn}-c`, opts: { failParentOnFailure: true } }],
    });
    const tolerant = await flow.add({
      name: 'p2',
      queueName: qn,
      children: [
        { name: 'bad', queueName: `${qn}-c`, opts: { ignoreDependencyOnFailure: true } },
        { name: 'good', queueName: `${qn}-c` },
      ],
    });
    await until(async () => (await q.getJobCounts('completed', 'failed')).completed === 1);
    assert.match((await q.getJob(failing.job.id))?.failedReason ?? '', /child failed/);
    assert.equal((await q.getJob(tolerant.job.id))?.returnvalue, 1);
  });

  test('schedulers: every, and cron computed by the worker', async () => {
    const qn = name();
    const q = queue(qn);
    const runs: string[] = [];
    worker(qn, async (job) => void runs.push(job.name));
    assert.equal(await q.upsertJobScheduler('tick', { every: 120 }, { name: 'tick', data: { a: 1 } }), true);
    await until(() => runs.length >= 3, 8000);
    const [s] = await q.getJobSchedulers();
    assert.equal(s?.id, 'tick');
    assert.equal(s?.every, 120);
    assert.ok(s!.count >= 3);
    assert.equal(await q.removeJobScheduler('tick'), true);
    await sleep(300);
    const after = runs.length;
    await sleep(400);
    assert.equal(runs.length, after);

    const cq = queue(name());
    await cq.upsertJobScheduler('cron', { pattern: '* * * * * *' }, { name: 'cron' });
    const cw = worker(cq.name, async (job) => void runs.push(`cron:${job.id}`));
    void cw;
    await until(() => runs.filter((r) => r.startsWith('cron:')).length >= 2, 8000);
    const list = await cq.getJobSchedulers();
    assert.equal(list[0]?.pattern, '* * * * * *');
  });

  test('worker.close waits for running jobs', async () => {
    const qn = name();
    const q = queue(qn);
    let finished = false;
    const w = worker(qn, async () => {
      await sleep(300);
      finished = true;
    });
    await q.add('t', {});
    await until(async () => (await q.getJobCounts('active')).active === 1);
    await w.close();
    assert.equal(finished, true);
    assert.equal((await q.getJobCounts('completed')).completed, 1);
  });
});
