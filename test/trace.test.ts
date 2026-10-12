import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { currentTrace, getTracer, parseTraceHeader, setTracer, takeTrace, traceHeader, Tracer, TRACE_FIELD, TRACE_HEADER, withTrace } from '../src/core/trace';
import { instrumentHttpClient, maskPath, uninstrumentHttpClient } from '../src/core/trace-http';
import type { Arg } from '../src/core/resp';

/** Collects the XADD commands the tracer sends. */
function fakeClient(fail = false) {
  const writes: Arg[][] = [];
  return {
    writes,
    fail,
    async pipeline(cmds: Arg[][]) {
      if (this.fail) throw new Error('down');
      writes.push(...cmds);
      return cmds.map(() => 'ok');
    },
  };
}

/** Field map of one XADD command. */
function fields(cmd: Arg[]): Record<string, string> {
  const star = cmd.indexOf('*');
  const out: Record<string, string> = {};
  for (let i = star + 1; i < cmd.length; i += 2) out[String(cmd[i])] = String(cmd[i + 1]);
  return out;
}

describe('Tracer', () => {
  test('nested steps share a trace and point at their parent', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    await t.step('webhook', { chain: 'eth' }, async () => {
      await t.step('db.update', {}, async () => undefined);
      t.event('push.sent', { to: 'device' });
    });
    await t.close();
    const spans = c.writes.map(fields);
    assert.equal(spans.length, 3);
    const root = spans.find((s) => s.name === 'webhook')!;
    const db = spans.find((s) => s.name === 'db.update')!;
    const push = spans.find((s) => s.name === 'push.sent')!;
    assert.equal(root.parentId, '');
    assert.equal(db.traceId, root.traceId);
    assert.equal(db.parentId, root.spanId);
    assert.equal(push.parentId, root.spanId);
    assert.equal(root.service, 'api');
    assert.equal(root.status, 'ok');
    assert.deepEqual(JSON.parse(root.attrs), { chain: 'eth' });
    assert.equal(c.writes[0]![0], 'XADD');
    assert.equal(c.writes[0]![1], 'snug:traces');
  });

  test('records a thrown error and rethrows it unchanged', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    const boom = new Error('receipt status 0');
    await assert.rejects(t.step('check', {}, () => Promise.reject(boom)), (e) => e === boom);
    await t.close();
    const s = fields(c.writes[0]!);
    assert.equal(s.status, 'error');
    assert.equal(s.error, 'receipt status 0');
    assert.ok(s.stack!.includes('receipt status 0'));
  });

  test('redacts secrets and bounds long strings', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000, maxAttrLength: 5 });
    t.event('login', { authToken: 'abc', nested: { privateKey: 'k', ok: 'fine' }, long: '1234567890' });
    await t.close();
    const attrs = JSON.parse(fields(c.writes[0]!).attrs!);
    assert.equal(attrs.authToken, '[redacted]');
    assert.equal(attrs.nested.privateKey, '[redacted]');
    assert.equal(attrs.nested.ok, 'fine');
    assert.equal(attrs.long, '12345…');
  });

  test('keeps spans when the server is down and writes them later', async () => {
    const c = fakeClient(true);
    let errors = 0;
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000, onError: () => errors++ });
    t.event('a');
    await t.flush();
    assert.equal(errors, 1);
    assert.equal(c.writes.length, 0);
    c.fail = false;
    await t.close();
    assert.equal(c.writes.length, 1);
  });

  test('continue() joins a trace from another process', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'ledger', flushMs: 60_000 });
    await t.continue({ traceId: 'aa'.repeat(16), spanId: 'bb'.repeat(8) }, 'chain-events:deposit', {}, async () => {
      assert.equal(currentTrace()!.traceId, 'aa'.repeat(16));
    });
    await t.close();
    const s = fields(c.writes[0]!);
    assert.equal(s.traceId, 'aa'.repeat(16));
    assert.equal(s.parentId, 'bb'.repeat(8));
  });
});

describe('trace in job data', () => {
  test('added only while a trace is active and a tracer is set', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    assert.deepEqual(withTrace({ a: 1 }), { a: 1 });
    setTracer(t);
    try {
      assert.deepEqual(withTrace({ a: 1 }), { a: 1 }); // no active trace
      await t.step('webhook', {}, async () => {
        const data = withTrace({ a: 1 }) as any;
        assert.equal(data[TRACE_FIELD].traceId, currentTrace()!.traceId);
        assert.equal(withTrace('text'), 'text');
        assert.deepEqual(withTrace([1]), [1]);
        const got = takeTrace(data);
        assert.equal(got!.spanId, currentTrace()!.spanId);
        assert.deepEqual(data, { a: 1 });
      });
    } finally {
      setTracer(undefined);
      await t.close();
    }
    assert.equal(getTracer(), undefined);
  });
});

describe('outgoing HTTP calls', () => {
  test('masks keys in paths and drops the query', () => {
    assert.equal(maskPath('/v2/abcDEF1234567890xyz?token=1'), '/v2/…');
    assert.equal(maskPath('/api/v2/push/send'), '/api/v2/push/send');
    assert.equal(maskPath('/coins/ethereum/market_chart'), '/coins/ethereum/market_chart');
  });

  test('records http and fetch calls made inside a trace, not outside', async () => {
    const server = http.createServer((req, res) => {
      res.statusCode = req.url?.startsWith('/fail') ? 503 : 200;
      res.end('ok');
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as AddressInfo).port;
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    setTracer(t);
    instrumentHttpClient();
    const get = (p: string) =>
      new Promise<void>((resolve, reject) => {
        http.get({ host: '127.0.0.1', port, path: p }, (res) => {
          res.resume();
          res.on('end', resolve);
        }).on('error', reject);
      });
    try {
      await get('/outside');
      await t.step('job', {}, async () => {
        await get('/v2/abcDEF1234567890xyz?key=secret');
        await (await fetch(`http://127.0.0.1:${port}/fail`)).text();
      });
      await t.flush();
      const spans = c.writes.map(fields);
      const root = spans.find((s) => s.name === 'job')!;
      const calls = spans.filter((s) => s.name.startsWith('HTTP '));
      assert.equal(calls.length, 2, JSON.stringify(spans.map((s) => s.name)));
      for (const s of calls) assert.equal(s.parentId, root.spanId);
      assert.ok(calls.some((s) => JSON.parse(s.attrs!).path === '/v2/…'));
      assert.ok(!JSON.stringify(spans).includes('secret'));
      const failed = calls.find((s) => JSON.parse(s.attrs!).path === '/fail')!;
      assert.equal(failed.status, 'error');
      assert.equal(JSON.parse(failed.attrs!).statusCode, 503);
    } finally {
      uninstrumentHttpClient();
      setTracer(undefined);
      await t.close();
      server.close();
    }
  });
});

describe('message headers', () => {
  test('a trace survives the header round trip', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    setTracer(t);
    try {
      assert.deepEqual(traceHeader(), {});
      await t.step('webhook', {}, async () => {
        const h = traceHeader();
        assert.deepEqual(parseTraceHeader(h[TRACE_HEADER]), currentTrace());
      });
      assert.equal(parseTraceHeader('nonsense'), undefined);
    } finally {
      setTracer(undefined);
      await t.close();
    }
  });
});
