import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { captureLogs, getLogShipper, LogShipper, parseLogLine, splitEntries, toLevel } from '../src/core/logs';
import { Tracer } from '../src/core/trace';
import type { Arg } from '../src/core/resp';

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

function fields(cmd: Arg[]): Record<string, string> {
  const star = cmd.indexOf('*');
  const out: Record<string, string> = {};
  for (let i = star + 1; i < cmd.length; i += 2) out[String(cmd[i])] = String(cmd[i + 1]);
  return out;
}

describe('parseLogLine', () => {
  test('Nest console logger', () => {
    const p = parseLogLine('\x1b[32m[Nest] 42  - \x1b[39m10/16/2026, 6:19:00 AM \x1b[33m    LOG\x1b[39m \x1b[33m[RouterExplorer] \x1b[39m\x1b[32mMapped {/api/x, GET} route\x1b[39m\x1b[33m +2ms\x1b[39m');
    assert.deepEqual(p, { level: 'info', context: 'RouterExplorer', message: 'Mapped {/api/x, GET} route' });
    assert.equal(parseLogLine('[Nest] 7  - 10/16/2026, 6:19:00 AM   ERROR [ExceptionsHandler] boom').level, 'error');
    assert.equal(parseLogLine('[Nest] 7  - 10/16/2026, 6:19:00 AM    WARN [X] careful').level, 'warn');
  });

  test('winston timestamp [Context] level: message', () => {
    const p = parseLogLine('2026-10-15T15:24:40.947Z [TokenService] \x1b[32minfo\x1b[39m: \x1b[32mUsing network: testnet\x1b[39m \x1b[32m+8ms\x1b[39m');
    assert.equal(p.level, 'info');
    assert.equal(p.context, 'TokenService');
    assert.equal(p.message, 'Using network: testnet');
    assert.equal(p.ts, Date.parse('2026-10-15T15:24:40.947Z'));
    assert.equal(parseLogLine('2026-10-15T15:24:40.947Z  error: no context').level, 'error');
  });

  test('JSON lines (pino, winston json)', () => {
    assert.deepEqual(parseLogLine('{"level":50,"time":1760000000000,"msg":"failed","name":"ledger"}'), {
      level: 'error',
      message: 'failed',
      context: 'ledger',
      ts: 1760000000000,
    });
    assert.equal(parseLogLine('{"level":"warn","message":"slow","context":"Prices"}').level, 'warn');
  });

  test('plain lines use the fallback, or a leading level word', () => {
    assert.deepEqual(parseLogLine('Refreshing token balances for wallet eth:0xabc'), { level: 'info', message: 'Refreshing token balances for wallet eth:0xabc' });
    assert.equal(parseLogLine('something broke', 'error').level, 'error');
    assert.deepEqual(parseLogLine('WARN: disk almost full'), { level: 'warn', message: 'disk almost full' });
    assert.deepEqual(parseLogLine('[error] lost connection'), { level: 'error', message: 'lost connection' });
    assert.deepEqual(parseLogLine('ERROR lost connection'), { level: 'error', message: 'lost connection' });
    // An ordinary sentence that starts with a level word is not a level
    assert.deepEqual(parseLogLine('Error handling is enabled'), { level: 'info', message: 'Error handling is enabled' });
  });

  test('toLevel', () => {
    assert.equal(toLevel('VERBOSE'), 'debug');
    assert.equal(toLevel('fatal'), 'error');
    assert.equal(toLevel(30), 'info');
    assert.equal(toLevel('weird', 'warn'), 'warn');
  });
});

describe('splitEntries', () => {
  test('a stack stays with its message', () => {
    const e = splitEntries('Error: boom\n    at f (a.js:1:1)\n    at g (b.js:2:2)\n');
    assert.equal(e.length, 1);
    assert.match(e[0]!, /boom\n {4}at f/);
  });

  test('two formatted entries in one write are split', () => {
    const e = splitEntries('2026-10-15T15:24:40.947Z [A] info: one\n2026-10-15T15:24:41.000Z [B] warn: two\n');
    assert.deepEqual(e, ['2026-10-15T15:24:40.947Z [A] info: one', '2026-10-15T15:24:41.000Z [B] warn: two']);
  });

  test('blank output is dropped', () => {
    assert.deepEqual(splitEntries('\n  \n'), []);
  });
});

describe('LogShipper', () => {
  test('writes lines to the stream with service and instance', async () => {
    const c = fakeClient();
    const s = new LogShipper(c as any, { service: 'api', instance: 'box-1', flushMs: 60_000 });
    s.ship({ level: 'warn', message: 'slow rpc', context: 'Rpc', traceId: 'ab'.repeat(16), spanId: 'cd'.repeat(8), ts: 5 });
    await s.close();
    assert.equal(c.writes.length, 1);
    assert.equal(c.writes[0]![1], 'snug:logs');
    assert.deepEqual(fields(c.writes[0]!), {
      ts: '5',
      service: 'api',
      instance: 'box-1',
      level: 'warn',
      message: 'slow rpc',
      context: 'Rpc',
      traceId: 'ab'.repeat(16),
      spanId: 'cd'.repeat(8),
    });
  });

  test('drops lines below the level and cuts long messages', async () => {
    const c = fakeClient();
    const s = new LogShipper(c as any, { service: 'api', level: 'info', maxMessageLength: 5, flushMs: 60_000 });
    s.ship({ level: 'debug', message: 'noise' });
    s.ship({ level: 'info', message: 'abcdefgh' });
    await s.close();
    assert.equal(c.writes.length, 1);
    assert.equal(fields(c.writes[0]!).message, 'abcde…');
  });

  test('keeps lines while the server is down and writes them later', async () => {
    const c = fakeClient(true);
    const errors: unknown[] = [];
    const s = new LogShipper(c as any, { service: 'api', flushMs: 60_000, onError: (e) => errors.push(e) });
    s.ship({ level: 'info', message: 'one' });
    await s.flush();
    assert.equal(errors.length, 1);
    c.fail = false;
    s.ship({ level: 'info', message: 'two' });
    await s.close();
    assert.deepEqual(c.writes.map((w) => fields(w).message), ['one', 'two']);
  });
});

describe('captureLogs', () => {
  test('copies console and stream output, with levels and the trace', async () => {
    const c = fakeClient();
    const s = new LogShipper(c as any, { service: 'ledger', flushMs: 60_000 });
    const tracer = new Tracer(fakeClient() as any, { service: 'ledger', flushMs: 60_000 });
    // Keep the test runner's own output out of the way.
    const realOut = process.stdout.write;
    const realErr = process.stderr.write;
    const swallowed: string[] = [];
    process.stdout.write = ((chunk: unknown) => (swallowed.push(String(chunk)), true)) as typeof process.stdout.write;
    process.stderr.write = ((chunk: unknown) => (swallowed.push(String(chunk)), true)) as typeof process.stderr.write;
    let traceId = '';
    let restore = () => undefined as void;
    try {
      restore = captureLogs(s);
      assert.equal(getLogShipper(), s);
      console.log('plain line');
      console.warn('careful');
      process.stderr.write('raw stderr\n');
      await tracer.step('job', {}, async (span) => {
        traceId = span.context.traceId;
        console.log('inside a trace');
      });
      process.stdout.write('2026-10-15T15:24:40.947Z [TokenService] debug: details\n');
    } finally {
      restore();
      process.stdout.write = realOut;
      process.stderr.write = realErr;
    }
    assert.equal(getLogShipper(), undefined);
    console.log('after restore is not shipped');
    await s.close();
    await tracer.close();
    const got = c.writes.map(fields);
    assert.deepEqual(
      got.map((f) => [f.level, f.message]),
      [
        ['info', 'plain line'],
        ['warn', 'careful'],
        ['error', 'raw stderr'],
        ['info', 'inside a trace'],
        ['debug', 'details'],
      ],
    );
    assert.equal(got[3]!.traceId, traceId);
    assert.equal(got[0]!.traceId, undefined);
    assert.equal(got[4]!.context, 'TokenService');
    assert.ok(swallowed.some((l) => l.includes('plain line')), 'output still reaches the terminal');
  });
});
