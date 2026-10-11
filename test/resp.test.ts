import assert from 'node:assert/strict';
import { test } from 'node:test';
import { encodeCommand, Reply, RespError, RespParser } from '../src/core/resp';

function parseAll(chunks: Buffer[]): Reply[] {
  const p = new RespParser();
  const out: Reply[] = [];
  for (const c of chunks) p.push(c, (r) => out.push(r));
  return out;
}

test('encodes commands with byte lengths', () => {
  assert.equal(encodeCommand(['SET', 'k', 'ä']).toString(), '*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nä\r\n');
  assert.equal(encodeCommand(['INCR', 5]).toString(), '*2\r\n$4\r\nINCR\r\n$1\r\n5\r\n');
});

test('parses every reply type', () => {
  const r = parseAll([Buffer.from('+OK\r\n:42\r\n$-1\r\n$3\r\nabc\r\n*2\r\n$1\r\na\r\n*1\r\n:1\r\n*-1\r\n-ERR boom\r\n')]);
  assert.equal(r[0], 'OK');
  assert.equal(r[1], 42);
  assert.equal(r[2], null);
  assert.equal(r[3], 'abc');
  assert.deepEqual(r[4], ['a', [1]]);
  assert.equal(r[5], null);
  assert.ok(r[6] instanceof RespError);
  assert.equal((r[6] as RespError).code, 'ERR');
});

test('survives replies split at every byte, including multibyte text', () => {
  const text = 'hé€llo';
  const whole = Buffer.from(`*2\r\n$${Buffer.byteLength(text)}\r\n${text}\r\n*1\r\n$2\r\nok\r\n`);
  const chunks = [...whole].map((b) => Buffer.from([b]));
  assert.deepEqual(parseAll(chunks), [[text, ['ok']]]);
});

test('rejects unknown protocol bytes', () => {
  assert.throws(() => parseAll([Buffer.from('?oops\r\n')]), /protocol error/);
});
