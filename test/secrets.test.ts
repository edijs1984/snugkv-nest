import assert from 'node:assert/strict';
import { after, before, describe, test } from 'node:test';
import { spawnSync } from 'node:child_process';
import * as http from 'node:http';
import { fetchSecrets, loadSecrets, SecretsError } from '../src/core/secrets';
import { freePort, haveServer, SNUGKV_BIN, startServer, TestServer } from './helpers';

const ADMIN = 'admin-token-for-nest-tests';
const haveSecrets = haveServer && (() => {
  const r = spawnSync(SNUGKV_BIN, ['-h'], { encoding: 'utf8' });
  return `${r.stdout}${r.stderr}`.includes('secrets-api-listen');
})();

describe('secrets (against the SnugKV module)', { skip: !haveSecrets && 'set SNUGKV_BIN to a snugkv binary with the secrets module' }, () => {
  let server: TestServer;
  let url: string;
  let token: string;

  const admin = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(url + path, { method, headers: { authorization: `Bearer ${ADMIN}` }, body: body === undefined ? undefined : JSON.stringify(body) });
    return (await res.json()) as Record<string, unknown>;
  };

  before(async () => {
    const port = await freePort();
    server = await startServer({ args: ['-secrets-api-listen', `127.0.0.1:${port}`], env: { SNUGKV_SECRETS_KEY: '0123456789abcdef-nest', SNUGKV_SECRETS_API_TOKEN: ADMIN } });
    url = `http://127.0.0.1:${port}`;
    for (let i = 0; i < 40; i++) {
      if ((await fetch(`${url}/v1/status`, { headers: { authorization: `Bearer ${ADMIN}` } }).catch(() => null))?.ok) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    await admin('POST', '/v1/envs/airsig/prod/import', { dotenv: 'MONGO_URI=mongodb://db/airsig\nPORT=3000\nMULTI="a\\nb"\n' });
    token = String((await admin('POST', '/v1/tokens', { name: 'nest', envs: ['airsig/prod'] })).secret);
  });
  after(() => server?.stop());

  test('fetchSecrets returns every value', async () => {
    const v = await fetchSecrets({ url, token, env: 'airsig/prod' });
    assert.deepEqual(v, { MONGO_URI: 'mongodb://db/airsig', PORT: '3000', MULTI: 'a\nb' });
  });

  test('loadSecrets fills the target; what is set wins unless override', async () => {
    const target: Record<string, string | undefined> = { PORT: '8080' };
    const set = await loadSecrets({ url, token, env: 'airsig/prod', target });
    assert.deepEqual(set, ['MONGO_URI', 'MULTI']);
    assert.equal(target.PORT, '8080');
    assert.equal(target.MONGO_URI, 'mongodb://db/airsig');
    await loadSecrets({ url, token, env: 'airsig/prod', target, override: true });
    assert.equal(target.PORT, '3000');
  });

  test('reads its settings from the environment', async () => {
    const saved = { ...process.env };
    Object.assign(process.env, { SNUGKV_SECRETS_URL: url, SNUGKV_SECRETS_TOKEN: token, SNUGKV_SECRETS_ENV: 'airsig/prod' });
    delete process.env.MONGO_URI;
    try {
      await loadSecrets();
      assert.equal(process.env.MONGO_URI, 'mongodb://db/airsig');
    } finally {
      process.env = saved;
    }
  });

  test('a wrong token or an environment the token cannot read fails at once', async () => {
    const t0 = Date.now();
    await assert.rejects(fetchSecrets({ url, token: 'sks_aaaaaaaaaaaaaaaa_nope', env: 'airsig/prod' }), (e: SecretsError) => e.status === 401);
    await assert.rejects(fetchSecrets({ url, token, env: 'airsig/dev' }), (e: SecretsError) => e.status === 403);
    assert.ok(Date.now() - t0 < 2000, 'no retrying on 4xx');
  });
});

describe('secrets (offline behaviour)', () => {
  test('bad settings are refused before any request', async () => {
    await assert.rejects(fetchSecrets({ url: 'http://x', env: 'nope' }), /project\/env/);
    await assert.rejects(fetchSecrets({ url: '', env: 'a/b' }), /no API address/);
  });

  test('retries while the API is starting, then gives up with a clear error', async () => {
    let calls = 0;
    const srv = http.createServer((_req, res) => {
      calls++;
      if (calls < 3) res.writeHead(503).end('{"error":"starting"}');
      else res.writeHead(200, { 'content-type': 'application/json' }).end('{"values":{"A":"1"}}');
    });
    await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r));
    const port = (srv.address() as { port: number }).port;
    try {
      assert.deepEqual(await fetchSecrets({ url: `http://127.0.0.1:${port}`, env: 'a/b', waitMs: 5000 }), { A: '1' });
      assert.equal(calls, 3);
    } finally {
      srv.close();
    }
    const dead = await freePort();
    await assert.rejects(fetchSecrets({ url: `http://127.0.0.1:${dead}`, env: 'a/b', waitMs: 300 }), /cannot reach/);
  });
});
