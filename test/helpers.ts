import { ChildProcess, spawn } from 'node:child_process';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';

export const SNUGKV_BIN = process.env.SNUGKV_BIN ?? '';
export const haveServer = SNUGKV_BIN !== '' && fs.existsSync(SNUGKV_BIN);

export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const port = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(port));
    });
    s.on('error', reject);
  });
}

export interface TestServer {
  port: number;
  dir: string;
  stop(): Promise<void>;
  /** Kill without a clean shutdown, like a crash. */
  kill(): Promise<void>;
}

async function waitPort(port: number, ms = 10_000): Promise<void> {
  const end = Date.now() + ms;
  for (;;) {
    const ok = await new Promise<boolean>((resolve) => {
      const c = net.connect(port, '127.0.0.1');
      c.once('connect', () => (c.destroy(), resolve(true)));
      c.once('error', () => resolve(false));
    });
    if (ok) return;
    if (Date.now() > end) throw new Error(`server did not start on ${port}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** Starts a real SnugKV server. Pass `dir` and `port` to restart on the same data. */
export async function startServer(opts: { dir?: string; port?: number; args?: string[]; env?: NodeJS.ProcessEnv } = {}): Promise<TestServer> {
  const dir = opts.dir ?? fs.mkdtempSync(path.join(os.tmpdir(), 'snugkv-nest-'));
  const port = opts.port ?? (await freePort());
  const adminPort = await freePort();
  const proc: ChildProcess = spawn(
    SNUGKV_BIN,
    ['-listen', `127.0.0.1:${port}`, '-admin-listen', `127.0.0.1:${adminPort}`, '-aof', path.join(dir, 'a.aof'), '-fsync', 'always', '-stream-dir', path.join(dir, 'streams'), ...(opts.args ?? [])],
    { stdio: 'ignore', env: { ...process.env, ...opts.env } },
  );
  const exited = new Promise<void>((resolve) => proc.once('exit', () => resolve()));
  await waitPort(port);
  const end = async (sig: NodeJS.Signals) => {
    if (proc.exitCode === null) proc.kill(sig);
    await exited;
  };
  return { port, dir, stop: () => end('SIGTERM'), kill: () => end('SIGKILL') };
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function until(cond: () => boolean | Promise<boolean>, ms = 8_000, what = 'condition'): Promise<void> {
  const end = Date.now() + ms;
  while (!(await cond())) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await sleep(20);
  }
}

export const quiet = { warn: () => undefined, error: () => undefined };
