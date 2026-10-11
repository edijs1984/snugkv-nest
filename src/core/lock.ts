import type { SnugKvClient } from './client';
import { sleep, uuid } from './util';

export interface LockOptions {
  /** Lock expires on its own after this long, in ms. Default 30000. */
  ttlMs?: number;
  /** Keep retrying to acquire for this long, in ms. Default 0 (try once). */
  waitMs?: number;
  /** Delay between attempts. Default 50. */
  retryMs?: number;
}

export class Lock {
  constructor(
    private readonly client: SnugKvClient,
    readonly key: string,
    readonly token: string,
  ) {}

  /** Releases only if we still hold it. Returns false if it had expired or was taken over. */
  async release(): Promise<boolean> {
    const r = await this.client.eval(RELEASE, [this.key], [this.token]);
    return r === 1;
  }

  /** Extends the expiry, only if we still hold the lock. */
  async extend(ttlMs: number): Promise<boolean> {
    const r = await this.client.eval(EXTEND, [this.key], [this.token, ttlMs]);
    return r === 1;
  }
}

const RELEASE = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;
const EXTEND = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end`;

/**
 * Single-instance mutex: `SET key token NX PX ttl`. It is safe against
 * crashes (the lock expires) but, like any single-server lock, not against
 * the server itself failing over. Keep critical sections idempotent.
 */
export class SnugKvLocks {
  constructor(
    private readonly client: SnugKvClient,
    private readonly prefix = 'lock:',
  ) {}

  /** Returns the lock, or null if it is held by someone else. */
  async acquire(name: string, opts: LockOptions = {}): Promise<Lock | null> {
    const key = this.prefix + name;
    const token = uuid();
    const ttl = opts.ttlMs ?? 30_000;
    const deadline = Date.now() + (opts.waitMs ?? 0);
    for (;;) {
      if (await this.client.setOpts(key, token, { pxMs: ttl, nx: true })) return new Lock(this.client, key, token);
      if (Date.now() >= deadline) return null;
      await sleep(opts.retryMs ?? 50);
    }
  }

  /** Runs `fn` while holding the lock. Returns `{ acquired: false }` if it could not be taken. */
  async run<T>(name: string, fn: () => Promise<T> | T, opts: LockOptions = {}): Promise<{ acquired: true; value: T } | { acquired: false }> {
    const lock = await this.acquire(name, opts);
    if (!lock) return { acquired: false };
    try {
      return { acquired: true, value: await fn() };
    } finally {
      await lock.release().catch(() => undefined);
    }
  }
}
