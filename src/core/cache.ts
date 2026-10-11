import type { SnugKvClient } from './client';

export interface CacheOptions {
  /** Prefix for every key. Default `cache:`. */
  prefix?: string;
  /** Default time to live in seconds. Default 60. */
  ttlSeconds?: number;
}

/**
 * JSON cache on top of strings with expiry. `wrap` runs the loader at most
 * once at a time per key inside this process (concurrent callers share the
 * result).
 */
export class SnugKvCache {
  private readonly prefix: string;
  private readonly ttl: number;
  private readonly inflight = new Map<string, Promise<unknown>>();

  constructor(private readonly client: SnugKvClient, opts: CacheOptions = {}) {
    this.prefix = opts.prefix ?? 'cache:';
    this.ttl = opts.ttlSeconds ?? 60;
  }

  key(key: string): string {
    return this.prefix + key;
  }

  async get<T = unknown>(key: string): Promise<T | undefined> {
    const raw = await this.client.get(this.key(key));
    return raw === null ? undefined : (JSON.parse(raw) as T);
  }

  async set(key: string, value: unknown, ttlSeconds = this.ttl): Promise<void> {
    await this.client.setJson(this.key(key), value, ttlSeconds);
  }

  async del(...keys: string[]): Promise<number> {
    return keys.length ? this.client.del(...keys.map((k) => this.key(k))) : 0;
  }

  /** Returns the cached value, or runs `load`, stores the result and returns it. `undefined` results are not cached. */
  async wrap<T>(key: string, load: () => Promise<T> | T, ttlSeconds = this.ttl): Promise<T> {
    const hit = await this.get<T>(key);
    if (hit !== undefined) return hit;
    const running = this.inflight.get(key) as Promise<T> | undefined;
    if (running) return running;
    const p = (async () => {
      try {
        const value = await load();
        if (value !== undefined) await this.set(key, value, ttlSeconds);
        return value;
      } finally {
        this.inflight.delete(key);
      }
    })();
    this.inflight.set(key, p);
    return p;
  }

  /** Deletes every key under the cache prefix that matches `pattern` (default all). */
  async clear(pattern = '*'): Promise<number> {
    let n = 0;
    let batch: string[] = [];
    for await (const k of this.client.scanIterator({ match: this.prefix + pattern, count: 200 })) {
      batch.push(k);
      if (batch.length >= 200) {
        n += await this.client.del(...batch);
        batch = [];
      }
    }
    if (batch.length) n += await this.client.del(...batch);
    return n;
  }
}
