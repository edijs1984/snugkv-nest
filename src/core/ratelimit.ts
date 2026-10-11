import type { SnugKvClient } from './client';

export interface RateLimitResult {
  allowed: boolean;
  /** Hits counted in the current window, including this one. */
  count: number;
  remaining: number;
  /** Milliseconds until the window resets. */
  resetMs: number;
}

const SCRIPT = `
local n = redis.call('incr', KEYS[1])
if n == 1 then redis.call('pexpire', KEYS[1], ARGV[1]) end
return {n, redis.call('pttl', KEYS[1])}
`;

/** Fixed-window limiter: at most `limit` hits per `windowMs` per key. */
export class SnugKvRateLimiter {
  constructor(
    private readonly client: SnugKvClient,
    private readonly prefix = 'rl:',
  ) {}

  async hit(key: string, limit: number, windowMs: number): Promise<RateLimitResult> {
    const raw = (await this.client.eval(SCRIPT, [this.prefix + key], [windowMs])) as number[];
    const count = Number(raw[0]);
    let resetMs = Number(raw[1]);
    if (resetMs < 0) {
      // Key lost its expiry (should not happen); restore it so the window cannot stick forever.
      await this.client.pexpire(this.prefix + key, windowMs);
      resetMs = windowMs;
    }
    return { allowed: count <= limit, count, remaining: Math.max(0, limit - count), resetMs };
  }
}
