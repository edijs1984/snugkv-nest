import type { SnugKvClient } from './client';
import { Arg, Reply } from './resp';

/**
 * Collects commands and runs them as MULTI … EXEC in a single write.
 * `exec()` resolves to the reply of each command, in order. For
 * check-and-set patterns use a Lua script (`client.eval`) instead of WATCH.
 */
export class Transaction {
  private readonly commands: Arg[][] = [];

  constructor(private readonly client: SnugKvClient) {}

  /** Any command. */
  cmd(...args: Arg[]): this {
    this.commands.push(args);
    return this;
  }

  set(key: string, value: string | number): this {
    return this.cmd('SET', key, value);
  }

  del(...keys: string[]): this {
    return this.cmd('DEL', ...keys);
  }

  incr(key: string, by = 1): this {
    return by === 1 ? this.cmd('INCR', key) : this.cmd('INCRBY', key, by);
  }

  expire(key: string, seconds: number): this {
    return this.cmd('EXPIRE', key, seconds);
  }

  hset(key: string, fields: Record<string, string | number>): this {
    return this.cmd('HSET', key, ...Object.entries(fields).flat());
  }

  rpush(key: string, ...values: Array<string | number>): this {
    return this.cmd('RPUSH', key, ...values);
  }

  sadd(key: string, ...members: string[]): this {
    return this.cmd('SADD', key, ...members);
  }

  zadd(key: string, score: number, member: string): this {
    return this.cmd('ZADD', key, score, member);
  }

  get size(): number {
    return this.commands.length;
  }

  exec(): Promise<Reply[]> {
    return this.client.execBlock(this.commands);
  }
}
