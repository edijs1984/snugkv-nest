import { EventEmitter } from 'node:events';
import { Connection, ConnectionOptions } from './connection';
import { Arg, Reply, RespError } from './resp';
import { SnugKvSubscriber } from './pubsub';
import { Transaction } from './tx';
import { parseUrl } from './util';

export interface SnugKvClientOptions extends Partial<ConnectionOptions> {
  /** `redis://user:pass@host:port`, `rediss://…`, `snugkv://…` or `snugkvs://…`. */
  url?: string;
}

export interface SetOptions {
  exSeconds?: number;
  pxMs?: number;
  /** Only set when the key does not exist. */
  nx?: boolean;
  /** Only set when the key already exists. */
  xx?: boolean;
  keepTtl?: boolean;
}

export function asArray(r: Reply): Reply[] {
  return Array.isArray(r) ? r : [];
}

export function pairsToObject(flat: Reply[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = String(flat[i + 1]);
  return out;
}

export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

export interface PendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveries: number;
}

export interface XAddOptions {
  /** Cap the stream length. */
  maxLen?: number;
  /** Trim approximately (`~`), cheaper. Default true when maxLen is set. */
  approximate?: boolean;
  /** Explicit entry id. Default `*`. */
  id?: string;
}

export interface XReadGroupOptions {
  group: string;
  consumer: string;
  streams: Array<{ key: string; id: string }>;
  count?: number;
  blockMs?: number;
  noAck?: boolean;
}

function resolveOptions(opts: SnugKvClientOptions): ConnectionOptions {
  const { url, ...given } = opts;
  const rest = Object.fromEntries(Object.entries(given).filter(([, v]) => v !== undefined));
  const fromUrl = url ? parseUrl(url) : undefined;
  return {
    host: '127.0.0.1',
    port: 6379,
    ...(fromUrl ? { ...fromUrl, tls: fromUrl.tls || undefined } : {}),
    ...rest,
  } as ConnectionOptions;
}

export function toEntries(raw: Reply): StreamEntry[] {
  if (!Array.isArray(raw)) return [];
  const out: StreamEntry[] = [];
  for (const item of raw) {
    if (!Array.isArray(item)) continue;
    const id = item[0];
    const flat = item[1];
    if (typeof id !== 'string') continue;
    const fields: Record<string, string> = {};
    if (Array.isArray(flat)) {
      for (let i = 0; i + 1 < flat.length; i += 2) fields[String(flat[i])] = String(flat[i + 1]);
    }
    out.push({ id, fields });
  }
  return out;
}

/**
 * A typed client for SnugKV: strings, hashes, lists, sets, sorted sets,
 * expiry, scan, Lua, pub/sub, transactions and streams, plus a raw `call` for
 * anything else. One client owns one connection; blocking reads belong on
 * their own client (see {@link SnugKvClient.duplicate}).
 */
export class SnugKvClient extends EventEmitter {
  private readonly conn: Connection;
  private readonly options: SnugKvClientOptions;

  constructor(options: SnugKvClientOptions = {}) {
    super();
    this.options = options;
    this.conn = new Connection(resolveOptions(options));
    // An unhandled 'error' event would crash the process; the connection retries by itself.
    this.conn.on('error', (e) => {
      if (this.listenerCount('error') > 0) this.emit('error', e);
    });
    for (const ev of ['ready', 'close', 'reconnecting', 'end'] as const) {
      this.conn.on(ev, (...a: unknown[]) => this.emit(ev, ...a));
    }
  }

  /** @internal */
  get connection(): Connection {
    return this.conn;
  }

  get isReady(): boolean {
    return this.conn.isReady;
  }

  connect(timeoutMs?: number): Promise<void> {
    return this.conn.connect(timeoutMs);
  }

  /** New client with its own connection and the same settings. */
  duplicate(overrides: SnugKvClientOptions = {}): SnugKvClient {
    return new SnugKvClient({ ...this.options, ...overrides });
  }

  close(): void {
    this.conn.close();
  }

  /** Raw command. Rejects on an error reply. */
  call(...args: Arg[]): Promise<Reply> {
    return this.conn.send(args);
  }

  /** Several commands in one round trip; error replies are returned as RespError values. */
  pipeline(commands: Arg[][]): Promise<Reply[]> {
    return this.conn.pipeline(commands);
  }

  async ping(): Promise<boolean> {
    return (await this.call('PING')) === 'PONG';
  }

  // ----- strings

  async get(key: string): Promise<string | null> {
    return (await this.call('GET', key)) as string | null;
  }

  async set(key: string, value: string, opts: { exSeconds?: number } = {}): Promise<void> {
    const args: Arg[] = ['SET', key, value];
    if (opts.exSeconds) args.push('EX', opts.exSeconds);
    await this.call(...args);
  }

  async del(...keys: string[]): Promise<number> {
    return (await this.call('DEL', ...keys)) as number;
  }

  // ----- strings (extended)

  async setOpts(key: string, value: string | number, opts: SetOptions = {}): Promise<boolean> {
    const args: Arg[] = ['SET', key, value];
    if (opts.exSeconds !== undefined) args.push('EX', opts.exSeconds);
    if (opts.pxMs !== undefined) args.push('PX', opts.pxMs);
    if (opts.keepTtl) args.push('KEEPTTL');
    if (opts.nx) args.push('NX');
    if (opts.xx) args.push('XX');
    return (await this.call(...args)) !== null;
  }

  async mget(...keys: string[]): Promise<Array<string | null>> {
    return asArray(await this.call('MGET', ...keys)) as Array<string | null>;
  }

  async mset(values: Record<string, string | number>): Promise<void> {
    const args: Arg[] = ['MSET'];
    for (const [k, v] of Object.entries(values)) args.push(k, v);
    await this.call(...args);
  }

  async incr(key: string, by = 1): Promise<number> {
    return (await this.call(by === 1 ? 'INCR' : 'INCRBY', key, ...(by === 1 ? [] : [by]))) as number;
  }

  async decr(key: string, by = 1): Promise<number> {
    return (await this.call(by === 1 ? 'DECR' : 'DECRBY', key, ...(by === 1 ? [] : [by]))) as number;
  }

  async incrByFloat(key: string, by: number): Promise<number> {
    return Number(await this.call('INCRBYFLOAT', key, by));
  }

  async append(key: string, value: string): Promise<number> {
    return (await this.call('APPEND', key, value)) as number;
  }

  async getDel(key: string): Promise<string | null> {
    return (await this.call('GETDEL', key)) as string | null;
  }

  /** JSON convenience: stores `JSON.stringify(value)`; `ttlSeconds` optional. */
  async setJson(key: string, value: unknown, ttlSeconds?: number): Promise<void> {
    await this.setOpts(key, JSON.stringify(value), { exSeconds: ttlSeconds });
  }

  async getJson<T = unknown>(key: string): Promise<T | null> {
    const raw = await this.get(key);
    return raw === null ? null : (JSON.parse(raw) as T);
  }

  // ----- keys & expiry

  async exists(...keys: string[]): Promise<number> {
    return (await this.call('EXISTS', ...keys)) as number;
  }

  async type(key: string): Promise<string> {
    return String(await this.call('TYPE', key));
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    return (await this.call('EXPIRE', key, seconds)) === 1;
  }

  async pexpire(key: string, ms: number): Promise<boolean> {
    return (await this.call('PEXPIRE', key, ms)) === 1;
  }

  async persist(key: string): Promise<boolean> {
    return (await this.call('PERSIST', key)) === 1;
  }

  /** Seconds left; -1 = no expiry, -2 = no such key. */
  async ttl(key: string): Promise<number> {
    return (await this.call('TTL', key)) as number;
  }

  async pttl(key: string): Promise<number> {
    return (await this.call('PTTL', key)) as number;
  }

  async rename(from: string, to: string): Promise<void> {
    await this.call('RENAME', from, to);
  }

  async dbsize(): Promise<number> {
    return (await this.call('DBSIZE')) as number;
  }

  /** One SCAN page. */
  async scan(cursor: string, opts: { match?: string; count?: number; type?: string } = {}): Promise<{ cursor: string; keys: string[] }> {
    const args: Arg[] = ['SCAN', cursor];
    if (opts.match) args.push('MATCH', opts.match);
    if (opts.count) args.push('COUNT', opts.count);
    if (opts.type) args.push('TYPE', opts.type);
    const raw = asArray(await this.call(...args));
    return { cursor: String(raw[0] ?? '0'), keys: asArray(raw[1] ?? null).map(String) };
  }

  /** Iterates every matching key without loading them all at once. */
  async *scanIterator(opts: { match?: string; count?: number; type?: string } = {}): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const page = await this.scan(cursor, opts);
      cursor = page.cursor;
      yield* page.keys;
    } while (cursor !== '0');
  }

  // ----- hashes

  /** Sets fields; returns how many were new. */
  async hset(key: string, fields: Record<string, string | number>): Promise<number> {
    const args: Arg[] = ['HSET', key];
    for (const [k, v] of Object.entries(fields)) args.push(k, v);
    return (await this.call(...args)) as number;
  }

  async hget(key: string, field: string): Promise<string | null> {
    return (await this.call('HGET', key, field)) as string | null;
  }

  async hmget(key: string, ...fields: string[]): Promise<Array<string | null>> {
    return asArray(await this.call('HMGET', key, ...fields)) as Array<string | null>;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return pairsToObject(asArray(await this.call('HGETALL', key)));
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return (await this.call('HDEL', key, ...fields)) as number;
  }

  async hexists(key: string, field: string): Promise<boolean> {
    return (await this.call('HEXISTS', key, field)) === 1;
  }

  async hlen(key: string): Promise<number> {
    return (await this.call('HLEN', key)) as number;
  }

  async hkeys(key: string): Promise<string[]> {
    return asArray(await this.call('HKEYS', key)).map(String);
  }

  async hvals(key: string): Promise<string[]> {
    return asArray(await this.call('HVALS', key)).map(String);
  }

  async hincrBy(key: string, field: string, by = 1): Promise<number> {
    return (await this.call('HINCRBY', key, field, by)) as number;
  }

  async hincrByFloat(key: string, field: string, by: number): Promise<number> {
    return Number(await this.call('HINCRBYFLOAT', key, field, by));
  }

  // ----- lists

  async lpush(key: string, ...values: Array<string | number>): Promise<number> {
    return (await this.call('LPUSH', key, ...values)) as number;
  }

  async rpush(key: string, ...values: Array<string | number>): Promise<number> {
    return (await this.call('RPUSH', key, ...values)) as number;
  }

  async lpop(key: string): Promise<string | null> {
    return (await this.call('LPOP', key)) as string | null;
  }

  async rpop(key: string): Promise<string | null> {
    return (await this.call('RPOP', key)) as string | null;
  }

  async lrange(key: string, start = 0, stop = -1): Promise<string[]> {
    return asArray(await this.call('LRANGE', key, start, stop)).map(String);
  }

  async llen(key: string): Promise<number> {
    return (await this.call('LLEN', key)) as number;
  }

  async ltrim(key: string, start: number, stop: number): Promise<void> {
    await this.call('LTRIM', key, start, stop);
  }

  // ----- sets

  async sadd(key: string, ...members: string[]): Promise<number> {
    return (await this.call('SADD', key, ...members)) as number;
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    return (await this.call('SREM', key, ...members)) as number;
  }

  async smembers(key: string): Promise<string[]> {
    return asArray(await this.call('SMEMBERS', key)).map(String);
  }

  async sismember(key: string, member: string): Promise<boolean> {
    return (await this.call('SISMEMBER', key, member)) === 1;
  }

  async scard(key: string): Promise<number> {
    return (await this.call('SCARD', key)) as number;
  }

  async spop(key: string): Promise<string | null> {
    return (await this.call('SPOP', key)) as string | null;
  }

  // ----- sorted sets

  /** Adds or updates members; returns how many were new. */
  async zadd(key: string, members: Array<{ member: string; score: number }>): Promise<number> {
    const args: Arg[] = ['ZADD', key];
    for (const m of members) args.push(m.score, m.member);
    return (await this.call(...args)) as number;
  }

  async zincrBy(key: string, member: string, by = 1): Promise<number> {
    return Number(await this.call('ZINCRBY', key, by, member));
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const raw = await this.call('ZSCORE', key, member);
    return raw === null ? null : Number(raw);
  }

  async zrank(key: string, member: string): Promise<number | null> {
    return (await this.call('ZRANK', key, member)) as number | null;
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    return (await this.call('ZREM', key, ...members)) as number;
  }

  async zcard(key: string): Promise<number> {
    return (await this.call('ZCARD', key)) as number;
  }

  /** Members by rank, lowest score first (or highest first with `reverse`). */
  async zrange(key: string, start = 0, stop = -1, opts: { reverse?: boolean } = {}): Promise<Array<{ member: string; score: number }>> {
    const raw = asArray(await this.call(opts.reverse ? 'ZREVRANGE' : 'ZRANGE', key, start, stop, 'WITHSCORES'));
    const out: Array<{ member: string; score: number }> = [];
    for (let i = 0; i + 1 < raw.length; i += 2) out.push({ member: String(raw[i]), score: Number(raw[i + 1]) });
    return out;
  }

  /** Members with min <= score <= max. Use `-inf` / `+inf` or `(5` for exclusive bounds. */
  async zrangeByScore(key: string, min: number | string, max: number | string, opts: { offset?: number; count?: number } = {}): Promise<string[]> {
    const args: Arg[] = ['ZRANGEBYSCORE', key, min, max];
    if (opts.count !== undefined) args.push('LIMIT', opts.offset ?? 0, opts.count);
    return asArray(await this.call(...args)).map(String);
  }

  async zpopMin(key: string): Promise<{ member: string; score: number } | null> {
    const raw = asArray(await this.call('ZPOPMIN', key));
    return raw.length >= 2 ? { member: String(raw[0]), score: Number(raw[1]) } : null;
  }

  // ----- scripting

  /** Runs a Lua script. */
  async eval(script: string, keys: string[] = [], args: Array<string | number> = []): Promise<Reply> {
    return this.call('EVAL', script, keys.length, ...keys, ...args);
  }

  // ----- pub/sub

  /** Returns the number of subscribers that received it. */
  async publish(channel: string, message: string): Promise<number> {
    return (await this.call('PUBLISH', channel, message)) as number;
  }

  /** A subscriber with its own connection and the same settings. */
  subscriber(overrides: SnugKvClientOptions = {}): SnugKvSubscriber {
    return new SnugKvSubscriber(this.duplicate({ name: undefined, ...overrides }));
  }

  // ----- transactions

  /** Builds a MULTI/EXEC block that is sent in one write, so other callers cannot interleave. */
  tx(): Transaction {
    return new Transaction(this);
  }

  /** @internal */
  async execBlock(commands: Arg[][]): Promise<Reply[]> {
    const replies = await this.pipeline([['MULTI'], ...commands, ['EXEC']]);
    const err = replies.find((r, i) => r instanceof RespError && i <= commands.length);
    if (err instanceof RespError && !(replies[replies.length - 1] instanceof Array)) throw err;
    const exec = replies[replies.length - 1];
    if (exec instanceof RespError) throw exec;
    if (exec === null) throw new Error('transaction aborted');
    return exec as Reply[];
  }

  // ----- streams

  static xaddArgs(key: string, fields: Record<string, string>, opts: XAddOptions = {}): Arg[] {
    const args: Arg[] = ['XADD', key];
    if (opts.maxLen !== undefined) {
      args.push('MAXLEN');
      if (opts.approximate ?? true) args.push('~');
      args.push(opts.maxLen);
    }
    args.push(opts.id ?? '*');
    for (const [k, v] of Object.entries(fields)) args.push(k, v);
    return args;
  }

  async xadd(key: string, fields: Record<string, string>, opts: XAddOptions = {}): Promise<string> {
    return (await this.call(...SnugKvClient.xaddArgs(key, fields, opts))) as string;
  }

  async xlen(key: string): Promise<number> {
    return (await this.call('XLEN', key)) as number;
  }

  async xrange(key: string, start = '-', end = '+', count?: number): Promise<StreamEntry[]> {
    const args: Arg[] = ['XRANGE', key, start, end];
    if (count !== undefined) args.push('COUNT', count);
    return toEntries(await this.call(...args));
  }

  /** Creates the group (and the stream). Returns false when it already existed. */
  async xgroupCreate(key: string, group: string, id = '$'): Promise<boolean> {
    try {
      await this.call('XGROUP', 'CREATE', key, group, id, 'MKSTREAM');
      return true;
    } catch (err) {
      if (err instanceof RespError && err.code === 'BUSYGROUP') return false;
      throw err;
    }
  }

  async xgroupDelConsumer(key: string, group: string, consumer: string): Promise<number> {
    return (await this.call('XGROUP', 'DELCONSUMER', key, group, consumer)) as number;
  }

  async xreadgroup(o: XReadGroupOptions): Promise<Array<{ key: string; entries: StreamEntry[] }> | null> {
    const args: Arg[] = ['XREADGROUP', 'GROUP', o.group, o.consumer];
    if (o.count !== undefined) args.push('COUNT', o.count);
    if (o.blockMs !== undefined) args.push('BLOCK', o.blockMs);
    if (o.noAck) args.push('NOACK');
    args.push('STREAMS', ...o.streams.map((s) => s.key), ...o.streams.map((s) => s.id));
    const raw = await this.call(...args);
    if (!Array.isArray(raw)) return null;
    const out: Array<{ key: string; entries: StreamEntry[] }> = [];
    for (const item of raw) {
      if (Array.isArray(item) && typeof item[0] === 'string') out.push({ key: item[0], entries: toEntries(item[1] ?? null) });
    }
    return out;
  }

  async xack(key: string, group: string, ...ids: string[]): Promise<number> {
    return (await this.call('XACK', key, group, ...ids)) as number;
  }

  async xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    start = '0-0',
    count = 50,
  ): Promise<{ next: string; entries: StreamEntry[]; deleted: string[] }> {
    const raw = await this.call('XAUTOCLAIM', key, group, consumer, minIdleMs, start, 'COUNT', count);
    if (!Array.isArray(raw)) return { next: '0-0', entries: [], deleted: [] };
    return {
      next: String(raw[0] ?? '0-0'),
      entries: toEntries(raw[1] ?? null),
      deleted: Array.isArray(raw[2]) ? raw[2].map(String) : [],
    };
  }

  /** Extended XPENDING. */
  async xpending(key: string, group: string, start: string, end: string, count: number, consumer?: string): Promise<PendingEntry[]> {
    const args: Arg[] = ['XPENDING', key, group, start, end, count];
    if (consumer) args.push(consumer);
    const raw = await this.call(...args);
    if (!Array.isArray(raw)) return [];
    const out: PendingEntry[] = [];
    for (const r of raw) {
      if (Array.isArray(r)) {
        out.push({ id: String(r[0]), consumer: String(r[1]), idleMs: Number(r[2]), deliveries: Number(r[3]) });
      }
    }
    return out;
  }
}
