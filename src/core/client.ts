import { EventEmitter } from 'node:events';
import { Connection, ConnectionOptions } from './connection';
import { Arg, Reply, RespError } from './resp';
import { parseUrl } from './util';

export interface SnugKvClientOptions extends Partial<ConnectionOptions> {
  /** `redis://user:pass@host:port`, `rediss://…`, `snugkv://…` or `snugkvs://…`. */
  url?: string;
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
 * A small client for the commands the bus needs, plus a raw `call` for
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
