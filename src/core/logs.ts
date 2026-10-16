import { hostname } from 'node:os';
import type { SnugKvClient } from './client';
import type { Arg } from './resp';
import { currentTrace } from './trace';

/**
 * Log shipping: every line an app writes to stdout or stderr (Nest's logger,
 * winston, pino, plain console.log) is also written to a SnugKV stream
 * (`snug:logs`), so one console can show the logs of all services together.
 *
 * Lines keep going to the terminal unchanged. Levels and contexts are read
 * from the common formats (Nest, winston, JSON lines); a line written inside
 * a trace carries its trace id, so the console links the two.
 */

export type LogLevel = 'error' | 'warn' | 'info' | 'debug';

const LEVEL_RANK: Record<LogLevel, number> = { error: 0, warn: 1, info: 2, debug: 3 };

/** One log line (or one multi-line entry, such as an error with its stack). */
export interface LogRecord {
  /** Time, ms since epoch. */
  ts: number;
  service: string;
  instance: string;
  level: LogLevel;
  /** Nest/winston context (the class name in `[TokenService]`), if any. */
  context?: string;
  message: string;
  traceId?: string;
  spanId?: string;
}

export interface LogShipperOptions {
  /** Name of this app, shown on every line. */
  service: string;
  /** This process (default: host name, the container id in Docker). */
  instance?: string;
  /** Stream the lines go to (default `snug:logs`). */
  stream?: string;
  /** Approximate cap on the stream length (default 100,000). */
  maxLen?: number;
  /** Lowest level shipped (default `debug`, i.e. everything). */
  level?: LogLevel;
  /** Write after this many lines are buffered (default 200). */
  batchSize?: number;
  /** Write buffered lines at least this often, ms (default 500). */
  flushMs?: number;
  /** Lines kept while the server is unreachable; older ones are dropped (default 5,000). */
  maxBuffer?: number;
  /** Longest message kept (default 8,000 characters). */
  maxMessageLength?: number;
  /** Called when lines cannot be written. Shipping never throws into your code. */
  onError?: (err: unknown) => void;
}

export class LogShipper {
  readonly service: string;
  readonly instance: string;
  readonly stream: string;
  private readonly maxLen: number;
  private readonly minRank: number;
  private readonly batchSize: number;
  private readonly maxBuffer: number;
  private readonly maxMessageLength: number;
  private readonly onError: (err: unknown) => void;
  private buffer: LogRecord[] = [];
  private timer: NodeJS.Timeout | undefined;
  private flushing: Promise<void> | undefined;

  constructor(
    private readonly client: Pick<SnugKvClient, 'pipeline'>,
    options: LogShipperOptions,
  ) {
    this.service = options.service;
    this.instance = options.instance ?? hostname();
    this.stream = options.stream ?? 'snug:logs';
    this.maxLen = options.maxLen ?? 100_000;
    this.minRank = LEVEL_RANK[options.level ?? 'debug'];
    this.batchSize = options.batchSize ?? 200;
    this.maxBuffer = options.maxBuffer ?? 5_000;
    this.maxMessageLength = options.maxMessageLength ?? 8_000;
    this.onError = options.onError ?? (() => undefined);
    this.timer = setInterval(() => void this.flush(), options.flushMs ?? 500);
    this.timer.unref?.();
  }

  /** Queues one line. Lines below the configured level are dropped. */
  ship(line: Omit<LogRecord, 'service' | 'instance' | 'ts'> & { ts?: number }): void {
    if (LEVEL_RANK[line.level] > this.minRank) return;
    const message = line.message.length > this.maxMessageLength ? `${line.message.slice(0, this.maxMessageLength)}…` : line.message;
    this.buffer.push({ ...line, message, ts: line.ts ?? Date.now(), service: this.service, instance: this.instance });
    if (this.buffer.length > this.maxBuffer) this.buffer.splice(0, this.buffer.length - this.maxBuffer);
    if (this.buffer.length >= this.batchSize) void this.flush();
  }

  /** Writes buffered lines now. Resolves when they are written (or kept for later on error). */
  async flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    if (!this.buffer.length) return;
    const batch = this.buffer;
    this.buffer = [];
    const commands: Arg[][] = batch.map((l) => [
      'XADD', this.stream, 'MAXLEN', '~', this.maxLen, '*',
      'ts', String(l.ts),
      'service', l.service,
      'instance', l.instance,
      'level', l.level,
      'message', l.message,
      ...(l.context ? ['context', l.context] : []),
      ...(l.traceId ? ['traceId', l.traceId] : []),
      ...(l.spanId ? ['spanId', l.spanId] : []),
    ]);
    this.flushing = this.client
      .pipeline(commands)
      .then(() => undefined)
      .catch((err) => {
        // Put the batch back (newest kept) so a short outage loses nothing.
        this.buffer = [...batch, ...this.buffer].slice(-this.maxBuffer);
        this.onError(err);
      })
      .finally(() => {
        this.flushing = undefined;
      });
    return this.flushing;
  }

  /** Flushes and stops the timer. */
  async close(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
    if (this.buffer.length) await this.flush();
  }
}

// ----- reading levels and contexts out of formatted lines

const ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;
// [Nest] 12  - 10/16/2026, 6:19:00 AM     LOG [RouterExplorer] Mapped {/api, GET} route +2ms
const NEST = /^\[Nest\]\s+\d+\s+-\s+.*?\s{2,}(LOG|ERROR|WARN|DEBUG|VERBOSE|FATAL)\s+(?:\[([^\]]+)\]\s+)?([\s\S]*)$/;
// 2026-10-15T15:24:40.947Z [TokenService] info: Using network: testnet +8ms
const WINSTON = /^(\d{4}-\d\d-\d\d[T ][\d:.]+Z?)\s+(?:\[([^\]]+)\]\s+)?(error|warn|warning|info|http|verbose|debug|silly):\s?([\s\S]*)$/i;
// [warn] something / warn: something / WARN something (not "Error handling…")
const LEADING_TAG = /^(?:\[(error|err|fatal|warn|warning|info|debug|trace|verbose)\]:?|(error|err|fatal|warn|warning|info|debug|trace|verbose):)\s+([\s\S]*)$/i;
const LEADING_UPPER = /^(ERROR|ERR|FATAL|WARN|WARNING|INFO|DEBUG|TRACE|VERBOSE)\s+([\s\S]*)$/;
const NEST_MS = /\s\+\d+ms$/;

export function toLevel(word: string | number | undefined, fallback: LogLevel = 'info'): LogLevel {
  if (typeof word === 'number') {
    // pino / bunyan numeric levels
    if (word >= 50) return 'error';
    if (word >= 40) return 'warn';
    if (word >= 30) return 'info';
    return 'debug';
  }
  switch ((word ?? '').toLowerCase()) {
    case 'error':
    case 'err':
    case 'fatal':
    case 'crit':
    case 'critical':
    case 'emerg':
    case 'alert':
      return 'error';
    case 'warn':
    case 'warning':
      return 'warn';
    case 'info':
    case 'log':
    case 'http':
    case 'notice':
      return 'info';
    case 'debug':
    case 'verbose':
    case 'silly':
    case 'trace':
      return 'debug';
    default:
      return fallback;
  }
}

export interface ParsedLine {
  level: LogLevel;
  context?: string;
  message: string;
  /** Time written in the line itself, if it had one. */
  ts?: number;
}

/**
 * Level, context and message of one formatted entry. `fallback` is the level
 * when the line does not say (from the console method or the output stream).
 */
export function parseLogLine(raw: string, fallback: LogLevel = 'info'): ParsedLine {
  const text = raw.replace(ANSI, '').replace(/\s+$/, '');
  const first = text.trimStart();

  if (first.startsWith('{') && first.endsWith('}')) {
    try {
      const j = JSON.parse(first) as Record<string, unknown>;
      const msg = j.message ?? j.msg;
      if (typeof msg === 'string') {
        const ctx = j.context ?? j.ctx ?? j.name;
        const time = typeof j.time === 'number' ? j.time : typeof j.timestamp === 'string' ? Date.parse(j.timestamp) : NaN;
        const out: ParsedLine = { level: toLevel(j.level as string | number | undefined, fallback), message: msg };
        if (typeof ctx === 'string' && ctx) out.context = ctx;
        if (Number.isFinite(time)) out.ts = time;
        if (typeof j.stack === 'string' && !msg.includes(j.stack)) out.message = `${msg}\n${j.stack}`;
        return out;
      }
    } catch {
      /* not JSON after all */
    }
  }

  let m = NEST.exec(text);
  if (m) {
    const out: ParsedLine = { level: toLevel(m[1], fallback), message: m[3]!.replace(NEST_MS, '') };
    if (m[2]) out.context = m[2];
    return out;
  }
  m = WINSTON.exec(text);
  if (m) {
    const out: ParsedLine = { level: toLevel(m[3], fallback), message: m[4]!.replace(NEST_MS, '') };
    if (m[2]) out.context = m[2];
    const ts = Date.parse(m[1]!);
    if (Number.isFinite(ts)) out.ts = ts;
    return out;
  }
  m = LEADING_TAG.exec(text);
  if (m) return { level: toLevel(m[1] ?? m[2], fallback), message: m[3]! };
  m = LEADING_UPPER.exec(text);
  if (m) return { level: toLevel(m[1], fallback), message: m[2]! };
  return { level: fallback, message: text };
}

/** True when a line starts a new entry in one of the known formats. */
function startsEntry(line: string): boolean {
  const t = line.replace(ANSI, '');
  return NEST.test(t) || WINSTON.test(t) || (t.startsWith('{') && t.trimEnd().endsWith('}'));
}

/**
 * Splits one write into entries. A write is usually one entry (several lines
 * only for a stack or a printed object); it is split only where a line starts
 * a new entry of a known format.
 */
export function splitEntries(chunk: string): string[] {
  const lines = chunk.replace(/\r\n/g, '\n').split('\n');
  const out: string[] = [];
  for (const line of lines) {
    if (out.length > 0 && startsEntry(line)) out.push(line);
    else if (out.length === 0) out.push(line);
    else out[out.length - 1] += `\n${line}`;
  }
  return out.map((e) => e.replace(/\s+$/, '')).filter((e) => e.replace(ANSI, '').trim() !== '');
}

// ----- capturing stdout and stderr

type Write = typeof process.stdout.write;
type ConsoleMethod = 'log' | 'info' | 'debug' | 'warn' | 'error' | 'trace';

let installed: { shipper: LogShipper; restore: () => void } | undefined;

/** The shipper installed by {@link captureLogs}, if any. */
export function getLogShipper(): LogShipper | undefined {
  return installed?.shipper;
}

/**
 * Copies everything the process writes to stdout and stderr to `shipper`
 * (the terminal output is unchanged). Returns a function that undoes it.
 * Installing again replaces the previous capture.
 */
export function captureLogs(shipper: LogShipper): () => void {
  installed?.restore();

  // The console method being run, so console.warn lines are warnings even
  // when the format does not say so.
  let methodLevel: LogLevel | undefined;
  let inside = false;

  const handle = (chunk: unknown, encoding: unknown, fallback: LogLevel) => {
    if (inside) return;
    inside = true;
    try {
      const text = typeof chunk === 'string' ? chunk : Buffer.isBuffer(chunk) || chunk instanceof Uint8Array ? Buffer.from(chunk).toString(typeof encoding === 'string' ? (encoding as BufferEncoding) : 'utf8') : '';
      if (!text) return;
      const trace = currentTrace();
      for (const entry of splitEntries(text)) {
        const p = parseLogLine(entry, methodLevel ?? fallback);
        shipper.ship({
          level: p.level,
          message: p.message,
          ...(p.context ? { context: p.context } : {}),
          ...(p.ts ? { ts: p.ts } : {}),
          ...(trace ? { traceId: trace.traceId, spanId: trace.spanId } : {}),
        });
      }
    } catch {
      /* never break the app's output */
    } finally {
      inside = false;
    }
  };

  const wrap = (stream: NodeJS.WriteStream, fallback: LogLevel): (() => void) => {
    const original = stream.write;
    const patched = function (this: NodeJS.WriteStream, chunk: unknown, encoding?: unknown, cb?: unknown) {
      handle(chunk, encoding, fallback);
      return (original as (...a: unknown[]) => boolean).call(this, chunk, encoding, cb);
    } as Write;
    stream.write = patched;
    return () => {
      if (stream.write === patched) stream.write = original;
    };
  };

  const consoleLevels: Array<[ConsoleMethod, LogLevel]> = [
    ['log', 'info'],
    ['info', 'info'],
    ['debug', 'debug'],
    ['warn', 'warn'],
    ['error', 'error'],
    ['trace', 'debug'],
  ];
  const restoreConsole = consoleLevels.map(([method, level]) => {
    const original = console[method];
    const patched = function (this: Console, ...args: unknown[]) {
      const prev = methodLevel;
      methodLevel = level;
      try {
        return (original as (...a: unknown[]) => void).apply(this, args);
      } finally {
        methodLevel = prev;
      }
    };
    console[method] = patched;
    return () => {
      if (console[method] === patched) console[method] = original;
    };
  });

  const restoreOut = wrap(process.stdout, 'info');
  const restoreErr = wrap(process.stderr, 'error');
  const restore = () => {
    restoreOut();
    restoreErr();
    restoreConsole.forEach((r) => r());
    if (installed?.restore === restore) installed = undefined;
  };
  installed = { shipper, restore };
  return restore;
}
